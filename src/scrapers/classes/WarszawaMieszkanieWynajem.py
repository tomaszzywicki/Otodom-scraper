import time
import os
import re
import requests
import psycopg2
from datetime import date
from bs4 import BeautifulSoup
import pandas as pd
import numpy as np
from sqlalchemy import create_engine
from dotenv import load_dotenv
import math

from ..settings import TIMEOUT, HEADERS, OUTPUT_FOLDER

load_dotenv()


class WarszawaMieszkanieWynajem:
    def __init__(self):
        self.voividoship = "mazowieckie"
        self.city = "warszawa"
        self.district = ""
        self.district_area = ""
        self.BASE_URL = f"https://www.otodom.pl/pl/wyniki/wynajem/mieszkanie/{self.voividoship}/{self.city}/{self.city}/{self.city}/{self.district}/{self.district_area}?ownerTypeSingleSelect=ALL&viewType=listing&by=LATEST&direction=DESC"
        self.cursor_init()

    def cursor_init(self):
        self.conn = psycopg2.connect(
            host=os.getenv("DB_HOST"),
            database=os.getenv("DB_NAME"),
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASSWORD"),
            port=os.getenv("DB_PORT"),
        )
        self.cursor = self.conn.cursor()

    def fetch_page(self, url):
        try:
            response = requests.get(url, headers=HEADERS, timeout=TIMEOUT)
            response.raise_for_status()
            return response.text
        except requests.RequestException as e:
            print(f"Error fetching {url}:\n\n {e}\n\n\n")
            return None

    def scrape_single_listing(self, url):
        page_content = self.fetch_page(url)
        if page_content is None:
            return None

        soup = BeautifulSoup(page_content, "lxml")

        tytul = soup.find(class_="ehdsj771")
        cena = soup.find(class_="e1w5xgvx1")
        czynsz = soup.find(class_="e1w5xgvx5")
        adres = soup.find(class_="e42rcgs1")

        # Te rzeczy w szarych "kafelkach" ale interesuje mnie tylko powierzchnia i ilość pokoi
        items = soup.find_all(class_="css-1ftqasz")
        items_attributes = {"powierzchnia": None, "pokoje": None}
        powierzchnia_pattern = re.compile(r"(\d+(?:\.\d+)?)\s?m²")
        pokoje_pattern = re.compile(r"(\d+)\s+pok\w*")

        for item in items:
            text = item.get_text()
            powierzchnia = powierzchnia_pattern.search(text)
            pokoje = pokoje_pattern.search(text)
            if powierzchnia:
                powierzchnia = powierzchnia.group(1)
                items_attributes["powierzchnia"] = powierzchnia
            if pokoje:
                pokoje = pokoje.group(1)
                items_attributes["pokoje"] = pokoje

        # Wszystkie informacje z tabel
        szczegoly = soup.find_all(class_="etn78ea3")
        attributes = {}

        # Tutaj iteruje po parzystych elementach listy, bo co drugi to klucz a co drugi wartość (i odpowienio je parsuje)
        for i in range(0, len(szczegoly), 2):
            key_text = szczegoly[i].get_text(separator=" ", strip=True)
            value_text = szczegoly[i + 1].get_text(separator=" ", strip=True)
            key_text = key_text.rstrip(" :").lower().replace(" ", "_")
            attributes[key_text] = value_text

        opis = soup.find(class_="e1f0p0zw1")

        # Daty dodania i aktualizacji oraz ID ogłoszenia
        date_and_id = soup.find_all(class_="e82kd4s2")
        date_id_attributes = {}

        for item in date_and_id:
            text = item.get_text()
            text = text.split(":")
            key = text[0].strip().lower().replace(" ", "_")
            value = text[1].strip()
            date_id_attributes[key] = value

        return {
            "tytul": tytul.text if tytul else None,
            "cena": cena.text if cena else None,
            "czynsz": czynsz.text.split("+ Czynsz ")[1] if czynsz else None,
            "adres": adres.text if adres else None,
            "powierzchnia": items_attributes["powierzchnia"],
            "pokoje": items_attributes["pokoje"],
            "attributes": attributes,
            "opis": opis.text if opis else None,
            "dodano": date_id_attributes["dodano"],
            "aktualizacja": date_id_attributes["aktualizacja"],
            "id": date_id_attributes["id"],
            "data_pobrania_danych": date.today().isoformat(),
        }

    def scrape_listings(self):

        DATABASE_URI = os.getenv("DATABASE_URI")
        engine = create_engine(DATABASE_URI)

        url = f"{self.BASE_URL}&page={1}"

        listing_num = 0

        # tu patrzymy sobie ile jest listingów łącznie na wszystkich stronach
        page_content = self.fetch_page(url)
        if page_content is None:
            return

        soup = BeautifulSoup(page_content, "lxml")

        text = soup.find("div", class_="css-15svspy").text
        match = re.search(r"\d+(?=\D*$)", text)

        if match:
            num_listings = int(match.group(0))
        else:
            num_listings = 1000

        listing_num = 0
        page_num = math.floor(num_listings / 36) + 1
        url = f"{self.BASE_URL}&page={page_num}"

        start_time = time.time()
        while True:
            url = f"{self.BASE_URL}&page={page_num}"

            print(f"Scraping page {page_num}...")
            print(f"Time elapsed: {round(time.time() - start_time, 2)} seconds")
            print(f"Total listings scraped: {listing_num}\n")

            page_content = self.fetch_page(url)
            if page_content is None:
                page_num -= 1
                break
            soup = BeautifulSoup(page_content, "lxml")

            listings_div = soup.find("div", {"data-cy": "search.listing.organic"})
            if not listings_div:
                # print("No more listings to scrape.")
                # break
                print("No listings found on page ", page_num)
                page_num -= 1
                continue

            a_items = listings_div.find_all("a", {"data-cy": "listing-item-link"})
            if not a_items:
                print("No listings found on page ", page_num)
                page_num -= 1
                continue

            for item in a_items:
                href = item["href"]
                link = f"https://www.otodom.pl/{href}"

                listing_content = self.scrape_single_listing(link)
                if listing_content is None:
                    continue

                row = self.prepare_row(
                    listing_content, link
                )  # dodać parametr href i potem w funkcji zmienić TODO

                self.insert_to_database(row, engine, link)
                # self.append_to_output(row, OUTPUT_FOLDER + "mieszkanie_wynajem.csv")

                time.sleep(0.1)
                listing_num += 1
                if listing_num >= num_listings or page_num == 1:
                    print("Scraping finished :)")
                    break

            # wyjście z głównego while'a
            if listing_num >= num_listings or page_num == 1:
                break

            page_num -= 1
            time.sleep(0.1)

        # Jak już skończy główną pętlę to teraz przechodzi spowrotem przez 1 i drugą stronę żeby sprawdzić czy nie ma albo nowych ogłoszeń z innym id niż jest w bazie albo czy nie ma ogłoszeń które są w bazie ale zostały zaaktualizowane

        page_num = 1
        listing_num = 0

        while True:
            print("Scraping potentially new listings...")

            url = f"{self.BASE_URL}&page={page_num}"

            page_content = self.fetch_page(url)
            if page_content is None:
                page_num += 1
                break
            soup = BeautifulSoup(page_content, "lxml")

            listings_div = soup.find("div", {"data-cy": "search.listing.organic"})
            if not listings_div:
                print("No more listings to scrape.")
                break

            a_items = listings_div.find_all("a", {"data-cy": "listing-item-link"})
            if not a_items:
                print("No listings found on page ", page_num)
                page_num += 1
                continue

            for item in a_items:
                href = item["href"]
                link = f"https://www.otodom.pl/{href}"

                listing_content = self.scrape_single_listing(link)
                if listing_content is None:
                    continue

                row = self.prepare_row(listing_content, link)
                self.insert_to_database(row, engine, link)

                time.sleep(0.1)
                listing_num += 1

            page_num += 1
            time.sleep(0.1)

            if page_num == 3:
                page_num = 1

    def prepare_row(self, data, link):
        df = pd.DataFrame(
            [
                {
                    "id_mieszkania": data["id"],
                    "tytul": data["tytul"],
                    "cena": data["cena"],
                    "czynsz": data["czynsz"],
                    "kaucja": data["attributes"].get("kaucja"),
                    "adres": data["adres"],
                    "powierzchnia": data["powierzchnia"],
                    "pokoje": data["pokoje"],
                    "ogrzewanie": data["attributes"].get("ogrzewanie"),
                    "piętro": data["attributes"].get("piętro"),
                    "stan_wykończenia": data["attributes"].get("stan_wykończenia"),
                    "typ_ogłoszeniodawcy": data["attributes"].get(
                        "typ_ogłoszeniodawcy"
                    ),
                    "dostępne_od": data["attributes"].get("dostępne_od"),
                    "informacje_dodatkowe": data["attributes"].get(
                        "informacje_dodatkowe"
                    ),
                    "rok_budowy": data["attributes"].get("rok_budowy"),
                    "winda": data["attributes"].get("winda"),
                    "rodzaj_zabudowy": data["attributes"].get("rodzaj_zabudowy"),
                    "materiał_budynku": data["attributes"].get("materiał_budynku"),
                    "okna": data["attributes"].get("okna"),
                    "wyposażenie": data["attributes"].get("wyposażenie"),
                    "bezpieczeństwo": data["attributes"].get("bezpieczeństwo"),
                    "certyfikat_energetyczny": data["attributes"].get(
                        "certyfikat_energetyczny"
                    ),
                    "opis": data["opis"],
                    "dodano": data["dodano"],
                    "aktualizacja": data["aktualizacja"],
                    "link": link,
                    "data_pobrania_danych": data["data_pobrania_danych"],
                }
            ]
        )

        def tidy_row(df):
            def replace_brak_informacji(value):
                value = str(value).lower()
                if value == "brak informacji":
                    return None
                else:
                    return value

            def change_money_columns(value):
                value = str(value)
                if "EUR" in value:
                    return (
                        float(
                            value.replace("EUR", "").replace(" ", "").replace(",", ".")
                        )
                        * 4.3
                    )
                elif "USD" in value:
                    return (
                        float(
                            value.replace("USD", "").replace(" ", "").replace(",", ".")
                        )
                        * 3.8
                    )
                elif "zł" in value:
                    return float(
                        value.replace("zł", "").replace(" ", "").replace(",", ".")
                    )
                else:
                    return None

            not_numeric_cols = df.select_dtypes(include=["object"]).columns
            for col in not_numeric_cols:
                df[col] = df[col].apply(replace_brak_informacji)

            df["cena"] = df["cena"].astype(str).apply(change_money_columns)
            df["czynsz"] = df["czynsz"].astype(str).apply(change_money_columns)
            df["kaucja"] = df["kaucja"].astype(str).apply(change_money_columns)

            def extract_address(value):
                value = str(value)
                miasto, dzielnica, obszar, ulica = None, None, None, None
                if value and value is not np.nan:
                    parts = value.split(", ")[-2::-1]
                    if len(parts) == 4:
                        miasto, dzielnica, obszar, ulica = parts
                    elif len(parts) == 3:
                        miasto, dzielnica, obszar = parts
                    elif len(parts) == 2:
                        miasto, dzielnica = parts
                return miasto, dzielnica, obszar, ulica

            df[["miasto", "dzielnica", "obszar", "ulica"]] = (
                df["adres"].apply(extract_address).apply(pd.Series)
            )

            df.drop(columns=["adres"], inplace=True)

            def extract_floor(value):
                if value is np.nan or value is None:
                    return None, None, None

                value = str(value)
                pietro, liczba_pieter, liczba_pieter_ponad_10 = None, None, None
                if value and value is not np.nan:
                    if ">" in value:
                        liczba_pieter_ponad_10 = True
                        value = value.strip("> ")
                        parts = value.split("/")
                        if len(parts) == 2:
                            liczba_pieter = parts[1]
                        else:
                            liczba_pieter = None
                        return None, liczba_pieter, liczba_pieter_ponad_10

                    liczba_pieter_ponad_10 = False
                    parts = value.split("/")
                    if len(parts) == 2:
                        pietro, liczba_pieter = parts
                    elif len(parts) == 1:
                        pietro = parts[0]

                    if pietro == "parter":
                        pietro = 0
                    elif pietro == "suterena":
                        pietro = -1
                    elif pietro == "poddasze" and liczba_pieter is not None:
                        pietro = liczba_pieter
                    elif pietro == "poddasze":
                        pietro = None
                    if liczba_pieter:
                        liczba_pieter = int(liczba_pieter)
                return pietro, liczba_pieter, liczba_pieter_ponad_10

            df[["pietro", "liczba_pieter", "liczba_pieter_ponad_10"]] = (
                df["piętro"].apply(extract_floor).apply(pd.Series)
            )
            df.drop(columns=["piętro"], inplace=True)

            def extract_informacje_dodatkowe(value):
                balkon, ogródek, taras, parking, piwnica = (
                    False,
                    False,
                    False,
                    False,
                    False,
                )
                if value and value is not np.nan:
                    value = str(value).lower()
                    if "balkon" in value:
                        balkon = True
                    if "ogródek" in value:
                        ogródek = True
                    if "taras" in value:
                        taras = True
                    if "garaż" in value or "parking" in value:
                        parking = True
                    if "piwnica" in value:
                        piwnica = True
                return balkon, ogródek, taras, parking, piwnica

            df[["balkon", "ogrodek", "taras", "parking", "piwnica"]] = (
                df["informacje_dodatkowe"]
                .apply(extract_informacje_dodatkowe)
                .apply(pd.Series)
            )
            df.drop(columns=["informacje_dodatkowe"], inplace=True)

            def extract_bezpieczenstwo(value):
                teren_zamkniety, ochrona = False, False
                if value and value is not np.nan:
                    value = str(value).lower()
                    if "teren zamknięty" in value:
                        teren_zamkniety = True
                    if "ochrona" in value:
                        ochrona = True
                return teren_zamkniety, ochrona

            df[["teren_zamkniety", "monitoring/ochrona"]] = (
                df["bezpieczeństwo"].apply(extract_bezpieczenstwo).apply(pd.Series)
            )
            df.drop(columns=["bezpieczeństwo"], inplace=True)

            new_column_order = [
                "id_mieszkania",
                "miasto",
                "dzielnica",
                "obszar",
                "ulica",
                "cena",
                "czynsz",
                "kaucja",
                "powierzchnia",
                "pokoje",
                "pietro",
                "liczba_pieter",
                "liczba_pieter_ponad_10",
                "rok_budowy",
                "rodzaj_zabudowy",
                "materiał_budynku",
                "okna",
                "ogrzewanie",
                "certyfikat_energetyczny",
                "stan_wykończenia",
                "winda",
                "parking",
                "piwnica",
                "balkon",
                "ogrodek",
                "taras",
                "teren_zamkniety",
                "monitoring/ochrona",
                "typ_ogłoszeniodawcy",
                "dostępne_od",
                "wyposażenie",
                "tytul",
                "opis",
                "dodano",
                "aktualizacja",
                "link",
                "data_pobrania_danych",
            ]

            df = df[new_column_order]

            columns_to_date = [
                "dostępne_od",
                "dodano",
                "aktualizacja",
            ]

            for col in columns_to_date:
                df[col] = pd.to_datetime(
                    df[col], errors="coerce", format="%d.%m.%Y", dayfirst=True
                )
            df["data_pobrania_danych"] = pd.to_datetime(
                df["data_pobrania_danych"], errors="coerce"
            )
            df = df.replace({"none": None})
            df.replace({"tak": True, "nie": False}, inplace=True)

            return df

        # Ogłoszenie nieaktywne
        if df["aktualizacja"].iloc[0] is None or df["aktualizacja"].iloc[0] == "None":
            return None

        return tidy_row(df)

    def append_to_output(self, row, output_path):
        if row is None:
            return
        file_exists = os.path.isfile(output_path)
        if file_exists:
            row.to_csv(output_path, mode="a", header=False, index=False)
        else:
            row.to_csv(output_path, mode="w", header=True, index=False)

    def insert_to_database(self, row, engine, link):
        self.cursor_init()
        if row is None:
            return
        id_mieszkania = row["id_mieszkania"].iloc[0]
        data_aktualizacji = row["aktualizacja"].iloc[0]
        self.cursor.execute(
            f"SELECT * FROM warszawa_wynajem WHERE id_mieszkania = '{id_mieszkania}' AND aktualizacja = '{data_aktualizacji}'"
        )

        db_row = self.cursor.fetchone()

        if db_row is None:
            row.to_sql("warszawa_wynajem", engine, if_exists="append", index=False)
            print(f"Inserted listing {id_mieszkania} to database")
        else:
            print(
                f"Listing {id_mieszkania} with aktualizacja {data_aktualizacji} already in database"
            )
        # trzeba update'ować linki bo jakimś cudem źle się wstawiają do bazy
        # nie mam pojęcia czemu
        update_query = """
        UPDATE warszawa_wynajem SET link = %s WHERE id_mieszkania = %s AND aktualizacja = %s
        """
        self.cursor.execute(update_query, (link, id_mieszkania, data_aktualizacji))
        self.conn.commit()
        self.conn.close()
