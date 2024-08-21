import time
import os
import re
import requests
from datetime import date
from bs4 import BeautifulSoup
import pandas as pd

from ..settings import TIMEOUT, HEADERS, OUTPUT_FOLDER


class MieszkanieWynajem:
    def __init__(self, voividoship, city, district="", district_area=""):
        self.BASE_URL = f"https://www.otodom.pl/pl/wyniki/wynajem/mieszkanie/{voividoship}/{city}/{city}/{city}/{district}/{district_area}?ownerTypeSingleSelect=ALL&viewType=listing&by=LATEST&direction=DESC"
        self.voividoship = voividoship
        self.city = city
        self.district = district
        self.district_area = district_area

    def fetch_page(self, url):
        try:
            response = requests.get(url, headers=HEADERS, timeout=TIMEOUT)
            response.raise_for_status()
            return response.text
        except requests.RequestException as e:
            print(f"Error fetching {url}: {e}\n\n\n")
            return None

    def scrape_single_listing(self, url):
        page_content = self.fetch_page(url)
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

        link = soup.find("link", rel="canonical")
        link = link["href"] if link else None

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
            "link": link,
            "data_pobrania_danych": date.today().isoformat(),
        }

    def scrape_listings(self, num_listings):
        page_num = 1
        listing_num = 0
        start_time = time.time()
        while True:
            url = f"{self.BASE_URL}&page={page_num}"

            print(f"Scraping page {page_num}...")
            print(f"Time elapsed: {time.time() - start_time} seconds")
            print(f"Total listings scraped: {listing_num}\n")

            page_content = self.fetch_page(url)
            soup = BeautifulSoup(page_content, "lxml")

            listings_div = soup.find("div", {"data-cy": "search.listing.organic"})
            if not listings_div:
                print("No more listings to scrape.")
                return

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

                row = self.listing_data_to_dataframe(listing_content)
                output_name = f'mieszkanie_wynajem_{self.city}_{self.district+"_" if self.district else ""}{self.district_area+"_" if self.district_area else ""}.csv'
                self.append_to_output(row, OUTPUT_FOLDER + output_name)

                time.sleep(0.1)
                listing_num += 1
                if listing_num >= num_listings:
                    print("Scraping finished :)")
                    return

            page_num += 1
            time.sleep(0.1)

    def listing_data_to_dataframe(self, data):
        row = pd.DataFrame(
            [
                {
                    "id": data["id"],
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
                    "link": data["link"],
                    "data_pobrania_danych": data["data_pobrania_danych"],
                }
            ]
        )
        return row

    def append_to_output(self, row, output_path):
        file_exists = os.path.isfile(output_path)
        if file_exists:
            row.to_csv(output_path, mode="a", header=False, index=False)
        else:
            row.to_csv(output_path, mode="w", header=True, index=False)
