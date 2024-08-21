from datetime import date
import requests
from bs4 import BeautifulSoup
from .settings import TIMEOUT, HEADERS
import re
import time
import pandas as pd
import os


def fetch_page(url):
    try:
        response = requests.get(url, headers=HEADERS, timeout=TIMEOUT)
        response.raise_for_status()
        return response.text
    except requests.RequestException as e:
        print(f"Error fetching {url}: {e}")
        return None


def scrape_data(url):
    page_content = fetch_page(url)
    if page_content:
        soup = BeautifulSoup(page_content, "lxml")
        tytul = soup.find(class_="ehdsj771")
        cena = soup.find(class_="e1w5xgvx1")
        czynsz = soup.find(class_="e1w5xgvx5")
        adres = soup.find(class_="e42rcgs1")

        items = soup.find_all(class_="css-1ftqasz")
        items_attributes = {"powierzchnia": None, "pokoje": None}
        powierzchnia_pattern = re.compile(r"(\d+.\d+)\s?m²")
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

        details = soup.find_all(class_="etn78ea3")
        attributes = {}
        for i in range(0, len(details), 2):
            key_text = details[i].get_text(separator=" ", strip=True)
            value_text = details[i + 1].get_text(separator=" ", strip=True)
            key_text = key_text.rstrip(" :").lower().replace(" ", "_")
            attributes[key_text] = value_text

        description = soup.find(class_="e1f0p0zw1")

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
            "opis": description.text if description else None,
            "dodano": date_id_attributes["dodano"],
            "aktualizacja": date_id_attributes["aktualizacja"],
            "id": date_id_attributes["id"],
            "link": link,
            "scraped_date": date.today().isoformat(),
        }


def scrape_all_listings(url, output_path, max_listings=3):  # można dać parametr descending
    page_number = 1
    listing_number = 1
    while True:
        url = f"{url}?by=DEFAULT&direction=DESC&viewType=listing&page={page_number}"
        page_content = fetch_page(url)
        soup = BeautifulSoup(page_content, "lxml")
        listings_div = soup.find("div", {"data-cy": "search.listing.organic"})
        if not listings_div:
            print("No listings found")
            return
        for listing in listings_div.find_all("article", {"data-cy": "listing-item"}):
            if not listing:
                continue
            div = listing.find(
                "div",
                {"aria-selected": "true", "class": "css-17rb9mp"},
            )
            if not div:
                continue
            href = div.find("a")["href"]
            if not href:
                continue
            link = f"https://www.otodom.pl/{href}"

            listing_content = scrape_data(link)
            row = listing_data_to_dataframe(listing_content)
            append_row_to_csv(row, output_path)

            time.sleep(0.1)
            listing_number += 1
            if listing_number > max_listings:
                print("Scraping finished :))")
                return
        page_number += 1
        time.sleep(1)


def listing_data_to_dataframe(listing):
    row = pd.DataFrame(
        [
            {
                "id": listing["id"],
                "tytul": listing["tytul"],
                "cena": listing["cena"],
                "czynsz": listing["czynsz"],
                "kaucja": listing["attributes"].get("kaucja"),
                "adres": listing["adres"],
                "powierzchnia": listing["powierzchnia"],
                "pokoje": listing["pokoje"],
                "ogrzewanie": listing["attributes"].get("ogrzewanie"),
                "piętro": listing["attributes"].get("piętro"),
                "stan_wykończenia": listing["attributes"].get("stan_wykończenia"),
                "dostępne_od": listing["attributes"].get("dostępne_od"),
                "dodatkowe_informacje": listing["attributes"].get(
                    "dodatkowe_informacje"
                ),
                "rok_budowy": listing["attributes"].get("rok_budowy"),
                "winda": listing["attributes"].get("winda"),
                "rodzaj_zabudowy": listing["attributes"].get("rodzaj_zabudowy"),
                "okna": listing["attributes"].get("okna"),
                "bezpieczeństwo": listing["attributes"].get("bezpieczeństwo"),
                "wyposażenie": listing["attributes"].get("wyposażenie"),
                "opis": listing["opis"],
                "dodano": listing["dodano"],
                "aktualizacja": listing["aktualizacja"],
                "link": listing["link"],
                "scraped_date": listing["scraped_date"],
            }
        ]
    )
    return row


def append_row_to_csv(row, output_path):
    file_exists = os.path.isfile(output_path)
    row.to_csv(output_path, mode="a", header = not file_exists, index=False)
