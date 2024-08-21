from bs4 import BeautifulSoup
from datetime import date
import re


class MieszkanieWynajem:
    def __init__(self, page_content):
        self.soup = BeautifulSoup(page_content, "lxml")

    def scrape_listing_details(self):
        soup = self.soup
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
            "description": description.text if description else None,
            "dodano": date_id_attributes["dodano"],
            "aktualizacja": date_id_attributes["aktualizacja"],
            "id": date_id_attributes["id"],
            "link": link,
            "scraped_date": date.today().isoformat(),
        }
