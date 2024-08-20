from bs4 import BeautifulSoup


class MieszkanieWynajem:
    def __init__(self, page_content):
        self.soup = BeautifulSoup(page_content, "lxml")

    def get_data(self):
        soup = self.soup
        tytul = soup.find(class_="ehdsj771")
        cena = soup.find(class_="e1w5xgvx1")
        czynsz = soup.find(class_="e1w5xgvx5")
        adres = soup.find(class_="e42rcgs1")
        items = soup.find_all(class_="css-1ftqasz")
        details = soup.find_all(class_="etn78ea3")
        description = soup.find(class_="e1f0p0zw1")
        date_and_id = soup.find_all(class_="e82kd4s1")
        link = soup.find("link", rel="canonical")

        return {
            "tytul": tytul.text if tytul else None,
            "cena": cena.text if cena else None,
            "czynsz": czynsz.text if czynsz else None,
            "adres": adres.text if adres else None,
            "items": [],  # TODO
            "details": [],  # TODO
            "description": description.text if description else None,
            "date_and_id": [],  # TODO
            "link": link["href"] if link else None,
        }
