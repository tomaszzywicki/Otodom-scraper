import requests
from bs4 import BeautifulSoup
from .settings import TIMEOUT, HEADERS


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
        tytul = soup.find_all(class_="ehdsj771")
        cena = soup.find_all(class_="e1w5xgvx1")
        czynsz = soup.find_all(class_="e1w5xgvx5")
        adres = soup.find_all(class_="e42rcgs1")
        items = soup.find_all(class_="css-1ftqasz")
        print(
            f"tytul: {tytul[0].text}\ncena: {cena[0].text}\nczynsz: {czynsz[0].text}\nadres: {adres[0].text}"
        )
