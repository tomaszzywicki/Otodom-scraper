from scraper.settings import BASE_URL
from scraper.scraper import scrape_data


def main():
    data = scrape_data(BASE_URL)


if __name__ == "__main__":
    main()
