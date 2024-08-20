from scraper.settings import BASE_URL
from scraper.scraper import scrape_data
import pprint


def main():
    data = scrape_data(BASE_URL)
    pprint.pprint(data)


if __name__ == "__main__":
    main()
