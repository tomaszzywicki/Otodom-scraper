from scraper.settings import BASE_URL, OUTPUT_PATH
from scraper.scraper import scrape_data, scrape_all_listings
import pprint
import time


def main():
    scrape_all_listings(BASE_URL, OUTPUT_PATH, max_listings=5000)


if __name__ == "__main__":
    main()
