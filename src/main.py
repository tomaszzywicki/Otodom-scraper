from scraper.settings import BASE_URL, OUTPUT_PATH
from scraper.scraper import scrape_data, scrape_all_listings
import pprint
import time


def main():
    start_time = time.time()
    scrape_all_listings(BASE_URL, OUTPUT_PATH, max_listings=80)
    end_time = time.time()
    print(f"Scraping took {end_time - start_time} seconds")


if __name__ == "__main__":
    main()
