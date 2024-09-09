from scrapers.classes.WarszawaMieszkanieWynajem import WarszawaMieszkanieWynajem
import psycopg2


def main():
    mieszkanie_scraper = WarszawaMieszkanieWynajem()
    mieszkanie_scraper.scrape_listings()


if __name__ == "__main__":
    main()
