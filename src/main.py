from scrapers.classes.WarszawaMieszkanieWynajem import WarszawaMieszkanieWynajem
import pandas as pd


def main():
    pd.set_option("future.no_silent_downcasting", True)
    mieszkanie_scraper = WarszawaMieszkanieWynajem()
    mieszkanie_scraper.scrape_listings()


if __name__ == "__main__":
    main()
