from scrapers.classes.MieszkanieWynajem import MieszkanieWynajem


def main():
    mieszkanie_scraper = MieszkanieWynajem(voividoship="mazowieckie", city="warszawa")
    mieszkanie_scraper.scrape_listings(10)


if __name__ == "__main__":
    main()
