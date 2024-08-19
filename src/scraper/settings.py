TIMEOUT = 10
offer_type = "wynajem"
house_type = "mieszkanie"  # mieszkanie lub dom
wojewodztwo = "mazowieckie"
city = "warszawa"

BASE_URL = (
    # f"https://www.otodom.pl/pl/wyniki/{offer_type}/{house_type}/{wojewodztwo}/{city}"
    "https://www.otodom.pl/pl/oferta/apartament-3-pokojowy-w-nowym-bloku-ul-sielcka-ID4s57N"
)

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36"
}
