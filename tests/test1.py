import os
from dotenv import load_dotenv
import finnhub

load_dotenv()

API_KEY = os.getenv("API_KEY")


def extract_news(company: str, start_date: str, end_date: str):
    if not API_KEY:
        raise RuntimeError("API_KEY missing in environment/.env")

    client = finnhub.Client(api_key=API_KEY)
    return client.company_news(company, _from=start_date, to=end_date)


if __name__ == "__main__":
    # Use a recent range; free Finnhub keys often only return ~12 months of news
    data = extract_news("AAPL", "2026-02-01", "2026-03-03")
    print(f"Fetched {len(data)} articles")
    for item in data[:3]:
        print(f"- {item.get('datetime')} | {item.get('headline')}")
