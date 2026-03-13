import pandas as pd
import requests

from src.utils.config_loader import load_config
from src.utils.logger import get_logger


logger = get_logger(__name__)
config = load_config()

BASE_CURRENCY = config["pipeline"]["base_currency"]
FX_API_BASE_URL = config["fx_api"]["base_url"]
FX_START_DATE = config["fx_api"]["start_date"]
FX_END_DATE = config["fx_api"]["end_date"]
FX_CURRENCIES = config["fx_api"]["currencies"]


def fetch_fx_rates_from_api() -> pd.DataFrame:
    symbols = ",".join([c for c in FX_CURRENCIES if c != BASE_CURRENCY])

    url = (
        f"{FX_API_BASE_URL}/{FX_START_DATE}..{FX_END_DATE}"
        f"?from={BASE_CURRENCY}&to={symbols}"
    )

    logger.info("Fetching FX rates from API: %s", url)

    response = requests.get(url, timeout=30)
    response.raise_for_status()

    payload = response.json()
    rates_by_date = payload.get("rates", {})

    rows = []

    for rate_date, daily_rates in rates_by_date.items():
        rows.append(
            {
                "rate_date": str(rate_date),
                "currency": BASE_CURRENCY,
                "rate_to_cad": 1.0,
            }
        )

        for currency, rate_from_cad in daily_rates.items():
            if rate_from_cad in (0, None):
                continue

            rate_to_cad = round(1 / rate_from_cad, 6)

            rows.append(
                {
                    "rate_date": str(rate_date),
                    "currency": currency,
                    "rate_to_cad": rate_to_cad,
                }
            )

    fx_df = (
        pd.DataFrame(rows)
        .sort_values(["rate_date", "currency"])
        .reset_index(drop=True)
    )

    logger.info("Fetched %s FX rows from API", len(fx_df))

    return fx_df