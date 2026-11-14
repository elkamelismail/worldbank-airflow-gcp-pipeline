"""
Test local autonome de l'extraction World Bank, sans Airflow ni GCP.

Exécution : python test_extraction.py
Dépendance : pip install requests
"""

import requests

BASE_URL = "https://api.worldbank.org/v2"


def extract_worldbank_data(countries, indicators, load_date):
    rows = []

    for country in countries:
        for indicator in indicators:
            page = 1
            while True:
                url = f"{BASE_URL}/country/{country}/indicator/{indicator}"
                response = requests.get(
                    url,
                    params={"format": "json", "page": page, "per_page": 1000},
                    timeout=30,
                )
                response.raise_for_status()
                data = response.json()

                # L'API peut retourner HTTP 200 avec un message d'erreur JSON.
                if (
                    not isinstance(data, list)
                    or len(data) != 2
                    or not isinstance(data[0], dict)
                    or "message" in data[0]
                    or not isinstance(data[1], list)
                ):
                    raise ValueError(
                        f"World Bank API error ({country}, {indicator}): {data}"
                    )

                metadata, observations = data
                for obs in observations:
                    if obs.get("value") is None:
                        continue
                    rows.append(
                        {
                            "country_code": obs["countryiso3code"],
                            "country_name": obs["country"]["value"],
                            "indicator_code": obs["indicator"]["id"],
                            "indicator_label": obs["indicator"]["value"],
                            "year": int(obs["date"]),
                            "value": float(obs["value"]),
                            "load_date": load_date,
                        }
                    )

                if page >= int(metadata["pages"]):
                    break
                page += 1

    if not rows:
        raise ValueError("No World Bank observations extracted")
    return rows


if __name__ == "__main__":
    try:
        rows = extract_worldbank_data(
            countries=["MAR"],
            indicators=["NY.GDP.MKTP.CD"],
            load_date="2026-11-14",
        )
        print(f"Nombre de lignes : {len(rows)}")
        print(f"Première observation : {rows[0]}")
    except (requests.RequestException, ValueError, KeyError, TypeError) as exc:
        print(f"Échec du test : {type(exc).__name__}: {exc}")
        raise
