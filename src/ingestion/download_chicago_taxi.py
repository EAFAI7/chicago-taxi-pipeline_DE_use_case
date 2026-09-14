"""
Télécharge une tranche du dataset "Chicago Taxi Trips" via l'API SODA
(Socrata Open Data API) et écrit les résultats bruts en JSON, un fichier
par page, dans data/raw/.

Le dataset complet fait plusieurs dizaines de Go : on ne le télécharge
jamais en entier. On découpe le téléchargement JOUR PAR JOUR : chaque
requête ne porte que sur une seule journée, avec une clause `$where`
simple. Le decoupage quotidien maintient des clauses `$where` simples et rapides, quelle que soit la duree totale
de la periode demandee.

A l'intérieur d'une même journée, si le volume dépasse PAGE_SIZE, on pagine
avec `$offset` classique : le volume journalier est assez petit pour que ça
reste performant (contrairement à un offset sur plusieurs mois de données).

Utilise le client officiel `sodapy`, sans app token (app_token=None est
supporté nativement, cf. https://dev.socrata.com/docs/app-tokens.html) :
le throttling est juste un peu plus bas, ce qui est suffisant pour la
volumétrie de ce test technique.

Variables d'environnement:
    SODA_START_DATE   date de début (incluse), format YYYY-MM-DD
    SODA_END_DATE     date de fin (exclue), format YYYY-MM-DD
    SODA_APP_TOKEN    token d'app Socrata (optionnel, laisser vide par défaut)
    DATA_DIR          racine du datalake local (defaut: /opt/airflow/data)

Dataset: https://data.cityofchicago.org/Transportation/Taxi-Trips-2013-2023-/wrvz-psew
"""
import json
import os
import time
from datetime import date, datetime, timedelta
from pathlib import Path

from sodapy import Socrata

DATASET_ID = "wrvz-psew"
DOMAIN = "data.cityofchicago.org"
PAGE_SIZE = 25_000  # tres rarement atteint : le volume est decoupe par jour
MAX_RETRIES = 6
REQUEST_TIMEOUT = 120


def _daterange(start: date, end: date):
    """Génère chaque date de `start` (inclue) & `end` (exclue)."""
    d = start
    while d < end:
        yield d
        d += timedelta(days=1)


def _get_page(client: Socrata, where_clause: str, limit: int, offset: int) -> list[dict]:
    """Récupère une page pour une journée donnée, avec retry/backoff."""
    last_error = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            return client.get(
                DATASET_ID,
                where=where_clause,
                order="trip_start_timestamp,trip_id",
                limit=limit,
                offset=offset,
            )
        except Exception as exc:  # noqa: BLE001 - sodapy peut afficher des erreurs variées (requests, HTTPError, 0 ligne)
            last_error = exc
            wait = min(2 ** attempt, 60)
            print(f"[download] tentative {attempt}/{MAX_RETRIES} échouée ({exc}), retry dans {wait}s")
            time.sleep(wait)
    raise RuntimeError(f"Echec du téléchargement après {MAX_RETRIES} tentatives") from last_error


def download(start_date: str, end_date: str, out_dir: Path, app_token: str | None = None) -> int:
    out_dir.mkdir(parents=True, exist_ok=True)

    start = datetime.strptime(start_date, "%Y-%m-%d").date()
    end = datetime.strptime(end_date, "%Y-%m-%d").date()

    client = Socrata(DOMAIN, app_token, timeout=REQUEST_TIMEOUT)

    page_num = 0
    total_rows = 0

    try:
        for day in _daterange(start, end):
            day_str = day.isoformat()
            next_day_str = (day + timedelta(days=1)).isoformat()
            where_clause = (
                f"trip_start_timestamp >= '{day_str}T00:00:00.000' "
                f"AND trip_start_timestamp < '{next_day_str}T00:00:00.000'"
            )

            offset = 0
            while True:
                rows = _get_page(client, where_clause, PAGE_SIZE, offset)
                if not rows:
                    break

                out_file = out_dir / f"trips_page_{page_num:05d}.json"
                with out_file.open("w", encoding="utf-8") as f:
                    json.dump(rows, f)

                n = len(rows)
                total_rows += n
                print(f"[download] {day_str} (offset {offset}): {n} lignes -> {out_file}")

                page_num += 1
                if n < PAGE_SIZE:
                    break

                offset += PAGE_SIZE
                time.sleep(0.3)

            time.sleep(0.3)  # petite pause à chaque extraction d'une journée
    finally:
        client.close()

    print(f"[download] terminé: {total_rows} lignes sur [{start_date}, {end_date}[")
    return total_rows


def main():
    start_date = os.environ.get("SODA_START_DATE", "2023-01-01")
    end_date = os.environ.get("SODA_END_DATE", "2023-07-01")
    app_token = os.environ.get("SODA_APP_TOKEN") or None
    data_dir = Path(os.environ.get("DATA_DIR", "/opt/airflow/data"))

    raw_dir = data_dir / "raw"
    total = download(start_date, end_date, raw_dir, app_token)

    if total == 0:
        raise RuntimeError(
            "Aucune ligne récupérée : vérifier les dates ou la disponibilité de l'API SODA."
        )


if __name__ == "__main__":
    main()
