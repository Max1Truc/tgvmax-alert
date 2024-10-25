from datetime import datetime
from threading import Thread
import logging
import time

import requests as r
import duckdb
import pandas as pd

TGVMAX_EXPORT_URL = "https://ressources.data.sncf.com/api/explore/v2.1/catalog/datasets/tgvmax/exports/parquet"
# TGVMAX_EXPORT_URL = "./tgvmax.parquet"

logger = logging.getLogger(__name__)


def is_recent(con):
    try:
        return con.sql(
            """
            SELECT CURRENT_TIMESTAMP - MAX(scrape_datetime)
                    < INTERVAL 1 hour
            FROM tgvmax
            GROUP BY scrape_datetime
            ORDER BY scrape_datetime DESC
            LIMIT 1
            """
        ).fetchone()[0]
    except duckdb.CatalogException:
        return False


def fetch(con):
    logger.info("fetching current data from SNCF")
    # using pandas because, last time I checked,
    # duckdb did two requests instead of one
    raw_todays_dataset = pd.read_parquet(TGVMAX_EXPORT_URL)

    logger.info("storing and sorting through data from SNCF")
    todays_dataset = con.sql(
        """
        SELECT CURRENT_TIMESTAMP as scrape_datetime, *
        FROM raw_todays_dataset
        """
    )

    con.execute(
        """
        -- create table like the parquet file
        CREATE TABLE IF NOT EXISTS tgvmax
        AS SELECT * FROM todays_dataset LIMIT 0;

        -- create view with only the latest entry for each train
        CREATE OR REPLACE VIEW tgvmax_latest
        AS SELECT * FROM tgvmax a
        WHERE scrape_datetime = (
            SELECT MAX(scrape_datetime)
            FROM tgvmax
            WHERE date = a.date
            AND train_no = a.train_no
            AND entity = a.entity
            AND axe = a.axe
            AND origine_iata = a.origine_iata
            AND destination_iata = a.destination_iata
            AND origine = a.origine
            AND destination = a.destination
            AND heure_depart = a.heure_depart
            AND heure_arrivee = a.heure_arrivee
        );

        -- insert data from the parquet file
        INSERT INTO tgvmax
        SELECT * FROM todays_dataset a
        WHERE NOT EXISTS(
            SELECT *
            FROM tgvmax_latest
            WHERE date = a.date
            AND train_no = a.train_no
            AND entity = a.entity
            AND axe = a.axe
            AND origine_iata = a.origine_iata
            AND destination_iata = a.destination_iata
            AND origine = a.origine
            AND destination = a.destination
            AND heure_depart = a.heure_depart
            AND heure_arrivee = a.heure_arrivee
            AND od_happy_card = a.od_happy_card
        );
        """
    )


def backup(con):
    logger.info("exporting data as parquet files")
    con.table("tgvmax").write_parquet(
        "./.data/public/tgvmax_full.parquet", compression="zstd"
    )
    con.table("tgvmax_latest").write_parquet(
        "./.data/public/tgvmax_latest.parquet", compression="zstd"
    )
    con.sql(
        """
        COPY (
            SELECT *
            FROM tgvmax
            WHERE scrape_datetime = (
                SELECT scrape_datetime
                FROM tgvmax
                ORDER BY scrape_datetime DESC
                LIMIT 1
            )
        )
        TO './.data/public/tgvmax_latest_incremental.parquet' (
            FORMAT PARQUET,
            COMPRESSION ZSTD,
            OVERWRITE_OR_IGNORE
        );
        """
    )
    con.sql(
        """
        COPY (
            SELECT strftime(scrape_datetime, '%Y-%m-%d') AS scrape_date, *
            FROM tgvmax
        )
        TO './.data/public/incremental/' (
            FORMAT PARQUET,
            PARTITION_BY (scrape_date),
            COMPRESSION ZSTD,
            OVERWRITE_OR_IGNORE
        );
        """
    )


def update():
    logger.info("starting update")
    con = duckdb.connect("./.data/db.duckdb")
    if not is_recent(con):
        fetch(con)
        backup(con)
    con.close()
    logger.info("update done")


def update_loop():
    while True:
        update()
        time.sleep(3600)


def update_loop_in_thread():
    t = Thread(target=update_loop)
    t.start()
