#!/usr/bin/env python
# coding: utf-8

import pandas as pd
from sqlalchemy import create_engine
from tqdm.auto import tqdm
import click

GREEN_DTYPES = {
    "VendorID": "Int64",
    "store_and_fwd_flag": "string",
    "RatecodeID": "float64",
    "PULocationID": "Int64",
    "DOLocationID": "Int64",
    "passenger_count": "float64",
    "trip_distance": "float64",
    "fare_amount": "float64",
    "extra": "float64",
    "mta_tax": "float64",
    "tip_amount": "float64",
    "tolls_amount": "float64",
    "ehail_fee": "float64",
    "improvement_surcharge": "float64",
    "total_amount": "float64",
    "payment_type": "float64",
    "trip_type": "float64",
    "congestion_surcharge": "float64",
    "cbd_congestion_fee": "float64"
}

GREEN_DATETIME_COLS = [
    "lpep_pickup_datetime",
    "lpep_dropoff_datetime"
]

ZONE_DTYPES = {
    "LocationID": "Int64",
    "Borough": "string",
    "Zone": "string",
    "service_zone": "string"
}

@click.command()
@click.option('--pg-user', default='root')
@click.option('--pg-pass', default='root')
@click.option('--pg-host', default='localhost')
@click.option('--pg-port', default='5432')
@click.option('--pg-db', default='ny_taxi')
@click.option('--chunksize', default=10000)
def main(pg_user, pg_pass, pg_host, pg_port, pg_db, chunksize):

    engine = create_engine(
        f'postgresql://{pg_user}:{pg_pass}@{pg_host}:{pg_port}/{pg_db}'
    )

    # --------------------------------------------------
    # Load GREEN taxi Parquet
    # --------------------------------------------------
    parquet_url = (
        "data/green_tripdata_2025-11.parquet"
    )

    print("Reading green taxi Parquet...")
    df = pd.read_parquet(parquet_url)

    # Apply dtypes
    df = df.astype(GREEN_DTYPES)
    for col in GREEN_DATETIME_COLS:
        df[col] = pd.to_datetime(df[col])

    print("Writing green_taxi_data table...")
    df.head(0).to_sql(
        "green_taxi_data",
        engine,
        if_exists="replace"
    )

    for i in tqdm(range(0, len(df), chunksize)):
        df.iloc[i:i + chunksize].to_sql(
            "green_taxi_data",
            engine,
            if_exists="append",
            index=False
        )

    # --------------------------------------------------
    # Load taxi zone lookup CSV
    # --------------------------------------------------
    zone_url = (
        "data/taxi_zone_lookup.csv"
    )

    print("Reading taxi zone lookup CSV...")
    df_zones = pd.read_csv(zone_url, dtype=ZONE_DTYPES)

    print("Writing taxi_zone_lookup table...")
    df_zones.to_sql(
        "taxi_zone_lookup",
        engine,
        if_exists="replace",
        index=False
    )

    print("✅ Ingestion completed successfully")

if __name__ == "__main__":
    main()
