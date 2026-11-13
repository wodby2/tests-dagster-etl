"""Loads the European Central Bank's daily euro exchange rates into the warehouse.

exchange_rates is a daily partitioned asset: each partition is one day's rates,
upserted into PostgreSQL. monthly_rates is built from it. A schedule
materializes yesterday's partition every day, and older days can be backfilled
from Dagster's interface.
"""

import dagster as dg
import psycopg2
import requests

CURRENCIES = ["USD", "GBP", "JPY", "CHF", "CNY"]

daily = dg.DailyPartitionsDefinition(start_date="2026-11-01")


class Warehouse(dg.ConfigurableResource):
    """The PostgreSQL database the pipeline loads into."""

    url: str

    def connect(self):
        return psycopg2.connect(self.url)


@dg.asset(
    partitions_def=daily,
    retry_policy=dg.RetryPolicy(max_retries=3, delay=300),
    kinds={"postgres"},
)
def exchange_rates(context: dg.AssetExecutionContext, warehouse: Warehouse) -> dg.MaterializeResult:
    """One row per currency and day, from the Frankfurter API."""
    day = context.partition_key
    response = requests.get(
        f"https://api.frankfurter.dev/v1/{day}",
        params={"symbols": ",".join(CURRENCIES)},
        timeout=30,
    )
    response.raise_for_status()
    payload = response.json()
    # The ECB publishes no rates on weekends and holidays; the API then returns
    # the last business day, which its own partition already holds.
    rows = [] if payload["date"] != day else [(day, c, r) for c, r in sorted(payload["rates"].items())]

    with warehouse.connect() as conn, conn.cursor() as cur:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS exchange_rates (
                day date NOT NULL,
                currency char(3) NOT NULL,
                rate numeric(18, 6) NOT NULL,
                PRIMARY KEY (day, currency)
            )
            """
        )
        # Upserting makes reruns and backfills safe.
        cur.executemany(
            """
            INSERT INTO exchange_rates (day, currency, rate) VALUES (%s, %s, %s)
            ON CONFLICT (day, currency) DO UPDATE SET rate = EXCLUDED.rate
            """,
            rows,
        )
    return dg.MaterializeResult(metadata={"rows": len(rows)})


@dg.asset(deps=[exchange_rates], kinds={"postgres"})
def monthly_rates(warehouse: Warehouse) -> dg.MaterializeResult:
    """The average rate of every currency per month."""
    with warehouse.connect() as conn, conn.cursor() as cur:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS monthly_rates (
                month date NOT NULL,
                currency char(3) NOT NULL,
                average_rate numeric(18, 6) NOT NULL,
                days integer NOT NULL,
                PRIMARY KEY (month, currency)
            );
            INSERT INTO monthly_rates
            SELECT date_trunc('month', day)::date, currency, avg(rate), count(*)
            FROM exchange_rates
            GROUP BY 1, 2
            ON CONFLICT (month, currency) DO UPDATE
            SET average_rate = EXCLUDED.average_rate, days = EXCLUDED.days
            """
        )
        cur.execute("SELECT count(*) FROM monthly_rates")
        count = cur.fetchone()[0]
    return dg.MaterializeResult(metadata={"rows": count})


daily_rates_job = dg.define_asset_job("daily_rates", selection=[exchange_rates], partitions_def=daily)
monthly_rates_job = dg.define_asset_job("monthly_rates_job", selection=[monthly_rates])

defs = dg.Definitions(
    assets=[exchange_rates, monthly_rates],
    jobs=[daily_rates_job, monthly_rates_job],
    schedules=[
        # Materializes the previous day's partition every day at 06:00 UTC.
        dg.build_schedule_from_partitioned_job(
            daily_rates_job, hour_of_day=6, default_status=dg.DefaultScheduleStatus.RUNNING
        ),
        dg.ScheduleDefinition(
            job=monthly_rates_job, cron_schedule="30 6 * * *", default_status=dg.DefaultScheduleStatus.RUNNING
        ),
    ],
    resources={"warehouse": Warehouse(url=dg.EnvVar("WAREHOUSE_URL"))},
)
