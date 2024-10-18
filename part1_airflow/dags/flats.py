import pendulum
from airflow.decorators import dag, task

from steps.messages import send_telegram_success_message, send_telegram_failure_message


@dag(
    schedule="@once",
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    catchup=False,
    tags=["ETL"],
    on_success_callback=send_telegram_success_message,
    on_failure_callback=send_telegram_failure_message,
)
def prepare_flats_dataset():
    import pandas as pd
    import numpy as np
    from airflow.providers.postgres.hooks.postgres import PostgresHook

    @task()
    def create_table():
        from sqlalchemy import (
            MetaData,
            UniqueConstraint,
            Table,
            Column,
            String,
            Integer,
            DateTime,
            Float,
            inspect,
        )

        table_name = "users_churn"
        hook = PostgresHook("database")
        engine = hook.get_sqlalchemy_engine()
        metadata = MetaData()
        table = Table(
            table_name,
            metadata,
            Column("id", Integer, primary_key=True, autoincrement=True),
            Column("customer_id", String),
            Column("begin_date", DateTime),
            Column("end_date", DateTime),
            Column("type", String),
            Column("paperless_billing", String),
            Column("payment_method", String),
            Column("monthly_charges", Float),
            Column("total_charges", Float),
            Column("internet_service", String),
            Column("online_security", String),
            Column("online_backup", String),
            Column("device_protection", String),
            Column("tech_support", String),
            Column("streaming_tv", String),
            Column("streaming_movies", String),
            Column("gender", String),
            Column("senior_citizen", Integer),
            Column("partner", String),
            Column("dependents", String),
            Column("multiple_lines", String),
            Column("target", Integer),
            UniqueConstraint("customer_id", name="unique_customer_constraint"),
        )
        if not inspect(engine).has_table(table_name):
            metadata.create_all(engine)

    @task()
    def extract(**kwargs):
        hook = PostgresHook("database")
        conn = hook.get_conn()
        sql = f"""
        select
            f.*
        from flats as f
        """
        data = pd.read_sql(sql, conn)
        conn.close()
        return data

    @task()
    def transform(data: pd.DataFrame):
        data["target"] = (data["end_date"] != "No").astype(int)
        data["end_date"].replace({"No": None}, inplace=True)
        return data

    @task()
    def load(data: pd.DataFrame):
        hook = PostgresHook("database")
        hook.insert_rows(
            table="users_churn",
            replace=True,
            target_fields=data.columns.tolist(),
            replace_index=["customer_id"],
            rows=data.values.tolist(),
        )

    create_table()
    data = extract()
    # transformed_data = transform(data)
    # load(transformed_data)


prepare_flats_dataset()
