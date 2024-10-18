import pendulum
from airflow.decorators import dag, task

from steps.messages import send_telegram_success_message, send_telegram_failure_message


@dag(
    schedule="@once",
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    catchup=False,
    tags=["Flats"],
    on_success_callback=send_telegram_success_message,
    on_failure_callback=send_telegram_failure_message,
)
def flats_get():
    import pandas as pd
    from airflow.providers.postgres.hooks.postgres import PostgresHook

    @task()
    def create_table():
        from sqlalchemy import (
            MetaData,
            UniqueConstraint,
            Table,
            BigInteger,
            Column,
            Integer,
            Float,
            Boolean,
        )

        table_name = "flats_churn"
        hook = PostgresHook("database")
        engine = hook.get_sqlalchemy_engine()
        metadata = MetaData()
        flats_table = Table(
            table_name,
            metadata,
            Column("flat_id", Integer, primary_key=True, autoincrement=True),
            Column("floor", Integer),
            Column("apartment", Boolean),
            Column("kitchen_area", Float),
            Column("living_area", Float),
            Column("rooms", Integer),
            Column("studio", Boolean),
            Column("total_area", Float),
            Column("price", BigInteger),
            Column("build_id", Integer),
            Column("build_year", Integer),
            Column("build_type", Integer),
            Column("latitude", Float),
            Column("longitude", Float),
            Column("ceiling_height", Float),
            Column("flats_count", Integer),
            Column("floors_total", Integer),
            Column("has_elevator", Boolean),
            UniqueConstraint("flat_id", name="unique_all_flats_constraint"),
        )

        flats_table.drop(engine, checkfirst=True)
        flats_table.create(engine)

    @task()
    def extract(**kwargs):
        hook = PostgresHook("database")
        conn = hook.get_conn()
        sql = """
        select f.id flat_id,f.floor,f.is_apartment apartment,f.kitchen_area,f.living_area,f.rooms,
        f.studio,f.total_area,f.price,f.building_id build_id,
        b.build_year,b.building_type_int build_type,b.latitude,b.longitude,
        b.ceiling_height,b.flats_count,b.floors_total,b.has_elevator
        from flats as f
        join buildings as b on f.building_id=b.id
        """
        data = pd.read_sql(sql, conn)
        conn.close()
        return data

    @task()
    def transform(data: pd.DataFrame):
        def remove_duplicates(data: pd.DataFrame):
            feature_cols = data.columns.tolist()
            is_duplicated_features = data.duplicated(subset=feature_cols, keep=False)
            data = pd.DataFrame(data[~is_duplicated_features].reset_index(drop=True))
            return data

        def remove_low_price_rows(data: pd.DataFrame):
            data = pd.DataFrame(data[data["price"] > 1100000])
            return data

        def remove_high_price_rows(data: pd.DataFrame) -> pd.DataFrame:
            data = pd.DataFrame(data[data["price"] < 500000000])
            return data

        def remove_outliers_iqr(data: pd.DataFrame):
            filtered_df = data.copy()
            num_cols = data.select_dtypes(["float", "int"]).columns
            threshold = 1.5
            for column in num_cols:
                Q1 = data[column].quantile(0.25)
                Q3 = data[column].quantile(0.75)
                IQR = Q3 - Q1

                lower_bound = Q1 - threshold * IQR
                upper_bound = Q3 + threshold * IQR

                filtered_df = pd.DataFrame(
                    filtered_df[
                        (filtered_df[column] >= lower_bound)
                        & (filtered_df[column] <= upper_bound)
                    ]
                )

            return filtered_df

        data = data.drop(columns=['id','studio','building_id'])
        data = remove_duplicates(data)
        data = remove_low_price_rows(data)
        data = remove_high_price_rows(data)
        data = remove_outliers_iqr(data)
        return data

    @task()
    def load(data: pd.DataFrame):
        hook = PostgresHook("database")
        hook.insert_rows(
            table="flats_churn",
            # replace=True,
            target_fields=data.columns.tolist(),
            # replace_index=["flat_id"],
            rows=data.values.tolist(),
        )

    create_table()
    extracted_data = extract()
    # transformed_data = transform(extracted_data)
    load(extracted_data)


flats_get()
