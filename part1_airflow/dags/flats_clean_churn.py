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
def flats_clean_dataset():
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

        table_name = "clean_flats_churn"
        hook = PostgresHook("database")
        engine = hook.get_sqlalchemy_engine()
        metadata = MetaData()
        flats_table = Table(
            table_name,
            metadata,
            Column("id", Integer, primary_key=True, autoincrement=True),
            Column("floor", Integer),
            Column("apartment", Integer),
            Column("kitchen_area", Float),
            Column("living_area", Float),
            Column("rooms", Integer),
            Column("studio", Integer),
            Column("total_area", Float),
            Column("price", BigInteger),
            Column("build_year", Integer),
            Column("build_type", Integer),
            Column("latitude", Float),
            Column("longitude", Float),
            Column("ceiling_height", Float),
            Column("flats_count", Integer),
            Column("floors_total", Integer),
            Column("has_elevator", Integer),
            UniqueConstraint("id", name="unique_cleaned_flats_constraint"),
        )

        flats_table.drop(engine, checkfirst=True)
        flats_table.create(engine)

    @task()
    def extract(**kwargs):
        hook = PostgresHook("database")
        conn = hook.get_conn()
        sql = """
        select f.flat_id,f.floor,f.apartment,f.kitchen_area,f.living_area,f.rooms,
        f.studio,f.total_area,f.price,f.build_id,
        f.build_year,f.build_type,f.latitude,f.longitude,
        f.ceiling_height,f.flats_count,f.floors_total,f.has_elevator
        from flats_churn as f
        """
        data = pd.read_sql(sql, conn)
        conn.close()
        return data

    @task()
    def transform(data: pd.DataFrame):
        def bool_to_int(data: pd.DataFrame):
            data[data.select_dtypes(include="bool").columns] = data.select_dtypes(
                include="bool"
            ).astype(int)
            return data

        def remove_duplicates(data: pd.DataFrame):
            feature_cols = data.columns.tolist()
            is_duplicated_features = data.duplicated(subset=feature_cols, keep=False)
            data = pd.DataFrame(data[~is_duplicated_features].reset_index(drop=True))
            return data

        def remove_low_price_rows(data: pd.DataFrame):
            """Удаляем квартиры с явно неверной ценой менее 99'999"""
            data = pd.DataFrame(data[data["price"] > 99999])
            return data

        def remove_high_price_rows(data: pd.DataFrame) -> pd.DataFrame:
            """Удаляем квартиры с ценой более 1'000'000'000"""
            data = pd.DataFrame(data[data["price"] < 1000000000])
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

        data = data.drop(columns=["flat_id", "build_id"])
        data = remove_duplicates(data)
        data = remove_low_price_rows(data)
        # data = remove_high_price_rows(data)
        data = remove_outliers_iqr(data)
        data = bool_to_int(data)
        return data

    @task()
    def load(data: pd.DataFrame):
        hook = PostgresHook("database")
        hook.insert_rows(
            table="clean_flats_churn",
            replace=True,
            target_fields=data.columns.tolist(),
            replace_index=["id"],
            rows=data.values.tolist(),
        )

    create_table()
    extracted_data = extract()
    transformed_data = transform(extracted_data)
    load(transformed_data)


flats_clean_dataset()
