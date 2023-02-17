from pyspark.sql import SparkSession
from src.etl.transformers import (
    simplify_dataframe,
    skyscraper_filtering,
    add_kinds_count,
    get_details_info_dataframe,
    enrich_df_details,
)
from src.etl.data_loaders import data_loader, load_details_data


def run():
    spark = SparkSession.builder.appName("amenitizTest").getOrCreate()

    df = data_loader(spark)
    df_simplified = simplify_dataframe(df)

    df_skyscraper_filtered = skyscraper_filtering(df_simplified)
    df_kinds_counted = add_kinds_count(df_skyscraper_filtered)

    details_data_df = load_details_data(df_kinds_counted)

    df_details = get_details_info_dataframe(details_data_df)

    df_enriched = enrich_df_details(df_kinds_counted, df_details)

    df_enriched.write.csv("data/places_output.csv", header=True)

    spark.stop()


if __name__ == "__main__":
    run()
