from pyspark.sql.functions import col
from src.etl.udfs import count_kinds_udf


def simplify_dataframe(df):
    return (
        df.select("*")
        .withColumn("lat", col("point").getItem("lat"))
        .withColumn("lon", col("point").getItem("lon"))
        .drop("point")
    )


def skyscraper_filtering(df):
    return df.filter(col("kinds").contains("skyscrapers"))


def add_kinds_count(df):
    return df.withColumn("kinds_amount", count_kinds_udf(col("kinds")))


def get_details_info_dataframe(df):
    clean_details_data_df = df.select(
        col("xid"),
        col("result.stars").alias("stars"),
        col("result.wikipedia").alias("wikipedia"),
        col("result.image").alias("image"),
        col("result.url").alias("url"),
        col("result.address.road").alias("road"),
        col("result.address.house_number").alias("house_number"),
        col("result.address.suburb").alias("suburb"),
        col("result.address.city").alias("city"),
        col("result.address.postcode").alias("postcode"),
        col("result.address.county").alias("county"),
        col("result.address.state").alias("state"),
        col("result.address.country").alias("country"),
    )
    return clean_details_data_df


def enrich_df_details(df, details_df):
    return df.join(details_df, ["xid"], "left")
