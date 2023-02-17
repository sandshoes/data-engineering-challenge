import requests
from pyspark.sql.functions import col
from src.etl.udfs import udf_execute_rest_api
from pyspark.sql.types import (
    IntegerType,
    StructType,
    StructField,
    StringType,
    FloatType,
    DecimalType,
)

API_KEY = '5ae2e3f221c38a28845f05b6870f119cd2582d880396d265508a42cc'
LANGUAGE = "en"
LON_LIST = [2.028471, 2.113615, 2.198759, 2.283903]
LAT_LIST = [41.315758, 41.383763, 41.451768]
KINDS = "accomodations"
FORMAT = "json"

schema = StructType(
    [
        StructField("xid", StringType()),
        StructField("name", StringType(), True),
        StructField("osm", StringType(), True),
        StructField("rate", IntegerType(), True),
        StructField("wikidata", StringType(), True),
        StructField("kinds", StringType(), True),
        StructField(
            "point",
            StructType(
                [
                    StructField("lat", FloatType(), True),
                    StructField("lon", FloatType(), True),
                ]
            ),
            True,
        ),
    ]
)


def fetch_initial_data():
    json_data = []
    for i in range(len(LAT_LIST[:-1])):
        min_lat = LAT_LIST[i]
        max_lat = LAT_LIST[i + 1]
        for j in range(len(LON_LIST[:-1])):
            min_lon = LON_LIST[j]
            max_lon = LON_LIST[j + 1]
            api_url = "https://api.opentripmap.com/0.1/{}/places/bbox?lon_min={}&lat_min={}&lon_max={}&lat_max={}&kinds={}&format={}&apikey={}".format(
                LANGUAGE, min_lon, min_lat, max_lon, max_lat, KINDS, FORMAT, API_KEY
            )
            headers = {"Content-Type": "application/json; charset=UTF-8"}
            data = requests.get(api_url, headers=headers)
            if len(data.json()) > 0:
                json_data += data.json()
    return json_data


def data_loader(spark):
    json_data = fetch_initial_data()
    return spark.createDataFrame(json_data, schema=schema)

def load_details_data(df):
    return df.select("xid").withColumn("result", udf_execute_rest_api(col("xid")))
