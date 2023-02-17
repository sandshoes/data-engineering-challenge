from pyspark.sql.functions import udf
from pyspark.sql.types import (
    IntegerType,
    StructType,
    StructField,
    StringType,
    DecimalType,
)
import requests

API_KEY = "<YOUR_API_KEY>"
LANGUAGE = "en"

details_schema = StructType(
    [
        StructField("stars", StringType(), True),
        StructField("wikipedia", StringType(), True),
        StructField("url", StringType(), True),
        StructField(
            "address",
            StructType(
                [
                    StructField("city", StringType(), True),
                    StructField("road", StringType(), True),
                    StructField("state", StringType(), True),
                    StructField("county", StringType(), True),
                    StructField("suburb", StringType(), True),
                    StructField("country", StringType(), True),
                    StructField("postcode", StringType(), True),
                    StructField("house_number", StringType(), True),
                ]
            ),
            True,
        ),
        StructField("image", StringType(), True),
    ]
)


def execute_rest_api(xid):
    details_url = "https://api.opentripmap.com/0.1/{}/places/xid/{}?apikey={}".format(
        LANGUAGE, xid, API_KEY
    )
    res = requests.get(details_url)
    return res.json()


count_kinds_udf = udf(lambda x: (x.count(",") + 1), IntegerType())
udf_execute_rest_api = udf(execute_rest_api, details_schema)
