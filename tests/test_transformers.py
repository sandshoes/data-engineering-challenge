import unittest
from decimal import Decimal
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    IntegerType,
    StructField,
    StructType,
    StringType,
    DecimalType,
    Row,
)
from src.etl.transformers import (
    simplify_dataframe,
    skyscraper_filtering,
    add_kinds_count,
    get_details_info_dataframe,
    enrich_df_details,
)


class SparkETLTestCase(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = (
            SparkSession.builder.master("local[*]").appName("Unit-tests").getOrCreate()
        )

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_simplify_dataframe(self):
        input_schema = StructType(
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
                            StructField("lat", DecimalType(), True),
                            StructField("lon", DecimalType(), True),
                        ]
                    ),
                    True,
                ),
            ]
        )
        input_data = [
            (
                "W666948775",
                "hotel el castell",
                "way/666948775",
                7,
                "Q65209044",
                "architecture,historical",
                Row(lat=Decimal(41.34), lon=Decimal(2.0440239906311035)),
            ),
            (
                "N1272888981",
                "hotel hesperia barcelona",
                "node/1272888981",
                3,
                "Q3044181",
                "skyscrapers,architecture",
                Row(lat=Decimal(41.3447151184082), lon=Decimal(2.0440239906311035)),
            ),
            (
                "N4362319193",
                "Fairmont Rey Juan Carlos",
                "node/4362319193",
                3,
                None,
                "accomodations,others",
                Row(lat=Decimal(41.3447151184082), lon=Decimal(2.0440239906311035)),
            ),
        ]

        input_df = self.spark.createDataFrame(data=input_data, schema=input_schema)

        expected_schema = StructType(
            [
                StructField("xid", StringType()),
                StructField("name", StringType(), True),
                StructField("osm", StringType(), True),
                StructField("rate", IntegerType(), True),
                StructField("wikidata", StringType(), True),
                StructField("kinds", StringType(), True),
                StructField("lat", DecimalType(), True),
                StructField("lon", DecimalType(), True),
            ]
        )

        expected_data = [
            (
                "W666948775",
                "hotel el castell",
                "way/666948775",
                7,
                "Q65209044",
                "architecture,historical",
                Decimal(41.34),
                Decimal(2.0440239906311035),
            ),
            (
                "N1272888981",
                "hotel hesperia barcelona",
                "node/1272888981",
                3,
                "Q3044181",
                "skyscrapers,architecture",
                Decimal(41.3447151184082),
                Decimal(2.0440239906311035),
            ),
            (
                "N4362319193",
                "Fairmont Rey Juan Carlos",
                "node/4362319193",
                3,
                None,
                "accomodations,others",
                Decimal(41.3447151184082),
                Decimal(2.0440239906311035),
            ),
        ]

        expected_df = self.spark.createDataFrame(
            data=expected_data, schema=expected_schema
        )

        transformed_df = simplify_dataframe(input_df)

        def field_list(fields):
            return (fields.name, fields.dataType, fields.nullable)

        fields1 = [*map(field_list, transformed_df.schema.fields)]
        fields2 = [*map(field_list, expected_df.schema.fields)]
        res = set(fields1) == set(fields2)

        self.assertTrue(res)
        self.assertEqual(
            sorted(expected_df.collect()), sorted(transformed_df.collect())
        )

    def test_skyscraper_filtering(self):
        schema = StructType(
            [
                StructField("xid", StringType()),
                StructField("name", StringType(), True),
                StructField("kinds", StringType(), True),
            ]
        )
        input_data = [
            ("W666948775", "hotel el castell", "architecture,historical"),
            (
                "N1272888981",
                "hotel hesperia barcelona",
                "random,skyscrapers,architecture",
            ),
            (
                "N4362319193",
                "Fairmont Rey Juan Carlos",
                "skyscrapers,accomodations,others",
            ),
        ]

        expected_data = [
            (
                "N1272888981",
                "hotel hesperia barcelona",
                "random,skyscrapers,architecture",
            ),
            (
                "N4362319193",
                "Fairmont Rey Juan Carlos",
                "skyscrapers,accomodations,others",
            ),
        ]

        input_df = self.spark.createDataFrame(data=input_data, schema=schema)

        expected_df = self.spark.createDataFrame(data=expected_data, schema=schema)

        transformed_df = skyscraper_filtering(input_df)

        def field_list(fields):
            return (fields.name, fields.dataType, fields.nullable)

        fields1 = [*map(field_list, transformed_df.schema.fields)]
        fields2 = [*map(field_list, expected_df.schema.fields)]
        res = set(fields1) == set(fields2)

        self.assertTrue(res)
        self.assertEqual(
            sorted(expected_df.collect()), sorted(transformed_df.collect())
        )

    def test_kinds_creation(self):
        input_schema = StructType(
            [
                StructField("xid", StringType()),
                StructField("name", StringType(), True),
                StructField("kinds", StringType(), True),
            ]
        )

        expected_schema = StructType(
            [
                StructField("xid", StringType()),
                StructField("name", StringType(), True),
                StructField("kinds", StringType(), True),
                StructField("kinds_amount", IntegerType(), True),
            ]
        )

        input_data = [
            ("W666948775", "hotel el castell", "skyscrapers,historical"),
            (
                "N1272888981",
                "hotel hesperia barcelona",
                "random,skyscrapers,architecture",
            ),
            (
                "N4362319193",
                "Fairmont Rey Juan Carlos",
                "skyscrapers,accomodations,others",
            ),
        ]

        expected_data = [
            ("W666948775", "hotel el castell", "skyscrapers,historical", 2),
            (
                "N1272888981",
                "hotel hesperia barcelona",
                "random,skyscrapers,architecture",
                3,
            ),
            (
                "N4362319193",
                "Fairmont Rey Juan Carlos",
                "skyscrapers,accomodations,others",
                3,
            ),
        ]

        input_df = self.spark.createDataFrame(data=input_data, schema=input_schema)

        expected_df = self.spark.createDataFrame(
            data=expected_data, schema=expected_schema
        )

        transformed_df = add_kinds_count(input_df)

        def field_list(fields):
            return (fields.name, fields.dataType, fields.nullable)

        fields1 = [*map(field_list, transformed_df.schema.fields)]
        fields2 = [*map(field_list, expected_df.schema.fields)]
        res = set(fields1) == set(fields2)

        self.assertTrue(res)
        self.assertEqual(
            sorted(expected_df.collect()), sorted(transformed_df.collect())
        )

    def test_get_details_into_dataframe(self):
        input_schema = StructType(
            [
                StructField("xid", StringType()),
                StructField(
                    "result",
                    StructType(
                        [
                            StructField("stars", IntegerType(), True),
                            StructField("wikipedia", StringType(), True),
                            StructField("image", StringType(), True),
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
                        ]
                    ),
                ),
            ]
        )

        expected_schema = StructType(
            [
                StructField("xid", StringType()),
                StructField("stars", IntegerType(), True),
                StructField("wikipedia", StringType(), True),
                StructField("image", StringType(), True),
                StructField("url", StringType(), True),
                StructField("road", StringType(), True),
                StructField("house_number", StringType(), True),
                StructField("suburb", StringType(), True),
                StructField("city", StringType(), True),
                StructField("postcode", StringType(), True),
                StructField("county", StringType(), True),
                StructField("state", StringType(), True),
                StructField("country", StringType(), True),
            ]
        )

        input_data = [
            (
                "N1272888981",
                {
                    "stars": 5,
                    "wikipedia": "https://en.wikipedia.org/wiki/Hyatt%20Regency%20Barcelona%20Tower",
                    "image": "random.jpeg",
                    "url": "https://www.booking.com/hotel/es/hesperiatower.html",
                    "address": {
                        "city": "Hospitalet",
                        "road": "Carrer de Jaume Ventura i Tort",
                        "state": "CAT",
                        "county": "BCN",
                        "suburb": "Bellvitge",
                        "country": "ESP",
                        "postcode": "08907",
                        "house_number": "144",
                    },
                },
            ),
        ]

        expected_data = [
            (
                "N1272888981",
                5,
                "https://en.wikipedia.org/wiki/Hyatt%20Regency%20Barcelona%20Tower",
                "random.jpeg",
                "https://www.booking.com/hotel/es/hesperiatower.html",
                "Carrer de Jaume Ventura i Tort",
                "144",
                "Bellvitge",
                "Hospitalet",
                "08907",
                "BCN",
                "CAT",
                "ESP",
            )
        ]

        input_df = self.spark.createDataFrame(data=input_data, schema=input_schema)

        expected_df = self.spark.createDataFrame(
            data=expected_data, schema=expected_schema
        )

        transformed_df = get_details_info_dataframe(input_df)

        def field_list(fields):
            return (fields.name, fields.dataType, fields.nullable)

        fields1 = [*map(field_list, transformed_df.schema.fields)]
        fields2 = [*map(field_list, expected_df.schema.fields)]
        res = set(fields1) == set(fields2)
        self.assertTrue(res)
        self.assertEqual(
            sorted(expected_df.collect()), sorted(transformed_df.collect())
        )

    def test_enrich_with_details(self):
        input_schema = StructType(
            [
                StructField("xid", StringType()),
                StructField("name", StringType(), True),
                StructField("kinds", StringType(), True),
            ]
        )

        join_schema = StructType(
            [
                StructField("xid", StringType()),
                StructField("stars", IntegerType(), True),
                StructField("wikipedia", StringType(), True),
                StructField("image", StringType(), True),
                StructField("url", StringType(), True),
                StructField("address", StringType(), True),
            ]
        )

        expected_schema = StructType(
            [
                StructField("xid", StringType()),
                StructField("name", StringType(), True),
                StructField("kinds", StringType(), True),
                StructField("stars", IntegerType(), True),
                StructField("wikipedia", StringType(), True),
                StructField("image", StringType(), True),
                StructField("url", StringType(), True),
                StructField("address", StringType(), True),
            ]
        )

        input_data = [
            ("W666948775", "hotel el castell", "skyscrapers,historical"),
            (
                "N1272888981",
                "hotel hesperia barcelona",
                "random,skyscrapers,architecture",
            ),
            (
                "N4362319193",
                "Fairmont Rey Juan Carlos",
                "skyscrapers,accomodations,others",
            ),
        ]

        join_data = [
            (
                "W666948775",
                7,
                "wikiurl",
                "jpeg here",
                "url",
                '{"country": "Spain", "road": "calle"}',
            ),
            (
                "N1272888981",
                9,
                "wikiurl",
                "jpeg here",
                "url",
                '{"country": "Italy", "road": "strada"}',
            ),
            (
                "N4362319193",
                10,
                "wikiurl",
                "jpeg here",
                "url",
                '{"country": "France", "road": "rue"}',
            ),
        ]

        expected_data = [
            (
                "W666948775",
                "hotel el castell",
                "skyscrapers,historical",
                7,
                "wikiurl",
                "jpeg here",
                "url",
                '{"country": "Spain", "road": "calle"}',
            ),
            (
                "N1272888981",
                "hotel hesperia barcelona",
                "random,skyscrapers,architecture",
                9,
                "wikiurl",
                "jpeg here",
                "url",
                '{"country": "Italy", "road": "strada"}',
            ),
            (
                "N4362319193",
                "Fairmont Rey Juan Carlos",
                "skyscrapers,accomodations,others",
                10,
                "wikiurl",
                "jpeg here",
                "url",
                '{"country": "France", "road": "rue"}',
            ),
        ]

        input_df = self.spark.createDataFrame(data=input_data, schema=input_schema)

        join_df = self.spark.createDataFrame(data=join_data, schema=join_schema)

        expected_df = self.spark.createDataFrame(
            data=expected_data, schema=expected_schema
        )

        transformed_df = enrich_df_details(input_df, join_df)

        def field_list(fields):
            return (fields.name, fields.dataType, fields.nullable)

        fields1 = [*map(field_list, transformed_df.schema.fields)]
        fields2 = [*map(field_list, expected_df.schema.fields)]
        res = set(fields1) == set(fields2)

        self.assertTrue(res)
        self.assertEqual(
            sorted(expected_df.collect()), sorted(transformed_df.collect())
        )
