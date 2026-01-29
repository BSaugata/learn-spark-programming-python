from pyspark import SparkConf
from pyspark.sql import *
from pyspark.sql.types import *
from pyspark.sql.functions import *


def ingest_movies_file(source_file: str, output_file: str):
    print(f"source_file: {source_file}, output_file:{output_file}")

    s_config = SparkConf().setAppName("What to binge").setMaster("local[2]")
    spark = (
        SparkSession.builder.config(conf=s_config).getOrCreate()
    )
    schema = StructType(
        [
            StructField("movie_id", StringType(), True),
            StructField("title", StringType(), True),
            StructField("genre", StringType(), True),
            StructField("release_year", IntegerType(), True),
            StructField("duration", IntegerType(), True),
        ]
    )
    movies_df = spark.read.format("json").schema(schema).load(source_file)
    movies_df.show(10, truncate=False)

    movies_df.write.format("parquet").mode("overwrite").save(output_file)


if __name__ == "__main__":
    ingest_movies_file(
        source_file="dataset/input/movies.json",
        output_file="dataset/output",
    )
