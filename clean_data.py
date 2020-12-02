import sys
assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+
from pyspark.sql import SparkSession, functions, types, Row
#clean the movies data

#release_date may have problem double check
movie_schema = types.StructType(
    [
        types.StructField("adult", types.BooleanType()),
        types.StructField("belongs_to_collection", types.MapType(types.IntegerType(), types.StringType(), types.StringType(), types.StringType(), types.StringType())),
        #types.StructField("belongs_to_collection", types.StringType()),
        types.StructField("budget", types.IntegerType()),
        #types.StructField("genres", types.ArrayType(types.MapType(types.IntegerType(), types.StringType()))),
        types.StructField("genres", types.StringType()),
        types.StructField("homepage", types.StringType()),
        types.StructField("id", types.IntegerType()),
        types.StructField("imdb_id", types.StringType()),
        types.StructField("original_language", types.StringType()),
        types.StructField("poster_path", types.StringType()),
        types.StructField("release_date", types.TimestampType()),
        types.StructField("revenue", types.IntegerType()),
        types.StructField("runtime", types.IntegerType()),
        #types.StructField("spoken_languages", types.ArrayType(types.MapType(types.StringType(), types.StringType()))),
        types.StructField("spoken_languages", types.StringType()),
        types.StructField("status", types.StringType()),
        types.StructField("tagline", types.StringType()),
        types.StructField("title", types.StringType()),
        types.StructField("video", types.BooleanType()),
        types.StructField("vote_average", types.FloatType()),
        types.StructField("vote_count", types.IntegerType()),
    ]
)

def main(inputs, output):
    movie_data = spark.read.csv(inputs, sep=",", schema=movie_schema)
    movie_data.show(20)

if __name__ == '__main__':
    inputs = sys.argv[1]
    outputs = sys.argv[2]
    spark = SparkSession.builder.appName("correlate_logs").getOrCreate()
    assert spark.version >= "2.4"  # make sure we have Spark 2.4+
    spark.sparkContext.setLogLevel("WARN")
    sc = spark.sparkContext
    main(inputs, outputs)