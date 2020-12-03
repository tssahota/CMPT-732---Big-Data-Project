import sys
assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+
from pyspark.sql import SparkSession, functions, types

def main(inputs):
    movie_data = spark.read.parquet(inputs)
    #popularity
    movie_data.describe('popularity').show()
    #movie_data.show(10)
    #get the most popular movie of the last 20 years
    sorted_popularity = movie_data.sort('popularity', ascending=False)
    sorted_popularity.select(sorted_popularity['title'], sorted_popularity['tagline'], sorted_popularity['popularity']).show(10)
if __name__ == '__main__':
    input = sys.argv[1]
    spark = SparkSession.builder.appName("temporal_trend_analysis").getOrCreate()
    assert spark.version >= "2.4"  # make sure we have Spark 2.4+
    spark.sparkContext.setLogLevel("WARN")
    sc = spark.sparkContext
    main(input)