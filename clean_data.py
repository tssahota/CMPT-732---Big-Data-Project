import sys
assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+
from pyspark.sql import SparkSession, functions, types, Row
#clean the movies data

# movie_schema = types.StructType(
#     [
#         types.StructField("adult", types.StringType()),
#         types.StructField("belongs_to_collection", types.StringType()),
#         types.StructField("budget", types.StringType()),
#         types.StructField("genres", types.StringType()),     
#         types.StructField("homepage", types.StringType()),
#         types.StructField("id", types.StringType()),
#         types.StructField("imdb_id", types.StringType()),
#         types.StructField("original_language", types.StringType()),
#         types.StructField("original_title", types.StringType()),
#         types.StructField("overview", types.StringType()),
#         types.StructField("popularity", types.StringType()),
#         types.StructField("poster_path", types.StringType()),
#         types.StructField("production_companies", types.StringType()),
#         types.StructField("production_countries", types.StringType()),
#         types.StructField("release_date", types.StringType()),
#         types.StructField("revenue", types.StringType()),
#         types.StructField("runtime", types.StringType()),
#         types.StructField("spoken_languages", types.StringType()),
#         types.StructField("status", types.StringType()),
#         types.StructField("tagline", types.StringType()),
#         types.StructField("title", types.StringType()),
#         types.StructField("video", types.StringType()),
#         types.StructField("vote_average", types.StringType()),
#         types.StructField("vote_count", types.StringType()),
#     ]
# )

'''movie_schema = types.StructType(
    [
        types.StructField("adult", types.BooleanType()),
        #types.StructField("belongs_to_collection", types.MapType(types.IntegerType(), types.StringType(), types.StringType(), types.StringType(), types.StringType())),
        types.StructField("belongs_to_collection", types.StringType()),
        types.StructField("budget", types.IntegerType()),
        #types.StructField("genres", types.ArrayType(types.MapType(types.IntegerType(), types.StringType()))),
        types.StructField("genres", types.StringType()),
        types.StructField("homepage", types.StringType()),
        types.StructField("id", types.IntegerType()),
        types.StructField("imdb_id", types.StringType()),
        types.StructField("original_language", types.StringType()),
        types.StructField("original_title", types.StringType()),
        types.StructField("overview", types.StringType()),
        types.StructField("popularity", types.DoubleType()),
        types.StructField("poster_path", types.StringType()),
        types.StructField("production_companies", types.StringType()),
        types.StructField("production_countries", types.StringType()),
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
)'''


def main(inputs, output):
    movie_data = spark.read.csv(inputs, schema=movie_schema, header=True)
    movie_data = movie_data.select(movie_data['adult'].cast('boolean'),
                                   movie_data['belongs_to_collection'],
                                   movie_data['budget'].cast('int'),
                                   movie_data['genres'],  
                                   movie_data['homepage'],
                                   movie_data['id'].cast('int'),
                                   movie_data['imdb_id'],
                                   movie_data['original_language'],
                                   movie_data['original_title'],
                                   movie_data['overview'],
                                   movie_data['popularity'].cast('double'),
                                   movie_data['poster_path'],
                                   movie_data['production_companies'],
                                   movie_data['production_countries'],
                                   movie_data['release_date'],
                                   movie_data['revenue'].cast('long'),
                                   movie_data['runtime'].cast('int'),
                                   movie_data['spoken_languages'],
                                   movie_data['status'],
                                   movie_data['tagline'],
                                   movie_data['title'],
                                   movie_data['video'].cast('boolean'),
                                   movie_data['vote_average'].cast('float'),
                                   movie_data['vote_count'].cast('int'))
    movie_data.show(20)



if __name__ == '__main__':
    input = sys.argv[1]
    output = sys.argv[2]
    spark = SparkSession.builder.appName("correlate_logs").getOrCreate()
    assert spark.version >= "2.4"  # make sure we have Spark 2.4+
    spark.sparkContext.setLogLevel("WARN")
    sc = spark.sparkContext
    main(input, output)