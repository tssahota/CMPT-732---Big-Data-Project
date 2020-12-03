import sys
assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+
from pyspark.sql import SparkSession, functions, types, Row

def calculate_return(revenue, budget):
    if revenue and budget:
        return revenue/budget
    else:
        return None

def main(inputs, output):
    movie_data = spark.read.csv(inputs, header=True)
    #change all revenue and budget == 0 to null to avoid calculation error 
    movie_data = movie_data.select(movie_data['adult'].cast('boolean'),
                                   movie_data['belongs_to_collection'],
                                   movie_data['budget'].cast('int').alias('ori_budget'),
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
                                   movie_data['release_date'].cast('timestamp'),
                                   movie_data['revenue'].cast('long').alias('ori_revenue'),
                                   movie_data['runtime'].cast('int'),
                                   movie_data['spoken_languages'],
                                   movie_data['status'],
                                   movie_data['tagline'],
                                   movie_data['title'],
                                   movie_data['video'].cast('boolean'),
                                   movie_data['vote_average'].cast('float'),
                                   movie_data['vote_count'].cast('int'))
    #replace zero by Nones
    moeny_udf = functions.udf(lambda value: None if value == 0 else value, types.IntegerType())
    movie_data = movie_data.withColumn('revenue', moeny_udf(movie_data['ori_revenue']))
    movie_data = movie_data.withColumn('budget', moeny_udf(movie_data['ori_budget']))
    #remove useless features
    movie_data = movie_data.drop('imdb_id', 'original_title', 'adult', 'ori_revenue', 'ori_budget')
    #calculate return as a new column
    return_udf = functions.udf(calculate_return, types.FloatType())
    movie_data = movie_data.withColumn('return', return_udf(movie_data['budget'], movie_data['revenue']))
    #check dataframe
    #movie_data = movie_data.select(movie_data['revenue'], movie_data['budget'], movie_data['return'])
    #movie_data.show(100)
    movie_data.write.parquet(output, mode='overwrite')

if __name__ == '__main__':
    input = sys.argv[1]
    output = sys.argv[2]
    spark = SparkSession.builder.appName("correlate_logs").getOrCreate()
    assert spark.version >= "2.4"  # make sure we have Spark 2.4+
    spark.sparkContext.setLogLevel("WARN")
    sc = spark.sparkContext
    main(input, output)