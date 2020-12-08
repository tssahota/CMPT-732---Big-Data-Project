import sys
assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+
from pyspark.sql import SparkSession, types, Window
from pyspark.sql.functions import rank, col, to_timestamp, year

def main(movie_path, genre_path):
    output_dir = "analysis_data"
    movie_data = spark.read.parquet(movie_path)
    genre_data = spark.read.parquet(genre_path)
    movie_data.describe('popularity').show()
    movie_data.describe('vote_average').show()
    #remove null data slot
    movie_data = movie_data.na.drop(subset=["title", "release_date", "popularity", "genre_ids", "budget", "profit"])
    movie_data = movie_data.select( (movie_data["profit"]/movie_data["budget"]).alias("return") , movie_data["genre_ids"], movie_data["vote_average"], movie_data["title"], movie_data["popularity"], movie_data["release_date"])
    movie_data = movie_data.withColumn('year', year(to_timestamp(movie_data['release_date'], 'yyyy-MM-dd')))
    movie_data = movie_data.where((movie_data['year'] > 2007) & (movie_data['year'] <= 2017)).drop("release_date", "genre_ids")
    #use windows to partition and sort datas
    popularity_window = Window.partitionBy(movie_data['year']).orderBy(movie_data['popularity'].desc())
    vote_average_window = Window.partitionBy(movie_data['year']).orderBy(movie_data['vote_average'].desc())
    return_window = Window.partitionBy(movie_data['year']).orderBy(movie_data['return'].desc())
    #call window and create new ranking columns
    movie_data = movie_data.select('*', rank().over(popularity_window).alias('poularity_rank'))
    movie_data = movie_data.select('*', rank().over(vote_average_window).alias('vote_average_rank'))
    movie_data = movie_data.select('*', rank().over(return_window).alias('return_rank'))
    movie_data = movie_data.where((col('poularity_rank') <= 10) | (col('vote_average_rank') <= 10) | (col('return_rank') <= 10))
    #drop unused columns
    movie_data = movie_data.drop('poularity_rank', 'vote_average_rank', 'return_rank')
    print(movie_data.count())
    movie_data.show(100)
    movie_data.write.mode('overwrite').parquet(output_dir + "/year_return")

    #genre analysis
    genre_data.orderBy(genre_data['genre_id'].desc()).show(50)


if __name__ == '__main__':
    movie_path = sys.argv[1]
    genre_path = sys.argv[2]
    spark = SparkSession.builder.appName("temporal_trend_analysis").getOrCreate()
    assert spark.version >= "2.4"  # make sure we have Spark 2.4+
    spark.sparkContext.setLogLevel("WARN")
    sc = spark.sparkContext
    main(movie_path, genre_path)