import sys
assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+
from pyspark.sql import SparkSession, types, Window
from pyspark.sql.functions import rank, col, to_timestamp, year

def main(movie_path, genre_path):
    movie_data = spark.read.parquet(movie_path)
    genre_data = spark.read.parquet(genre_path)
    #popularity
    movie_data.describe('popularity').show()
    #movie_data.show(10)
    movie_data = movie_data.select( movie_data["genre_ids"], movie_data["title"], movie_data["popularity"], movie_data["release_date"])
    genre_data.orderBy(genre_data['genre_id'].desc()).show(50)
    #remove null data slot
    movie_data = movie_data.na.drop(subset=["title", "release_date", "popularity", "genre_ids"])
    # print(movie_data.where(movie_data["popularity"].isNull()).count())
    # print(movie_data.where(movie_data["release_date"].isNull()).count())
    # print(movie_data.where(movie_data["title"].isNull()).count())

    movie_data = movie_data.withColumn('year',year(to_timestamp(movie_data['release_date'], 'yyyy-MM-dd')))
    sorted_movie_data = movie_data.where((movie_data['year'] > 2007) & (movie_data['year'] <= 2017)).drop("genre_ids", "release_date")
    #movie_data.write.partitionBy('year').orderBy('popularity').json("./result", mode="overwrite")
    #movie_data.orderBy('year', 'popularity', ascending=False).show(100)
    #sorted_movie_data.show(10)
    window = Window.partitionBy(sorted_movie_data['year']).orderBy(sorted_movie_data['popularity'].desc())
    popularity_year_result = sorted_movie_data.select('*', rank().over(window).alias('rank')).filter(col('rank') <= 10).orderBy(col('year'), col('rank'))
    popularity_year_result.show(100)
    popularity_year_result.repartition(1).write.csv('popularity_year_result_1', sep='|', header=True)     
    #sorted_popularity = movie_data.sort('popularity', ascending=False)
    #sorted_popularity.select(sorted_popularity['title'], sorted_popularity['tagline'], sorted_popularity['popularity']).show(10)

if __name__ == '__main__':
    movie_path = sys.argv[1]
    genre_path = sys.argv[2]
    spark = SparkSession.builder.appName("temporal_trend_analysis").getOrCreate()
    assert spark.version >= "2.4"  # make sure we have Spark 2.4+
    spark.sparkContext.setLogLevel("WARN")
    sc = spark.sparkContext
    main(movie_path, genre_path)