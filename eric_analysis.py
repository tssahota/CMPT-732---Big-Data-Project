import sys
assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+
from pyspark.sql import SparkSession, types, Window
from pyspark.sql.functions import rank, col, to_timestamp, year, month, explode

def main():
    input_dir = "Processed_Data"
    output_dir = "analysis_data"
    movie_data = spark.read.parquet(input_dir+"/movies_aggregated_data.parquet")
    #movie_data.describe('popularity').show()
    #movie_data.describe('vote_average').show()
    #remove null data slot
    movie_data.show(1)
    #movie_data.printSchema()
    #movie_data = movie_data.where((movie_data['title'].isNotNull()) & (movie_data['release_date'].isNotNull()) & (movie_data['release_date'].isNotNull()) & (movie_data['popularity'].isNotNull()) & (movie_data['genre_ids'].isNotNull()) & (movie_data['profit'].isNotNull()))
    movie_data = movie_data.na.drop(subset=["title", "popularity", "genre_ids", "profit", 'avg_user_rating'])
    print(movie_data.count())
    #create new columns for processing
    movie_data = movie_data.select(movie_data['profit'] , movie_data["genre_ids"], movie_data["vote_average"], movie_data["title"], movie_data["popularity"], movie_data['avg_user_rating'], movie_data["release_date"])
    movie_data = movie_data.withColumn('year', year(to_timestamp(movie_data['release_date'], 'yyyy-MM-dd')))
    movie_data = movie_data.withColumn('month', month(to_timestamp(movie_data['release_date'], 'yyyy-MM-dd')))
    movie_data = movie_data.where((movie_data['year'] >= 2000) & (movie_data['year'] <= 2017)).drop("release_date")
    cached_movie_data = movie_data.cache()

    #*** task1
    #use windows to partition and sort data
    popularity_window = Window.partitionBy(cached_movie_data['year']).orderBy(cached_movie_data['popularity'].desc())
    vote_average_window = Window.partitionBy(cached_movie_data['year']).orderBy(cached_movie_data['vote_average'].desc())
    profit_window = Window.partitionBy(cached_movie_data['year']).orderBy(cached_movie_data['profit'].desc())
    avg_user_rating_window = Window.partitionBy(cached_movie_data['year']).orderBy(cached_movie_data['avg_user_rating'].desc())

    # #call window and create new ranking columns
    year_profit_data = cached_movie_data.drop('month')
    year_profit_data = year_profit_data.select('*', rank().over(popularity_window).alias('poularity_rank'))
    year_profit_data = year_profit_data.select('*', rank().over(vote_average_window).alias('vote_average_rank'))
    year_profit_data = year_profit_data.select('*', rank().over(profit_window).alias('profit_rank'))
    year_profit_data = year_profit_data.select('*', rank().over(profit_window).alias('avg_user_rating_rank'))
    year_profit_data = year_profit_data.where((col('poularity_rank') <= 10) | (col('vote_average_rank') <= 10) | (col('profit_rank') <= 10) | (col('avg_user_rating_rank') <= 10))
    # #drop unused columns
    year_profit_data = year_profit_data.drop('poularity_rank', 'vote_average_rank', 'profit_rank', 'avg_user_rating_rank')
    # year_profit_data.show(10)
    # year_profit_data.write.mode('overwrite').parquet(output_dir + "/task1")

    #***genre analysis task3
    genre_data = spark.read.parquet(input_dir+"/genre_details.parquet")
    #genre_data.orderBy(genre_data['genre_id'].desc()).show(50)
    #drop genres that's not in the list since the amount of data is not enough for analysis
    #genres_list = ['Drama', 'Comedy', 'Thriller', 'Romance', 'Action', 'Horror', 'Crime', 'Adventure', 'Science Fiction', 'Mystery', 'Fantasy', 'Animation']
    #genre_data = genre_data.where(genre_data['genre_name'].isin(genres_list))
    ex_movie_data = cached_movie_data.select('*', explode(cached_movie_data['genre_ids']).alias('genre_id'))
    ex_movie_data = ex_movie_data.drop('genre_ids', 'month')
    genre_movie_data = ex_movie_data.join(genre_data, 'genre_id')
    #genre_movie_data.show(20)
    #genre_movie_data = genre_movie_data.groupBy('genre_id').count().orderBy('count')
    #genre_movie_data = genre_movie_data.groupBy('genre_id').agg({''})
    genre_pop_window = Window.partitionBy(genre_movie_data['genre_id']).orderBy(genre_movie_data['popularity'].desc())
    genre_profit_window = Window.partitionBy(genre_movie_data['genre_id']).orderBy(genre_movie_data['profit'].desc())
    genre_vote_average_window = Window.partitionBy(genre_movie_data['genre_id']).orderBy(genre_movie_data['vote_average'].desc())
    genre_avg_user_rating_window = Window.partitionBy(genre_movie_data['genre_id']).orderBy(genre_movie_data['avg_user_rating'].desc())

    genre_pop_data = genre_movie_data.select('*', rank().over(genre_pop_window).alias('poularity_rank'))
    genre_pop_data = genre_pop_data.select('*', rank().over(genre_profit_window).alias('profit_rank'))
    genre_pop_data = genre_pop_data.select('*', rank().over(genre_vote_average_window).alias('vote_average_rank'))
    genre_pop_data = genre_pop_data.select('*', rank().over(genre_avg_user_rating_window).alias('avg_user_rating_rank'))
    genre_pop_data = genre_pop_data.where((col('poularity_rank') <= 10) | (col('vote_average_rank') <= 10) | (col('profit_rank') <= 10) | (col('avg_user_rating_rank') <= 10))
    #genre_pop_data.show(100)
    #genre_pop_data.write.mode('overwrite').parquet(output_dir + "/task3")

    #task2
    #genre_movie_data.show(20)
    #genre_movie_data.groupBy('genre_id', 'year').max('return', 'vote_average', 'popularity').show()
    # window = Window.partitionBy(genre_movie_data['year']).groupBy('genre_id').agg({'popularity': 'max'}).orderBy(genre_movie_data['popularity'].desc())
    # test = genre_movie_data.select('*', rank().over(genre_pop_window).alias('poularity_rank'))
    # test.show(10)
    # genre_pop_window = Window.partitionBy(genre_movie_data['genre_id'], genre_movie_data['year']).orderBy(genre_movie_data['popularity'].desc())
    # genre_return_window = Window.partitionBy(genre_movie_data['genre_id']).orderBy(genre_movie_data['return'].desc())
    # genre_vote_average_window = Window.partitionBy(genre_movie_data['genre_id']).orderBy(genre_movie_data['vote_average'].desc())

if __name__ == '__main__':
    spark = SparkSession.builder.appName("temporal_trend_analysis").getOrCreate()
    assert spark.version >= "2.4"  # make sure we have Spark 2.4+
    spark.sparkContext.setLogLevel("WARN")
    sc = spark.sparkContext
    main()