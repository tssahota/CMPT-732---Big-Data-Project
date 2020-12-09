import sys
assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+
from pyspark.sql import SparkSession, types, Window
from pyspark.sql.functions import rank, col, to_timestamp, year, month, explode, broadcast, avg, count, sum

def main():
    input_dir = "../Processed_Data"
    output_dir = "../web_dev/apps/analysis_data"

    #Read movie data
    movie_df = spark.read.parquet(input_dir+"/movies_aggregated_data.parquet")
    #movie_df.describe('popularity').show()
    #movie_df.describe('vote_average').show()
    #movie_df.show(1)
    movie_df.printSchema()
    #print(movie_df.count())
    #drop rows contain null
    movie_df = movie_df.na.drop(subset=["title", 'release_date', "popularity", "profit", 'avg_user_rating', 'vote_average'])
    #create new columns for processing
    movie_df = movie_df.withColumn('year', year(to_timestamp(movie_df['release_date'], 'yyyy-MM-dd')))
    movie_df = movie_df.withColumn('month', month(to_timestamp(movie_df['release_date'], 'yyyy-MM-dd')))
    #movie_df.where(col('title') == "The Guide").show()
    #movie_df = movie_df.dropDuplicates()
    cached_movie_df = movie_df.cache()

    #******Read genre_data******
    genre_df = spark.read.parquet(input_dir+"/genre_details.parquet")
    #drop genres that's not in the list since the amount of data is not enough for analysis
    #genres_list = ['Drama', 'Comedy', 'Thriller', 'Romance', 'Action', 'Horror', 'Crime', 'Adventure', 'Science Fiction', 'Mystery', 'Fantasy', 'Animation']
    #genre_df = genre_df.where(genre_df['genre_name'].isin(genres_list))

    #******Read production company detail data******
    company_df = spark.read.parquet(input_dir+"/company_details.parquet")
    #company_df.show(10)

    #******Read collection detail data******
    collection_df = spark.read.parquet(input_dir+"/collection_details.parquet").select(col('collection_ids').alias('collection_id'), 'collection_name')
    #collection_df.show(10)

    #******task1 10 Most popular/highest return/highest avg user-rated/highest vote average movies each year (2000-2017)******
    task1_df = cached_movie_df.select("title", 'profit', "vote_average", "popularity", 'avg_user_rating', "year")
    task1_df = task1_df.where((task1_df['year'] >= 2000) & (task1_df['year'] <= 2017))
    #use windows to partition and sort data
    popularity_window = Window.partitionBy('year').orderBy(col('popularity').desc())
    vote_average_window = Window.partitionBy('year').orderBy(col('vote_average').desc())
    profit_window = Window.partitionBy('year').orderBy(col('profit').desc())
    avg_user_rating_window = Window.partitionBy('year').orderBy(col('avg_user_rating').desc())

    #call window and create new ranking columns
    task1_df = task1_df.select('*', rank().over(popularity_window).alias('poularity_rank'))
    task1_df = task1_df.select('*', rank().over(vote_average_window).alias('vote_average_rank'))
    task1_df = task1_df.select('*', rank().over(profit_window).alias('profit_rank'))
    task1_df = task1_df.select('*', rank().over(profit_window).alias('avg_user_rating_rank'))
    task1_df = task1_df.where((col('poularity_rank') <= 10) | (col('vote_average_rank') <= 10) | (col('profit_rank') <= 10) | (col('avg_user_rating_rank') <= 10))
    #drop unused columns
    task1_df = task1_df.drop('poularity_rank', 'vote_average_rank', 'profit_rank', 'avg_user_rating_rank')
    #task1_df.where(col('title') == "The Guide").show()
    # print('task1 result')
    # task1_df.show(10)
    #task1_df.write.mode('overwrite').parquet(output_dir + "/task1")

    #******create genre x movie data ******
    ex_movie_df = cached_movie_df.select('*', explode(cached_movie_df['genre_ids']).alias('genre_id'))
    ex_movie_df = ex_movie_df.drop('genre_ids', 'month')
    genre_movie_df = ex_movie_df.join(broadcast(genre_df), 'genre_id')
    genre_movie_df = genre_movie_df.cache()

    #***task2 10 Most popular/highest return/highest avg rated/highest vote average genre each year (2000-2017)******
    task2_df = genre_movie_df.select('profit', "vote_average", "popularity", 'avg_user_rating', "year", 'genre_name')
    task2_df = task2_df.where((task2_df['year'] >= 2000) & (task2_df['year'] <= 2017))
    task2_df = genre_movie_df.groupBy('year','genre_name').agg(avg(col('profit')).alias('profit'), avg(col('popularity')).alias('popularity'), avg(col('vote_average')).alias('vote_average'), avg(col('avg_user_rating')).alias('avg_user_rating'))
    # print('task2 result')
    # task2_df.show(10)
    #task2_df.write.mode('overwrite').parquet(output_dir + "/task2")

    #******genre analysis task3 10 Most popular/highest return/highest avg rated/highest vote average movie in each genre (2000-2017)******
    #genre_movie_df.show(20)
    task3_df = genre_movie_df.select("year", 'genre_name', 'title', 'profit', "vote_average", "popularity", 'avg_user_rating')
    task3_df = task3_df.where((task3_df['year'] >= 2000) & (task3_df['year'] <= 2017))
    genre_pop_window = Window.partitionBy('genre_name').orderBy(col('popularity').desc())
    genre_profit_window = Window.partitionBy('genre_name').orderBy(col('profit').desc())
    genre_vote_average_window = Window.partitionBy('genre_name').orderBy(col('vote_average').desc())
    genre_avg_user_rating_window = Window.partitionBy('genre_name').orderBy(col('avg_user_rating').desc())

    task3_df = task3_df.select('*', rank().over(genre_pop_window).alias('poularity_rank'))
    task3_df = task3_df.select('*', rank().over(genre_profit_window).alias('profit_rank'))
    task3_df = task3_df.select('*', rank().over(genre_vote_average_window).alias('vote_average_rank'))
    task3_df = task3_df.select('*', rank().over(genre_avg_user_rating_window).alias('avg_user_rating_rank'))
    task3_df = task3_df.where((col('poularity_rank') <= 10) | (col('vote_average_rank') <= 10) | (col('profit_rank') <= 10) | (col('avg_user_rating_rank') <= 10))
    task3_df = task3_df.drop('poularity_rank', 'vote_average_rank', 'profit_rank', 'avg_user_rating_rank')
    # print('task3 result')
    # task3_df.show(10)
    #task3_df.write.mode('overwrite').parquet(output_dir + "/task3")

    #******task4 Top 10 Prod companies with movies that are Most(or avg?) popular/highest avg profit/highest avg rated/highest vote average (all-time)******
    task4_df = cached_movie_df.select('profit', "vote_average", "popularity", 'avg_user_rating', 'production_company_ids')
    task4_df = task4_df.select('*', explode(task4_df['production_company_ids']).alias('company_id')).drop('production_company_ids')
    task4_df = task4_df.join(broadcast(company_df), 'company_id')
    task4_df = task4_df.groupBy('production_company').agg(count(col('profit')).alias('count'), avg(col('profit')).alias('profit'), avg(col('popularity')).alias('popularity'), avg(col('vote_average')).alias('vote_average'), avg(col('avg_user_rating')).alias('avg_user_rating'))
    task4_df = task4_df.where(task4_df['count'] > 15).drop('count')
    #print(task4_df.count())
    #task4_df.show(20)
    #task4_df.write.mode('overwrite').parquet(output_dir + "/task4")

    #******task8 Original languages other than english which have highest avg popularity, max total revenue, highest average ratings******
    task8_df = cached_movie_df.select('profit', "vote_average", "popularity", 'avg_user_rating', 'language_id')
    task8_df = task8_df.select('*', explode(task8_df['language_id']).alias('language')).drop('language_id')
    task8_df = task8_df.groupBy('language').agg(count(col('profit')).alias('count'), avg(col('profit')).alias('profit'), avg(col('popularity')).alias('popularity'), avg(col('vote_average')).alias('vote_average'), avg(col('avg_user_rating')).alias('avg_user_rating'))
    task8_df = task8_df.where(task8_df['count'] > 50).drop('count')
    #print(task8_df.count())
    #task8_df.show(20)
    #task8_df.write.mode('overwrite').parquet(output_dir + "/task8")

    #******task11 Collection with movies of highest avg popularity, highest return, highest avg return and highest avg rating******
    task11_df = cached_movie_df.select('profit', "vote_average", "popularity", 'avg_user_rating', 'collection_ids')
    task11_df = task11_df.select('*', explode(task11_df['collection_ids']).alias('collection_id')).drop('collection_ids')
    #No big difference in join
    task11_df = task11_df.join(collection_df, 'collection_id')
    task11_df = task11_df.groupBy('collection_name').agg(count(col('profit')).alias('count'), avg(col('profit')).alias('profit'), avg(col('popularity')).alias('popularity'), avg(col('vote_average')).alias('vote_average'), avg(col('avg_user_rating')).alias('avg_user_rating'))
    task11_df = task11_df.where(task11_df['count'] > 1).drop('count')
    #print(task11_df.count())
    task11_df.show(20)
    #task11_df.write.mode('overwrite').parquet(output_dir + "/task11")


if __name__ == '__main__':
    spark = SparkSession.builder.appName("task1-3").getOrCreate()
    assert spark.version >= "2.4"  # make sure we have Spark 2.4+
    spark.sparkContext.setLogLevel("WARN")
    sc = spark.sparkContext
    main()