import sys
assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+
from pyspark.sql import SparkSession, types, Window
from pyspark.sql.functions import rank, col, to_timestamp, year, month, explode, broadcast, avg, count, sum, lit, when, max
from pyspark.sql import functions
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.stat import Correlation

def main(input_dir, output_dir):
    #Read movie data
    movie_df = spark.read.parquet(input_dir+"/movies_aggregated_data.parquet")
    #variable made by Tavleen for analysis
    movie_data = movie_df

    #populate average for Null values so that these values dont effect analysis
    profit_avg = int(movie_data.where(movie_data['profit'].isNotNull()).select(movie_data['*'], lit(1).alias('dummy'))\
                    .groupBy('dummy').agg(avg(movie_data['profit']).alias('avg_profit')).rdd.collect()[0].avg_profit)
    #print(profit_avg)
    popularity_avg = int(movie_data.where(movie_data['popularity'].isNotNull()).select(movie_data['*'], lit(1).alias('dummy'))\
                    .groupBy('dummy').agg(avg(movie_data['popularity']).alias('avg_pop')).rdd.collect()[0].avg_pop)
    #print(popularity_avg)
    avg_user_rating_avg = int(movie_data.where(movie_data['avg_user_rating'].isNotNull()).select(movie_data['*'], lit(1).alias('dummy'))\
                    .groupBy('dummy').agg(avg(movie_data['avg_user_rating']).alias('avg_user')).rdd.collect()[0].avg_user)
    #print(avg_user_rating_avg)
    vote_avg_avg = int(movie_data.where(movie_data['vote_average'].isNotNull()).select(movie_data['*'], lit(1).alias('dummy'))\
                    .groupBy('dummy').agg(avg(movie_data['vote_average']).alias('avg_vote')).rdd.collect()[0].avg_vote)
    #print(vote_avg_avg)
    movie_data_avg = movie_data.withColumn("profit_new", when(movie_data['profit'].isNull(), profit_avg).otherwise(movie_data['profit']))\
                                .drop("profit").withColumnRenamed("profit_new", "profit") \
                                .withColumn("popularity_new", when(movie_data['popularity'].isNull(), popularity_avg).otherwise(movie_data['popularity']))\
                                .drop("popularity").withColumnRenamed("popularity_new", "popularity") \
                                .withColumn("avg_user_rating_new", when(movie_data['avg_user_rating'].isNull(), avg_user_rating_avg).otherwise(movie_data['avg_user_rating']))\
                                .drop("avg_user_rating").withColumnRenamed("avg_user_rating_new", "avg_user_rating") \
                                .withColumn("vote_average_new", when(movie_data['vote_average'].isNull(), vote_avg_avg).otherwise(movie_data['vote_average']))\
                                .drop("vote_average").withColumnRenamed("vote_average_new", "vote_average") 
    print(movie_data_avg.where(movie_data['profit'].isNotNull() & movie_data['profit'].isNotNull() & movie_data['profit'].isNotNull() & movie_data['profit'].isNotNull()).count())
    
    #movie_data_avg.show(10)

    #movie_df.describe('popularity').show()
    #movie_df.describe('vote_average').show()
    #movie_df.show(1)
    #movie_df.printSchema()
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
    
    #******Read country detail data******
    country_df = spark.read.parquet(input_dir+"/country_details.parquet")
    #country_df.show(10)

    #******Read keyword detail data******
    keyword_df = spark.read.parquet(input_dir+"/keyword_details.parquet")
    #keywords_df.show(10)

    #******Read cast detail data******
    cast_df = spark.read.parquet(input_dir+"/cast_details.parquet")
    cast_power_df = spark.read.parquet(input_dir+"/cast_power.parquet")
    cast_power_df.show(10)

    #******Read crew detail data******
    crew_df = spark.read.parquet(input_dir+"/crew_details.parquet")
    director_power_df = spark.read.parquet(input_dir+"/director_power.parquet")
    director_power_df.show(10)

    #******Read Youtube detail data******
    youtube_df = spark.read.parquet(input_dir+"/youtube_data.parquet")
    youtube_df.show(10)
    
    #******task1 10 Most popular/highest return/highest avg user-rated/highest vote average movies each year (2000-2017)******
    task1_df = cached_movie_df.select("title", 'profit', "vote_average", "popularity", 'avg_user_rating', "year")
    task1_df = task1_df.where((task1_df['year'] >= 2000) & (task1_df['year'] <= 2017))
    #use windows to partition and sort data
    popularity_window = Window.partitionBy('year').orderBy(col('popularity').desc())
    vote_average_window = Window.partitionBy('year').orderBy(col('vote_average').desc())
    profit_window = Window.partitionBy('year').orderBy(col('profit').desc())
    avg_user_rating_window = Window.partitionBy('year').orderBy(col('avg_user_rating').desc())
    
    #call window and create new ranking columns
    task1_df = task1_df.select('*', rank().over(popularity_window).alias('popularity_rank'))
    task1_df = task1_df.select('*', rank().over(vote_average_window).alias('vote_average_rank'))
    task1_df = task1_df.select('*', rank().over(profit_window).alias('profit_rank'))
    task1_df = task1_df.select('*', rank().over(profit_window).alias('avg_user_rating_rank'))
    task1_df = task1_df.where((col('popularity_rank') <= 10) | (col('vote_average_rank') <= 10) | (col('profit_rank') <= 10) | (col('avg_user_rating_rank') <= 10))
    #drop unused columns
    task1_df = task1_df.drop('popularity_rank', 'vote_average_rank', 'profit_rank', 'avg_user_rating_rank')
    #task1_df.where(col('title') == "The Guide").show()
    # print('task1 result')
    # task1_df.show(10)
    task1_df.write.mode('overwrite').parquet(output_dir + "/task1")

    #******create genre x movie data ******
    ex_movie_df = cached_movie_df.select('*', explode(cached_movie_df['genre_ids']).alias('genre_id'))
    ex_movie_df = ex_movie_df.drop('genre_ids', 'month')
    genre_movie_df = ex_movie_df.join(broadcast(genre_df), 'genre_id')
    genre_movie_df = genre_movie_df.cache()

   #***task2 Most popular/highest return/highest avg rated/highest vote average genre each year (all-time)******
    task2_df = genre_movie_df.select('title', 'profit', "vote_average", "popularity", 'avg_user_rating', "year", 'genre_name')
    #task2_df = task2_df.where((task2_df['year'] >= 2000) & (task2_df['year'] <= 2017))
    task2_df = task2_df.where((task2_df['year'] <= 2017))
    #task2_df.orderBy(col('year').desc()).show(100)
    task2_df = task2_df.groupBy('year','genre_name').agg(count(col('title')).alias('movie count'), avg(col('profit')).alias('profit'), avg(col('popularity')).alias('popularity'), avg(col('vote_average')).alias('vote_average'), avg(col('avg_user_rating')).alias('avg_user_rating'))
    task2_df = task2_df.select('*', rank().over(popularity_window).alias('popularity_rank'))
    task2_df = task2_df.select('*', rank().over(vote_average_window).alias('vote_average_rank'))
    task2_df = task2_df.select('*', rank().over(profit_window).alias('profit_rank'))
    task2_df = task2_df.select('*', rank().over(profit_window).alias('avg_user_rating_rank'))
    task2_df = task2_df.where((col('popularity_rank') <= 1) | (col('vote_average_rank') <= 1) | (col('profit_rank') <= 1) | (col('avg_user_rating_rank') <= 1))
    task2_df = task2_df.select('year', 'avg_user_rating', 'profit', 'vote_average', 'popularity', 'genre_name')
    #print('task2 result\n')
    #print(task2_df.orderBy(col('year').desc()).count())
    #task2_df.orderBy(col('year').desc()).show(10)
    #print('task2 end\n')
    task2_df.write.mode('overwrite').parquet(output_dir + "/task2")

    #******genre analysis task3 10 Most popular/highest return/highest avg rated/highest vote average movie in each genre (2000-2017)******
    #genre_movie_df.show(20)
    task3_df = genre_movie_df.select("year", 'genre_name', 'title', 'profit', "vote_average", "popularity", 'avg_user_rating')
    task3_df = task3_df.where((task3_df['year'] >= 2000) & (task3_df['year'] <= 2017))
    genre_pop_window = Window.partitionBy('genre_name').orderBy(col('popularity').desc())
    genre_profit_window = Window.partitionBy('genre_name').orderBy(col('profit').desc())
    genre_vote_average_window = Window.partitionBy('genre_name').orderBy(col('vote_average').desc())
    genre_avg_user_rating_window = Window.partitionBy('genre_name').orderBy(col('avg_user_rating').desc())

    task3_df = task3_df.select('*', rank().over(genre_pop_window).alias('popularity_rank'))
    task3_df = task3_df.select('*', rank().over(genre_profit_window).alias('profit_rank'))
    task3_df = task3_df.select('*', rank().over(genre_vote_average_window).alias('vote_average_rank'))
    task3_df = task3_df.select('*', rank().over(genre_avg_user_rating_window).alias('avg_user_rating_rank'))
    task3_df = task3_df.where((col('popularity_rank') <= 10) | (col('vote_average_rank') <= 10) | (col('profit_rank') <= 10) | (col('avg_user_rating_rank') <= 10))
    task3_df = task3_df.drop('popularity_rank', 'vote_average_rank', 'profit_rank', 'avg_user_rating_rank')
    # print('task3 result')
    # task3_df.show(10)
    task3_df.write.mode('overwrite').parquet(output_dir + "/task3")
    
    #******task4 Top 10 Prod companies with movies that are Most(or avg?) popular/highest avg profit/highest avg rated/highest vote average (all-time)******
    task4_df = cached_movie_df.select('profit', "vote_average", "popularity", 'avg_user_rating', 'production_company_ids')
    task4_df = task4_df.select('*', explode(task4_df['production_company_ids']).alias('company_id')).drop('production_company_ids')
    task4_df = task4_df.join(broadcast(company_df), 'company_id')
    task4_df = task4_df.groupBy('production_company').agg(count(col('profit')).alias('count'), avg(col('profit')).alias('profit'), avg(col('popularity')).alias('popularity'), avg(col('vote_average')).alias('vote_average'), avg(col('avg_user_rating')).alias('avg_user_rating'))
    task4_df = task4_df.where(task4_df['count'] > 15).drop('count')
    #print(task4_df.count())
    #task4_df.orderBy(task4_df['profit'], ascending = False).show(10)
    task4_df.write.mode('overwrite').parquet(output_dir + "/task4")

    #******task8 Original languages other than english which have highest avg popularity, max total revenue, highest average ratings******
    task8_df = cached_movie_df.select('profit', "vote_average", "popularity", 'avg_user_rating', 'language_id')
    task8_df = task8_df.select('*', explode(task8_df['language_id']).alias('language')).drop('language_id')
    task8_df = task8_df.groupBy('language').agg(count(col('profit')).alias('count'), avg(col('profit')).alias('profit'), avg(col('popularity')).alias('popularity'), avg(col('vote_average')).alias('vote_average'), avg(col('avg_user_rating')).alias('avg_user_rating'))
    task8_df = task8_df.where(task8_df['count'] > 50)
    #print(task8_df.count())
    #task8_df.show(20)
    task8_df.write.mode('overwrite').parquet(output_dir + "/task8")

    #******task10 Most total popular production countries******
    task10_df = cached_movie_df.select("popularity",'prod_country_ids')
    task10_df = task10_df.select('*', explode(task10_df['prod_country_ids']).alias('country_id')).drop('prod_country_ids')
    #No big difference in join
    task10_df = task10_df.groupBy("country_id").agg(count('popularity').alias('count'))
    task10_df = task10_df.join(broadcast(country_df), 'country_id')
    # task10_df = task10_df.groupBy('collection_name').agg(count(col('profit')).alias('count'), avg(col('profit')).alias('profit'), avg(col('popularity')).alias('popularity'), avg(col('vote_average')).alias('vote_average'), avg(col('avg_user_rating')).alias('avg_user_rating'))
    # task10_df = task10_df.where(task10_df['count'] > 1)
    print('task10 result\n')
    #print(task10_df.count(), country_df.count())
    #task10_df.show(20)
    task10_df.write.mode('overwrite').parquet(output_dir + "/task10")

    #******task11 Collection with movies of highest avg popularity, highest return, highest avg return and highest avg rating******
    task11_df = cached_movie_df.select('profit', "vote_average", "popularity", 'avg_user_rating', 'collection_ids')
    task11_df = task11_df.select('*', explode(task11_df['collection_ids']).alias('collection_id')).drop('collection_ids')
    #No big difference in join
    task11_df = task11_df.join(collection_df, 'collection_id')
    task11_df = task11_df.groupBy('collection_name').agg(count(col('profit')).alias('count'), avg(col('profit')).alias('profit'), avg(col('popularity')).alias('popularity'), avg(col('vote_average')).alias('vote_average'), avg(col('avg_user_rating')).alias('avg_user_rating'))
    task11_df = task11_df.where(task11_df['count'] > 1)
    #task11_df.show(20)
    task11_df.write.mode('overwrite').parquet(output_dir + "/task11")
    
    #******task6 Most common words in movie titles******
    #Insight: Were the most frequent choice of words for a movie title. A carefully chosen and intruiging title can boost box office sales and popularity a movie. Therefore, the most frequent choice may be worth noting 
    task6_df = movie_data.select(functions.explode(functions.split(movie_data['title'], " ")).alias('title'))
    task6_df.show(20)
    task6_df.write.mode('overwrite').parquet(output_dir + "/task6")

    #******task7 Most common themes in movies******
    #Insight: "Woman director", "independent film", and "murder" are the most frequent keywords used to tag movies. This gives us insight into themes that the creators or the public believe are important features and can promote a movie's success
    task7_df = movie_data.select(movie_data['keyword_ids'])
    task7_df = task7_df.select(explode(task7_df['keyword_ids']).alias('keyword_id')) 
    task7_df = task7_df.join(keyword_df, keyword_df['keyword_id']==task7_df['keyword_id']).select(keyword_df['keyword'])
    task7_df.write.mode('overwrite').parquet(output_dir + "/task7")

    #******task14 Most common release month + release months that generate the highest avg return******
    #Display % of movies released each month as pie chart and month-avg profit as horizontal bar graph
    task14_df = cached_movie_df.groupBy("month").agg(count(cached_movie_df['tmdb_id']).alias('count'), avg(cached_movie_df['profit']).alias('avg_profit'))
    task14_df.show(12)
    task14_df.write.mode('overwrite').parquet(output_dir + "/task14")
    

    #**task16****Actors & Directors with highest average revenue/******
    #calculate actor/cast_power
    task16_df1 = cast_df.join(cast_power_df, cast_df['cast_id']==cast_power_df['cast_split']) \
                        .select(cast_df['cast_name'].alias('name'), cast_power_df['cast_power'].alias('avg_revenue'), cast_power_df['count'].alias('count') ,lit('Actor').alias('job'))

    #calculate director_power
    task16_df2 = crew_df.join(director_power_df, crew_df['crew_id']==director_power_df['director_split']) \
                        .select(crew_df['crew_name'].alias('name'), director_power_df['director_power'].alias('avg_revenue'), director_power_df['count'].alias('count'),lit('Director').alias('job') )
    task16_df = task16_df1.union(task16_df2)
    task16_df = task16_df.where(((task16_df['job'] == 'Actor') & (task16_df['count'] > 6)) | (task16_df['job'] == 'Director')).drop(task16_df['count']).distinct()
    #use windows to partition and sort data
    revenue_window = Window.partitionBy('job').orderBy(col('avg_revenue').desc())
    
    #call window and create new ranking columns
    task16_df = task16_df.select('*', rank().over(revenue_window).alias('avg_revenue_rank'))
    task16_df = task16_df.where((col('avg_revenue_rank') <= 10))
    task16_df.show(20)
    task16_df.write.mode('overwrite').parquet(output_dir + "/task16")
    
    #***task18***Data Distribution and Outliers******   
    movietube = movie_data.na.drop(subset=['budget', 'profit','popularity', 'revenue', 'vote_average', 'vote_count', 'runtime', 'avg_user_rating','tmdb_id'])\
                             .select('budget', 'popularity', 'revenue', 'vote_average', 'vote_count', 'runtime', 'avg_user_rating','tmdb_id','profit','release_date') 
    movietube = movietube.join(youtube_df,  movietube['tmdb_id']==youtube_df['tmdb_id'],'left')\
                         .select(movietube['*'], youtube_df['youtube_views'].cast('int').alias('youtube_views'), youtube_df['youtube_likes'].cast('int').alias('youtube_likes'),\
                                 youtube_df['youtube_dislikes'].cast('int').alias('youtube_dislikes'))
    movietube.cache()
    task18_df = movietube
    task18_df.describe().show(20)
    task18_df.write.mode('overwrite').parquet(output_dir + "/task18")
    
    
    #***task15***Correlation between continuous quantitative features******
    task15_df = movietube.na.drop(subset=['youtube_views','youtube_likes','youtube_dislikes'])\
                             .select('budget', 'popularity', 'revenue', 'vote_average', 'vote_count', \
                                'runtime', 'avg_user_rating','profit' ,'youtube_views','youtube_likes','youtube_dislikes') 
    assembler = VectorAssembler(inputCols=['budget','vote_average', 'vote_count','runtime', 'popularity','revenue', 'avg_user_rating',\
                                           'youtube_views', 'youtube_likes','youtube_dislikes'],outputCol='features')
    corr_df = assembler.transform(task15_df)
    task15_dense_matrix = Correlation.corr(corr_df, "features").withColumnRenamed('pearson(features)', 'features').head().features
    rows = task15_dense_matrix.toArray().tolist()
    task15_df = spark.createDataFrame(rows,['budget','popularity', 'revenue', 'vote_average', 'vote_count', 'runtime', 'avg_user_rating',\
                                            'youtube_views', 'youtube_likes','youtube_dislikes'])
    task15_df.show(10)
    task15_df.write.mode('overwrite').parquet(output_dir + "/task15")

    #***task19***Change in quantitative features over the years******
    task19_df = cached_movie_df.na.drop(subset=['runtime'])\
                                  .join(youtube_df,  cached_movie_df['tmdb_id']==youtube_df['tmdb_id'],'left')\
                                  .select(cached_movie_df['*'], youtube_df['youtube_views'].cast('int').alias('youtube_views'), \
                                    youtube_df['youtube_likes'].cast('int').alias('youtube_likes'), youtube_df['youtube_dislikes'].cast('int').alias('youtube_dislikes'))
    task19_df = task19_df.groupBy('year').agg(avg(task19_df['budget']).alias('budget'), \
                                              avg(task19_df['revenue']).alias('revenue'), \
                                              avg(task19_df['vote_average']).alias('vote_average'), \
                                              avg(task19_df['vote_count']).alias('vote_count'), \
                                              avg(task19_df['runtime']).alias('runtime'), \
                                              avg(task19_df['popularity']).alias('popularity'), \
                                              avg(task19_df['avg_user_rating']).alias('avg_user_rating'), \
                                              avg(task19_df['youtube_views']).alias('youtube_views'), \
                                              avg(task19_df['youtube_likes']).alias('youtube_likes'), \
                                              avg(task19_df['youtube_dislikes']).alias('youtube_dislikes'))
    task19_df.show(5)
    task19_df.write.mode('overwrite').parquet(output_dir + "/task19")

    #******task5 Average budget and average revenue comparison for movies in each genres ******
    #Display: Count as bubble size, Y axis-revenue, X axis Month
    task5_df = cached_movie_df.select(cached_movie_df['month'],cached_movie_df['tmdb_id'], cached_movie_df['revenue'],\
                                     functions.explode(cached_movie_df['genre_ids']).alias('genre_id'))
    task5_df = task5_df.groupBy("month",'genre_id').agg(count(cached_movie_df['tmdb_id']).alias('count'), avg(cached_movie_df['revenue']).alias('revenue'))
    task5_df = task5_df.join(genre_df, task5_df['genre_id']==genre_df['genre_id']).select(task5_df['*'],genre_df['genre_name'].alias('genre')).drop('genre_id') 
    task5_df.show(50)
    task5_df.write.mode('overwrite').parquet(output_dir + "/task5")

    #******task9 Average budget and average revenue comparison for movies in each genres ******
    #Display: Count as bubble size, Y axis-revenue, X axis budget
    task9_df = cached_movie_df.select(cached_movie_df['budget'],cached_movie_df['tmdb_id'], cached_movie_df['revenue'],\
                                                             functions.explode(cached_movie_df['genre_ids']).alias('genre_id'))
    task9_df = task9_df.groupBy('genre_id').agg(count(cached_movie_df['tmdb_id']).alias('count'), avg(cached_movie_df['revenue']).alias('revenue'), \
                                                avg(cached_movie_df['budget']).alias('budget'))
    task9_df = task9_df.join(genre_df, task9_df['genre_id']==genre_df['genre_id']).select(task9_df['*'],genre_df['genre_name'].alias('genre')).drop('genre_id') 
    task9_df.show(12)
    task9_df.write.mode('overwrite').parquet(output_dir + "/task9")

    
if __name__ == '__main__':
    input_dir = sys.argv[1]
    output_dir = sys.argv[2]
    spark = SparkSession.builder.appName("task1-3").getOrCreate()
    assert spark.version >= "2.4"  # make sure we have Spark 2.4+
    spark.sparkContext.setLogLevel("WARN")
    sc = spark.sparkContext
    main(input_dir, output_dir)