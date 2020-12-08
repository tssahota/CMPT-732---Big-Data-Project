import sys
assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+
from pyspark.sql import SparkSession, functions, types, Row

#cleans movies_metadata.csv and stores parquet files
def process_ratings(inputs, output_dir, write_mode):
    ratings_schema = types.StructType(
        [
            types.StructField("user_id", types.StringType()),
            types.StructField("movie_id", types.StringType()),
            types.StructField("rating", types.StringType()),
            types.StructField("timestamp", types.StringType()),     
        ]
    )
    rating_data = spark.read.csv(inputs, schema=ratings_schema, header=True)
    rating_data = rating_data.select(rating_data['user_id'].cast('int').alias('original_userid'),
                                   rating_data['movie_id'].cast('int').alias('original_movieid'),
                                   rating_data['rating'].cast('float'),
                                   functions.from_unixtime(rating_data['timestamp']).cast('timestamp').alias('timestamp'))

    #Convert 0 user_id and movie_id values to None
    rating_data = rating_data.select(rating_data['*'], functions.when(rating_data['original_userid'] != 0, rating_data['original_userid']).alias('user_id'), \
                                                    functions.when(rating_data['original_movieid'] != 0, rating_data['original_movieid']).alias('movie_id')) \
                               .drop('original_userid','original_movieid')
    rating_data.cache()
    
    #Calculate average_user_rating for each movie and add columns to movies_aggregated_data
    avg_ratings = rating_data.groupBy('movie_id').agg(functions.avg(rating_data['rating']).alias('average_rating'))
    
    #read movies_agg datset
    movies_agg_data_old = spark.read.parquet(output_dir + "/movies_aggregated_data.parquet")
    
    movies_agg_data_new = movies_agg_data_old.join(avg_ratings, movies_agg_data_old['movie_id']==avg_ratings['movie_id'], 'left') \
                                             .select(movies_agg_data_old['*'],avg_ratings['average_rating'].alias('average_rating')) 
    movies_agg_data_new = movies_agg_data_new.withColumn("avg_user_rating_new", functions.when(movies_agg_data_new['average_rating'].isNull(), movies_agg_data_new['avg_user_rating']) \
                                                                         .otherwise(movies_agg_data_new['average_rating'])) \
                                             .drop('average_rating','avg_user_rating').withColumnRenamed("avg_user_rating_new", "avg_user_rating")
    movies_agg_data_new.cache().distinct()
    print("Movie data new count: ", movies_agg_data_new.count())
    
    #always overwrite aggregated_movie data
    movies_agg_data_new.write.mode('overwrite').parquet(output_dir + "/movies_aggregated_data.parquet")

    #Store ratings as parquet files depedning on write_mode
    if write_mode.lower() == 'overwrite':          
      rating_data.write.mode('overwrite').parquet(output_dir + "/rating_details.parquet")
      print("Files overwritten: movies_aggregated_data, rating_details")
    else:
      rating_data.write.mode('append').parquet(output_dir + "/rating_details.parquet")
      print("Files appended: rating_details")
    
    #rating_data.write.csv(output_dir)
    #movies_agg_data_new.write.csv(output_dir)
    '''print('rating_details:')
    rating_data.show(5)
    print('Movies agg data:')
    movies_agg_data_new.show(5)'''


def main(inputs, output):
    output_dir = output[0]
    write_mode = "append" #default writemode is append
    #check if write_mode is also mentioned in command line:
    if len(output) > 1:
      write_mode = output[1] 
    process_ratings(inputs,output_dir,write_mode)

if __name__ == '__main__':
    inputs = sys.argv[1]
    output = sys.argv[2:]
    spark = SparkSession.builder.appName("user ratings").getOrCreate()
    assert spark.version >= "2.4"  # make sure we have Spark 2.4+
    spark.sparkContext.setLogLevel("WARN")
    sc = spark.sparkContext
    main(inputs, output)