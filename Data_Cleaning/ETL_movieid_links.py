import sys
assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+
from pyspark.sql import SparkSession, functions, types, Row

#load link.csv into parquet files
def process_links(inputs, output_dir,write_mode):
    links_schema = types.StructType(
        [
            types.StructField("movie_id", types.IntegerType()),
            types.StructField("imdb_id", types.IntegerType()),
            types.StructField("tmdb_id", types.IntegerType()),
        ]
    )
    links_data = spark.read.csv(inputs, schema=links_schema, header=True)

    #add movie_id to movies_aggregate_data files
    movies_agg_data_old = spark.read.parquet(output_dir + "/movies_aggregated_data.parquet")

    movies_agg_data_new = movies_agg_data_old.join(links_data, movies_agg_data_old['imdb_id']==links_data['imdb_id'], 'left') \
                                         .select(movies_agg_data_old['*'],links_data['movie_id'].alias('links_movie_id')) 
    movies_agg_data_new = movies_agg_data_new.withColumn("movie_id_new", functions.when(movies_agg_data_new['links_movie_id'].isNull(), movies_agg_data_new['movie_id']) \
                                                                         .otherwise(movies_agg_data_new['links_movie_id'])) \
                                             .drop('movie_id','links_movie_id').withColumnRenamed("movie_id_new", "movie_id")
    movies_agg_data_new.cache() #to avoid parquet not found error
    print('New parquet count: ', movies_agg_data_new.count())

    #always overwrite aggregated_movie data
    movies_agg_data_new.write.mode('overwrite').parquet(output_dir + "/movies_aggregated_data.parquet")

    #Store link as parquet files depedning on write_mode
    if write_mode.lower() == 'overwrite':          
      links_data.write.mode('overwrite').parquet(output_dir + "/link_details.parquet")
      print("Files overwritten: movies_aggregated_data, movies_links")

    else:
      links_data.write.mode('append').parquet(output_dir + "/link_details.parquet")
      print("Files appended: movies_links")
      print("Files overwritten: movies_aggregated_data")

    #links_data.write.csv(output_dir)
    #movies_agg_data_new.write.csv(output_dir)
    '''print('Links_data:')
    links_data.show(5)
    print('Movies_agg_data:')
    movies_agg_data_new.show(5)'''

    
def main(inputs, output):   
    output_dir = output[0]
    write_mode = "append" #default write_mode is append
    #check if write_mode is also mentioned in command line:
    if len(output) > 1:
      write_mode = output[1]
    process_links(inputs,output_dir,write_mode)

if __name__ == '__main__':
    inputs = sys.argv[1]
    output = sys.argv[2:]
    spark = SparkSession.builder.appName("movieid_links").getOrCreate()
    assert spark.version >= "2.4"  # make sure we have Spark 2.4+
    spark.sparkContext.setLogLevel("WARN")
    sc = spark.sparkContext
    main(inputs, output)