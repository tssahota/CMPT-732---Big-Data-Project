import sys
assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+
from pyspark.sql import SparkSession, functions, types, Row

#cleans credits.csv and stores it as parquet files
def process_credits(inputs,output_dir, write_mode):
    credits_schema = types.StructType(
        [
            types.StructField("cast", types.StringType()),
            types.StructField("crew", types.StringType()),
            types.StructField("tmdb_id", types.StringType()),
        ]
    )
    credits_data = spark.read.csv(inputs, schema=credits_schema, header=True)
    credits_data = credits_data.select(credits_data['cast'],
                                   credits_data['crew'],
                                   credits_data['tmdb_id'].cast('int'))

    #Convert JSON strings in cast and crew columns 
    #columns to ArrayType containg array of cast_ids and director_ids respectively
    json_id_schema = types.ArrayType(types.StructType([types.StructField("id", types.IntegerType())]))
    json_id_job_schema = types.ArrayType(types.StructType([types.StructField("id", types.IntegerType()),types.StructField("job", types.StringType())]))

    credits_data = credits_data.select(credits_data['*'], functions.from_json(credits_data['cast'], json_id_schema).getField('id').alias("cast_ids"),
                                                          functions.from_json(credits_data['crew'], json_id_job_schema).alias("crew_id"))
    
    credits_data.cache()
    #seperate writers, producers and director ids
    crew_data = credits_data.select(credits_data['*'], functions.explode(credits_data['crew_id']).alias('crew_job'))
    crew_data = crew_data.select(crew_data['tmdb_id'], crew_data['crew_job'].getField('id').alias('crew_id_split'), \
                                 functions.lower(crew_data['crew_job'].getField('job')).alias('job'))
    crew_data.distinct().cache()
    
    writers = crew_data.where(crew_data['job'] == 'writer').select(crew_data['crew_id_split'], crew_data['tmdb_id'])
    writers_agg = writers.groupBy('tmdb_id').agg(functions.collect_set('crew_id_split').alias('writer_ids'))
    
    producers = crew_data.where(crew_data['job'] == 'producer').select(crew_data['crew_id_split'], crew_data['tmdb_id'])
    producers_agg = producers.groupBy('tmdb_id').agg(functions.collect_set('crew_id_split').alias('producer_ids'))
    
    directors = crew_data.where(crew_data['job'] == 'director').select(crew_data['crew_id_split'], crew_data['tmdb_id'])
    directors_agg = directors.groupBy('tmdb_id').agg(functions.collect_set('crew_id_split').alias('director_ids'))

    #Aggregate cast_ids and crew_ids with movies_aggregated_data
    movies_agg_data_old = spark.read.parquet(output_dir + "/movies_aggregated_data.parquet")
    print("Movie data original count: ", movies_agg_data_old.count())

    movies_agg_data_new = movies_agg_data_old.join(credits_data, movies_agg_data_old['tmdb_id']==credits_data['tmdb_id'], 'left') \
                                         .select(movies_agg_data_old['*'],credits_data['cast_ids'].alias('cast'))
    movies_agg_data_new = movies_agg_data_new.join(writers_agg, movies_agg_data_new['tmdb_id']==writers_agg['tmdb_id'], 'left') \
                                         .select(movies_agg_data_new['*'],writers_agg['writer_ids'].alias('writer'))
    movies_agg_data_new = movies_agg_data_new.join(producers_agg, movies_agg_data_new['tmdb_id']==producers_agg['tmdb_id'], 'left') \
                                         .select(movies_agg_data_new['*'],producers_agg['producer_ids'].alias('producer'))
    movies_agg_data_new = movies_agg_data_new.join(directors_agg, movies_agg_data_new['tmdb_id']==directors_agg['tmdb_id'], 'left') \
                                         .select(movies_agg_data_new['*'],directors_agg['director_ids'].alias('director'))
                                           
    movies_agg_data_new = movies_agg_data_new.withColumn("cast_ids_new", functions.when(movies_agg_data_new['cast'].isNull(), movies_agg_data_new['cast_ids']) \
                                                                         .otherwise(movies_agg_data_new['cast'])) \
                                             .withColumn("writer_ids_new", functions.when(movies_agg_data_new['writer'].isNull(), movies_agg_data_new['writer_ids']) \
                                                                         .otherwise(movies_agg_data_new['writer'])) \
                                             .withColumn("producer_ids_new", functions.when(movies_agg_data_new['producer'].isNull(), movies_agg_data_new['producer_ids']) \
                                                                         .otherwise(movies_agg_data_new['producer'])) \
                                             .withColumn("director_ids_new", functions.when(movies_agg_data_new['director'].isNull(), movies_agg_data_new['director_ids']) \
                                                                         .otherwise(movies_agg_data_new['director'])) \
                                             .drop('cast','cast_ids', 'writer', 'writer_ids', 'producer', 'producer_ids','director', 'director_ids') \
                                             .withColumnRenamed("cast_ids_new", "cast_ids").withColumnRenamed("writer_ids_new", "writer_ids") \
                                             .withColumnRenamed("producer_ids_new", "producer_ids").withColumnRenamed("director_ids_new", "director_ids")
    movies_agg_data_new.cache().distinct()
    print("Movie data new count: ", movies_agg_data_new.count())

    #Create a seperate file with unique cast details
    json_cast_schema = types.ArrayType(types.StructType([types.StructField("gender", types.IntegerType()), types.StructField("id", types.IntegerType()), \
                                                        types.StructField("name", types.StringType()), types.StructField("profile_path", types.StringType())]))
    cast_details = credits_data.select(functions.explode(functions.from_json(credits_data['cast'], json_cast_schema)).alias('cast_df')).distinct()
    cast_details = cast_details.select(cast_details['cast_df'].getField('id').alias('cast_id'),\
                                       cast_details['cast_df'].getField('name').alias('cast_name'), \
                                       cast_details['cast_df'].getField('gender').alias('cast_gender'), \
                                       cast_details['cast_df'].getField('profile_path').alias('cast_profile_path')).distinct()
    
    #Create a seperate file with unique Director details
    json_crew_schema = types.ArrayType(types.StructType([types.StructField("gender", types.IntegerType()), types.StructField("id", types.IntegerType()), \
                                                        types.StructField("name", types.StringType()), types.StructField("job", types.StringType()), \
                                                        types.StructField("profile_path", types.StringType())]))
    crew_details = credits_data.select(functions.explode(functions.from_json(credits_data['crew'], json_crew_schema)).alias('crew_df')).distinct()
    crew_details = crew_details.select(crew_details['crew_df'].getField('id').alias('crew_id'),\
                                       crew_details['crew_df'].getField('name').alias('crew_name'), \
                                       crew_details['crew_df'].getField('gender').alias('crew_gender'),
                                       crew_details['crew_df'].getField('job').alias('crew_job'), \
                                       crew_details['crew_df'].getField('profile_path').alias('crew_profile_path')).distinct()

    #modification for predictor
    cast_details = cast_details.withColumn("cast_power", functions.lit(None).cast('int'))
    crew_details = crew_details.withColumn("crew_power", functions.lit(None).cast('int'))

    #always overwrite aggregated_movie data
    movies_agg_data_new.write.mode('overwrite').parquet(output_dir + "/movies_aggregated_data.parquet")
       
    #Store cast and crew details as parquet files depedning on write_mode
    if write_mode.lower() == 'overwrite':          
      cast_details.write.mode('overwrite').parquet(output_dir + "/cast_details.parquet")
      crew_details.write.mode('overwrite').parquet(output_dir + "/crew_details.parquet")
      print("Files overwritten: movies_aggregated_data, cast_details, crew_details")
    else: 
      cast_details.write.mode('append').parquet(output_dir + "/cast_details.parquet")
      crew_details.write.mode('append').parquet(output_dir + "/crew_details.parquet")
      print("Files appended: cast_details, crew_details")

    #crew_details.write.csv(output_dir)
    #cast_details.write.csv(output_dir)
    #movies_agg_data_new.write.csv(output_dir)
    '''crew_details.show(10)
    cast_details.show(10)
    movies_agg_data_new.show()'''
    
def main(inputs, output):  
    output_dir = output[0]
    write_mode = "append" #default writemode is append
    #check if write_mode is also mentioned in command line:
    if len(output) > 1:
      write_mode = output[1] 
    process_credits(inputs,output_dir,write_mode)

if __name__ == '__main__':
    inputs = sys.argv[1]
    output = sys.argv[2:]
    spark = SparkSession.builder.appName("data prep").getOrCreate()
    assert spark.version >= "2.4"  # make sure we have Spark 2.4+
    spark.sparkContext.setLogLevel("WARN")
    sc = spark.sparkContext
    main(inputs, output)