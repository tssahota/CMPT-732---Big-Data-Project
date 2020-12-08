import sys
assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+
from pyspark.sql import SparkSession, functions, types, Row

#cleans keyword.csv and stores it as parquet files
def process_keyword(inputs,output_dir, write_mode):
    keyword_schema = types.StructType(
        [
            types.StructField("tmdb_id", types.IntegerType()),
            types.StructField("keywords", types.StringType()),
        ]
    )
    keyword_data = spark.read.csv(inputs, schema=keyword_schema, header=True)
    
    #Convert JSON strings in keywords columns 
    #columns to ArrayType containing array of keyword_ids espectively
    json_id_schema = types.ArrayType(types.StructType([types.StructField("id", types.IntegerType())]))
    keyword_data = keyword_data.select(keyword_data['*'], functions.from_json(keyword_data['keywords'], json_id_schema).getField('id').alias("keyword_list"))
    
    keyword_data.cache()
    
    #Aggregate keyword_ids with movies_aggregated_data
    movies_agg_data_old = spark.read.parquet(output_dir + "/movies_aggregated_data.parquet")
    print("Movie data old count: ",movies_agg_data_old.count())
    movies_agg_data_new = movies_agg_data_old.join(keyword_data, movies_agg_data_old['tmdb_id']==keyword_data['tmdb_id'], 'left') \
                                         .select(movies_agg_data_old['*'],keyword_data['keyword_list'].alias('keywords')) 

    movies_agg_data_new = movies_agg_data_new.withColumn("keyword_ids_new", functions.when(movies_agg_data_new['keywords'].isNull(), movies_agg_data_new['keyword_ids']) \
                                                                         .otherwise(movies_agg_data_new['keywords'])) \
                                             .drop('keywords','keyword_ids').withColumnRenamed("keyword_ids_new", "keyword_ids")
    movies_agg_data_new.cache().distinct()
    print("Movie data new count: ", movies_agg_data_new.count())

    #Create a seperate file with unique keyword details
    json_keyword_schema = types.ArrayType(types.StructType([types.StructField("id", types.IntegerType()), types.StructField("name", types.StringType())]))
    keyword_details = keyword_data.select(functions.explode(functions.from_json(keyword_data['keywords'], json_keyword_schema)).alias('keyword_df')).distinct()
    keyword_details = keyword_details.select(keyword_details['keyword_df'].getField('id').alias('keyword_id'),\
                                       keyword_details['keyword_df'].getField('name').alias('keyword')).distinct()
    keyword_details.cache()    

    #always overwrite aggregated_movie data
    movies_agg_data_new.write.mode('overwrite').parquet(output_dir + "/movies_aggregated_data.parquet")
       
    #Store keyword and crew details as parquet files depedning on write_mode
    if write_mode.lower() == 'overwrite':          
      keyword_details.write.mode('overwrite').parquet(output_dir + "/keyword_details.parquet")
      print("Files overwritten: movies_aggregated_data, keyword_details")
    else:
      keyword_details.write.mode('append').parquet(output_dir + "/keyword_details.parquet")
      print("Files appended: keyword_details")

    #keyword_details.write.csv(output_dir)
    #movies_agg_data_new.write.csv(output_dir)
    '''keyword_details.show(5)
    movies_agg_data_new.show(5)'''


def main(inputs, output):  
    output_dir = output[0]
    write_mode = "append" #default writemode is append
    #check if write_mode is also mentioned in command line:
    if len(output) > 1:
      write_mode = output[1] 
    process_keyword(inputs,output_dir,write_mode)

if __name__ == '__main__':
    inputs = sys.argv[1]
    output = sys.argv[2:]
    spark = SparkSession.builder.appName("data prep").getOrCreate()
    assert spark.version >= "2.4"  # make sure we have Spark 2.4+
    spark.sparkContext.setLogLevel("WARN")
    sc = spark.sparkContext
    main(inputs, output)