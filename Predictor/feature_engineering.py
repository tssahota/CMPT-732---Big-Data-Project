import sys
assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+
from pyspark.sql import SparkSession, functions, types, Row

def main(inputs, output):
    data = spark.read.parquet(input_dir + "/movies_aggregated_data.parquet")
    cast = spark.read.parquet(input_dir + "/cast_details.parquet")
    crew = spark.read.parquet(input_dir + "/crew_details.parquet")
    keyword = spark.read.parquet(input_dir + "/keyword_details.parquet")

    pred_revenue = data.where(data['budget'].isNotNull() & data['revenue'].isNotNull() & data['runtime'].isNotNull())
    print("First pred_revenue count:", pred_revenue.count())

    #Create binary variable for belongs_to_collection(Yes=1, No=0)
    pred_revenue = pred_revenue.withColumn("collection", functions.when(pred_revenue['collection_ids'].isNotNull(), functions.lit(1)).otherwise(functions.lit(0)))

    #Spit genre into 20 genres
    pred_revenue = pred_revenue.withColumn("genre_12", functions.when(functions.array_contains(pred_revenue['genre_ids'], 12), functions.lit(1)).otherwise(functions.lit(0)))\
                               .withColumn("genre_878", functions.when(functions.array_contains(pred_revenue['genre_ids'], 878), functions.lit(1)).otherwise(functions.lit(0))) \
                               .withColumn("genre_99", functions.when(functions.array_contains(pred_revenue['genre_ids'], 99), functions.lit(1)).otherwise(functions.lit(0))) \
                               .withColumn("genre_16", functions.when(functions.array_contains(pred_revenue['genre_ids'], 16), functions.lit(1)).otherwise(functions.lit(0))) \
                               .withColumn("genre_10770", functions.when(functions.array_contains(pred_revenue['genre_ids'], 10770), functions.lit(1)).otherwise(functions.lit(0))) \
                               .withColumn("genre_53", functions.when(functions.array_contains(pred_revenue['genre_ids'], 53), functions.lit(1)).otherwise(functions.lit(0))) \
                               .withColumn("genre_9648", functions.when(functions.array_contains(pred_revenue['genre_ids'], 9648), functions.lit(1)).otherwise(functions.lit(0))) \
                               .withColumn("genre_10769", functions.when(functions.array_contains(pred_revenue['genre_ids'], 10769), functions.lit(1)).otherwise(functions.lit(0))) \
                               .withColumn("genre_36", functions.when(functions.array_contains(pred_revenue['genre_ids'], 36), functions.lit(1)).otherwise(functions.lit(0))) \
                               .withColumn("genre_10749", functions.when(functions.array_contains(pred_revenue['genre_ids'], 10749), functions.lit(1)).otherwise(functions.lit(0))) \
                               .withColumn("genre_14", functions.when(functions.array_contains(pred_revenue['genre_ids'], 14), functions.lit(1)).otherwise(functions.lit(0))) \
                               .withColumn("genre_37", functions.when(functions.array_contains(pred_revenue['genre_ids'], 37), functions.lit(1)).otherwise(functions.lit(0))) \
                               .withColumn("genre_28", functions.when(functions.array_contains(pred_revenue['genre_ids'], 28), functions.lit(1)).otherwise(functions.lit(0))) \
                               .withColumn("genre_35", functions.when(functions.array_contains(pred_revenue['genre_ids'], 35), functions.lit(1)).otherwise(functions.lit(0))) \
                               .withColumn("genre_27", functions.when(functions.array_contains(pred_revenue['genre_ids'], 27), functions.lit(1)).otherwise(functions.lit(0))) \
                               .withColumn("genre_10751", functions.when(functions.array_contains(pred_revenue['genre_ids'], 10751), functions.lit(1)).otherwise(functions.lit(0))) \
                               .withColumn("genre_18", functions.when(functions.array_contains(pred_revenue['genre_ids'], 18), functions.lit(1)).otherwise(functions.lit(0))) \
                               .withColumn("genre_10402", functions.when(functions.array_contains(pred_revenue['genre_ids'], 10402), functions.lit(1)).otherwise(functions.lit(0))) \
                               .withColumn("genre_80", functions.when(functions.array_contains(pred_revenue['genre_ids'], 80), functions.lit(1)).otherwise(functions.lit(0))) \
                               .withColumn("genre_10752", functions.when(functions.array_contains(pred_revenue['genre_ids'], 10752), functions.lit(1)).otherwise(functions.lit(0))) 

    pred_revenue.cache()
    print("Orginial pred_revenue count:", pred_revenue.count())
    print("Orginial cast count:", cast.count())
    print("Orginial crew count:", crew.count())

    #Calculate cast_power, director_power, producer_power, keyword_power features
    cast_temp = pred_revenue.select(pred_revenue['*'], functions.explode(pred_revenue['cast_ids']).alias('cast_split'))
    cast_power = cast_temp.groupBy('cast_split').agg(functions.avg(cast_temp['profit']).alias('cast_power'))
    cast_power.cache()
    cast_temp = cast_temp.join(cast_power, cast_temp['cast_split']==cast_power['cast_split'], 'left').distinct()
    cast_join = cast_temp.groupBy('tmdb_id').agg(functions.avg(cast_temp['cast_power']).alias('cast_power'))
    
    director_temp = pred_revenue.select(pred_revenue['*'], functions.explode(pred_revenue['director_ids']).alias('director_split'))
    director_power = director_temp.groupBy('director_split').agg(functions.avg(director_temp['profit']).alias('director_power'))
    director_power.cache()
    director_temp = director_temp.join(director_power, director_temp['director_split']==director_power['director_split'], 'left').distinct()
    director_join = director_temp.groupBy('tmdb_id').agg(functions.avg(director_temp['director_power']).alias('director_power'))
    
    producer_temp = pred_revenue.select(pred_revenue['*'], functions.explode(pred_revenue['producer_ids']).alias('producer_split'))
    producer_power = producer_temp.groupBy('producer_split').agg(functions.avg(producer_temp['profit']).alias('producer_power'))
    producer_power.cache()
    producer_temp = producer_temp.join(producer_power, producer_temp['producer_split']==producer_power['producer_split'], 'left').distinct()
    producer_join = producer_temp.groupBy('tmdb_id').agg(functions.avg(producer_temp['producer_power']).alias('producer_power'))
    
    keyword_temp = pred_revenue.select(pred_revenue['*'], functions.explode(pred_revenue['keyword_ids']).alias('keyword_split'))
    keyword_power = keyword_temp.groupBy('keyword_split').agg(functions.avg(keyword_temp['profit']).alias('keyword_power'))
    keyword_power.cache()
    keyword_temp = keyword_temp.join(keyword_power, keyword_temp['keyword_split']==keyword_power['keyword_split'], 'left').distinct()
    keyword_join = keyword_temp.groupBy('tmdb_id').agg(functions.avg(keyword_temp['keyword_power']).alias('keyword_power'))
    
    pred_revenue = pred_revenue.join(cast_join, pred_revenue['tmdb_id']==cast_join['tmdb_id'], 'left').select(pred_revenue['*'], cast_join['cast_power'])  
    pred_revenue = pred_revenue.join(director_join, pred_revenue['tmdb_id']==director_join['tmdb_id'], 'left').select(pred_revenue['*'], director_join['director_power'])  
    pred_revenue = pred_revenue.join(producer_join, pred_revenue['tmdb_id']==producer_join['tmdb_id'], 'left').select(pred_revenue['*'], producer_join['producer_power'])  
    pred_revenue = pred_revenue.join(keyword_join, pred_revenue['tmdb_id']==keyword_join['tmdb_id'], 'left').select(pred_revenue['*'], keyword_join['keyword_power']) #.distinct()
    
    cast.cache()
    crew.cache()
    print("New pred_revenue count:", pred_revenue.count())
    print("New cast count:", cast.count())
    print("New crew count:", crew.count())

    #pred_revenue.show(3)
    #cast_power.show(3)
    #director_power.show(3)
    #producer_power.show(3)
    #keyword_power.show(3)


    #write files
    cast = cast.join(cast_power, cast['cast_id']==cast_power['cast_split'], 'left').select(cast['*'], cast_power['cast_power'])
    cast.write.mode('overwrite').parquet(input_dir + "/cast_details.parquet") 
    crew = crew.join(director_power, [crew['crew_id']==director_power['director_split'], crew['crew_job'] == 'Director'], 'left').select(crew['*'], director_power['director_power'].alias('crew_power'))
    crew = crew.join(producer_power, [crew['crew_id']==producer_power['producer_split'], crew['crew_job'] == 'Producer'], 'left').select(crew['*'], producer_power['producer_power']) 
    crew = crew.withColumn("crew_power_new", functions.when(crew['crew_power'].isNotNull(), crew['crew_power']).otherwise(crew['producer_power'])) \
              .drop('crew_power','producer_power').withColumnRenamed("crew_power_new", "crew_power") 
    crew.write.mode('overwrite').parquet(input_dir + "/crew_details.parquet") 

    #cast.where(cast['cast_power'].isNotNull()).show(3)
    #crew.where(crew['crew_power'].isNotNull()).show(3)
    
    train, test = pred_revenue.randomSplit([0.90, 0.10]) 
    #save the train file
    train.write.mode('overwrite').parquet(output_dir + "/pred_revenue_train.parquet") 
    #save test file
    test.write.mode('overwrite').parquet(output_dir + "/pred_revenue_test.parquet") 


if __name__ == '__main__':
    input_dir = sys.argv[1]
    output_dir = sys.argv[2]
    spark = SparkSession.builder.appName("data prep").getOrCreate()
    assert spark.version >= "2.4"  # make sure we have Spark 2.4+
    spark.sparkContext.setLogLevel("WARN")
    sc = spark.sparkContext
    main(input_dir, output_dir)