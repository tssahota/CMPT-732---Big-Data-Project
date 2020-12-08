import sys
assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+
from pyspark.sql import SparkSession, functions, types, Row

from pyspark.ml import PipelineModel
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler, SQLTransformer
from pyspark.ml.regression import GBTRegressor, GBTParams, GBTRegressionModel


#cleans movies_metadata.csv and stores parquet files
'''def process_ratings(inputs, output_dir, write_mode):
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
    print('rating_details:')
    rating_data.show(5)
    print('Movies agg data:')
    movies_agg_data_new.show(5)
    '''

def main(input_directory, output_directory):
	#remove null values for budget and revenue and split data into train/validation and test
	data = spark.read.parquet(output_directory + "/movies_aggregated_data.parquet")
	pred_revenue = data.where(data['budget'].isNotNull() & data['revenue'].isNotNull() & data['runtime'].isNotNull())

	train, validation, test = data.randomSplit([0.67.5, 22.5, 0.10]) #90% -training and validation (3:1), 10% test
	train = train.cache()
    validation = validation.cache()

	#save test file
	test.write.mode('overwrite').parquet(output_dir + "/pred_revenue_test.parquet") 

	#Transformations
	#add belongs_to_collect (yes/no) feature --> one hot encoding/split to 2 columns?
	#split genres to 20 columns?
	#popularity is extremely skewed, add log or something?
	# make a production_company_power, cast_power, director_power, producer_power,keyword_power
	#convert release data to dayofyear
	#include vote_average and vote_count
	#predictor will take average values for things that are not input by user

	#transform data + training
    query = "SELECT budget, vote_average, vote_count, runtime,  FROM __THIS__ as today \
            INNER JOIN __THIS__ as yesterday ON date_sub(today.date, 1) = yesterday.date AND today.station = yesterday.station"
    sqlTrans = SQLTransformer(statement=query)

    #assembler = VectorAssembler(inputCols=['latitude', 'longitude', 'elevation','day_of_year'],outputCol='features')       1.1
    assembler = VectorAssembler(inputCols=['latitude', 'longitude', 'elevation','day_of_year','yesterday_tmax'],outputCol='features')
    regressor = GBTRegressor(featuresCol='features', labelCol='tmax', maxIter=100, maxDepth=4, seed=60)

    pipeline = Pipeline(stages=[sqlTrans, assembler, regressor])
    model = pipeline.fit(train)
    
    # use the model to make predictions
    predictions = model.transform(validation)
    predictions.show()

    # evaluate the predictions
    r2_evaluator = RegressionEvaluator(predictionCol='prediction', labelCol='tmax',
            metricName='r2')
    r2 = r2_evaluator.evaluate(predictions)
    
    rmse_evaluator = RegressionEvaluator(predictionCol='prediction', labelCol='tmax',
            metricName='rmse')
    rmse = rmse_evaluator.evaluate(predictions)

    #save trained model to file
    model.write().overwrite().save(model_file)

    print('r2 =', r2)
    print('rmse =', rmse)

    # If you used a regressor that gives .featureImportances, maybe have a look...
    print(model.stages[-1].featureImportances)
	data = spark.read.parquet(output_directory + "/movies_aggregated_data.parquet")
	pred_user_rating = data.where(data['avg_user_rating'].isNotNull())
	print(pred_user_rating.count())



if __name__ == '__main__':
    input_directory = sys.argv[1]
    output_directory = sys.argv[2]
    spark = SparkSession.builder.appName("user ratings").getOrCreate()
    assert spark.version >= "2.4"  # make sure we have Spark 2.4+
    spark.sparkContext.setLogLevel("WARN")
    sc = spark.sparkContext
    main(input_directory, output_directory)