import sys
assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+
from pyspark.sql import SparkSession, functions, types, Row

from pyspark.ml import PipelineModel
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler, SQLTransformer
from pyspark.ml.regression import LinearRegression, GBTRegressor, GBTParams, GBTRegressionModel, GeneralizedLinearRegression, DecisionTreeRegressor


#cleans movies_metadata.csv and stores parquet files
'''def process_ratings(inputs, output_dir, write_mode):
'''

def main(input_dir, output_dir):
	#remove null values for budget and revenue and split data into train/validation and test
	data = spark.read.parquet(input_dir + "/pred_revenue_train.parquet")
	pred_revenue = data.where(data['budget'].isNotNull() & data['revenue'].isNotNull() & data['runtime'].isNotNull())

	train, validation= data.randomSplit([0.75, 0.25]) 
	train = train.cache()
    validation = validation.cache()

	#Transformations
	#add belongs_to_collect (yes/no) feature --> one hot encoding/split to 2 columns? -Done
	#split genres to 20 columns? - Done
	#popularity is extremely skewed, add log or something? 
	# make a production_company_power, cast_power, director_power, producer_power,keyword_power - Done
	#convert release data to dayofyear - Done
	#include vote_average and vote_count ?
	#predictor will take average values for things that are not input by user

	#transform featured_engineered data and select features to include
    query = "SELECT budget, popularity, revenue, vote_average, vote_count, runtime, dayofyear(release_date) AS dayofyear, avg_user_rating, \
    collection, cast_power, director_power, producer_power, keyword_power, genre_12, genre_878, genre_99, genre_16, genre_10770, genre_53, \
    genre_9648, genre_10769, genre_36, genre_10749, genre_14, genre_37, genre_28, genre_35, genre_27, genre_10751, genre_18, genre_10402, \
    genre_80, genre_10752 FROM __THIS__ "
    sqlTrans = SQLTransformer(statement=query)

    #put features you want to include, here
    assembler = VectorAssembler(inputCols=['budget', 'vote_average', 'vote_count','runtime', 'dayofyear'],outputCol='features')
    
    #Try different regressors with different parameter values to find the best results. Refer to regressor section of: https://spark.apache.org/docs/latest/api/python/pyspark.ml.html#pyspark.ml.regression.DecisionTreeRegressor
    #1. First do feature importance using each model and find the optimum features. 
    #1.5 Consider doing some regularization since we are overfitting by using genre
    #2. Next, fine tune each model and evaluate on validation set to find best parameters for each model. Can use: ParamGridBuilder (recommended)), CrossValidator, TrainValidationSplit (https://spark.apache.org/docs/latest/api/python/pyspark.ml.html#pyspark.ml.regression.DecisionTreeRegressor)
    #3. Next, compare all trained and saved models' performance on test set using prediction_test.py 
    #4. I believe GBTRegression should give best vallues. Write down everything you do and the results roughly in word for report, including why certain parameters/features/models were discarded
    regressor1 = GBTRegressor(featuresCol='features', labelCol='revenue', maxIter=100, maxDepth=4, seed=60) #change parameters for fine-tuning
    regressor2 = LinearRegression(featuresCol='features', labelCol='revenue', maxIter=10, regParam=0.3, elasticNetParam=0.8) #change parameters for fine-tuning
    #maybe try GeneralizedLinearRegression too with family = 'Gaussian', but dont think it'll work :\
    regressor4 = DecisionTreeRegressor(featuresCol='features', labelCol='revenue', maxDepth=5, maxBins=32) #change parameters for fine-tuning
    regressor3 = RandomForestRegressor(featuresCol='features', labelCol='revenue', maxDepth=5, maxBins=32) #change parameters for fine-tuning
    

    pipeline1 = Pipeline(stages=[sqlTrans, assembler, regressor1]) #make different pipelines for different regressors and evaluate simultaneously
    model1 = pipeline1.fit(train) 
    pipeline2 = Pipeline(stages=[sqlTrans, assembler, regressor1]) #make different pipelines for different regressors and evaluate simultaneously
    model2 = pipeline2.fit(train)  
    #and so on for other models
    
    # use the models to make predictions
    predictions1 = model1.transform(validation)
    print("Prediction for model 1 :")
    predictions1.show()
    predictions2 = model1.transform(validation)
    print("Prediction for model 2: ")
    predictions1.show()
    predictions2.show()
    #and so on for other models

    # evaluate the predictions
    r2_evaluator = RegressionEvaluator(predictionCol='prediction', labelCol='revenue',
            metricName='r2')
    r2_1 = r2_evaluator.evaluate(predictions1)
    r2_2 = r2_evaluator.evaluate(predictions2)
    #and so on for other models    

    rmse_evaluator = RegressionEvaluator(predictionCol='prediction', labelCol='revenue',
            metricName='rmse')
    rmse_1 = rmse_evaluator.evaluate(predictions1)
    rmse_2 = rmse_evaluator.evaluate(predictions2)

    #save trained model to file when finalized
    model1.write().overwrite().save(model_file)

    print('r2_1=', r2_1)
    print('rmse_1=', rmse_1)
    print('r2_2=', r2_2)
    print('rmse_2=', rmse_2)

    # If you used a regressor that gives .featureImportances, maybe have a look...
    print(model1.stages[-1].featureImportances)

if __name__ == '__main__':
    input_dir = sys.argv[1]
    output_dir = sys.argv[2]
    spark = SparkSession.builder.appName("predictor_train").getOrCreate()
    assert spark.version >= "2.4"  # make sure we have Spark 2.4+
    spark.sparkContext.setLogLevel("WARN")
    sc = spark.sparkContext
    main(input_dir, output_dir)