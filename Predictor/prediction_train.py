import sys
assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+
from pyspark.sql import SparkSession, functions, types, Row

from pyspark.ml import PipelineModel
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler, SQLTransformer
from pyspark.ml.regression import LinearRegression, GBTRegressor, RandomForestRegressor, DecisionTreeRegressor
from pyspark.ml.tuning import ParamGridBuilder, TrainValidationSplit
from pyspark.ml.feature import StandardScaler, RobustScaler

def create_param_grid(type_, regressor):
    if type_ == 'GBT':
    	paramGrid = ParamGridBuilder().addGrid(regressor.maxIter, [50, 100]).addGrid(regressor.maxDepth, [5, 10]).addGrid(regressor.seed, [60]).build()
    elif type_ == 'LR':
        paramGrid = ParamGridBuilder().addGrid(regressor.maxIter, [50, 100]).addGrid(regressor.regParam, [0.3, 0.6]).build()
    elif type_ ==  'DTR':
        paramGrid = ParamGridBuilder().addGrid(regressor.maxDepth, [5, 10]).build()
    elif type_ == 'RF':
        paramGrid = ParamGridBuilder().addGrid(regressor.maxDepth, [5, 10]).build()
    return paramGrid
  
def create_regressor(type_):
    if type_ == 'GBT':
        regressor = GBTRegressor(featuresCol='scaled_features', labelCol='revenue')
    elif type_ == 'LR':
        regressor = LinearRegression(featuresCol='scaled_features', labelCol='revenue', elasticNetParam=0.8)
    elif type_ ==  'DTR':
        regressor = DecisionTreeRegressor(featuresCol='scaled_features', labelCol='revenue')
    elif type_ == 'RF':
        regressor = RandomForestRegressor(featuresCol='scaled_features', labelCol='revenue')
    return regressor

def join_process_data(data, youtube):
    joined = data.join(youtube, on = ['tmdb_id'], how = 'inner')
    joined_filtered = joined.filter('keyword_power is not null')
    joined_filtered = joined_filtered.withColumn('youtube_views', joined_filtered.youtube_views.cast('double'))
    joined_filtered = joined_filtered.withColumn('youtube_likes', joined_filtered.youtube_likes.cast('double'))
    joined_filtered = joined_filtered.withColumn('youtube_dislikes', joined_filtered.youtube_dislikes.cast('double'))
    return joined_filtered
    
def main(input_dir, output_dir):
    # Train, Validation and Test data loading
    data = spark.read.parquet(input_dir + "/pred_revenue_train.parquet")
    youtube = spark.read.parquet(input_dir + "/youtube_data.parquet")
    test_data = spark.read.parquet(input_dir+"/pred_revenue_test.parquet")
    data_processed = join_process_data(data, youtube)
    test = join_process_data(test_data, youtube)
    train, validation= data_processed.randomSplit([0.80, 0.20]) 
    train = train.cache()
    validation = validation.cache()
    #query = "SELECT budget, popularity, revenue, vote_average, vote_count, runtime, dayofyear(release_date) AS dayofyear, avg_user_rating, \
    #collection, cast_power, director_power, producer_power, keyword_power, genre_12, genre_878, genre_99, genre_16, genre_10770, genre_53, \
    #genre_9648, genre_10769, genre_36, genre_10749, genre_14, genre_37, genre_28, genre_35, genre_27, genre_10751, genre_18, genre_10402, \
    #genre_80, genre_10752, youtube_views, youtube_likes, youtube_dislikes FROM __THIS__ "
    query = "SELECT budget, popularity, revenue, keyword_power,youtube_views, youtube_likes, vote_count  FROM __THIS__ "
    sqlTrans = SQLTransformer(statement=query)

    #Features to be included
    #assembler = VectorAssembler(inputCols=['budget', 'vote_count', 'popularity', 'collection', 'genre_878', 'genre_99', 'genre_16', 'genre_10770', 'genre_53', 'genre_36', 'genre_37', 'genre_9648', 'genre_14', 'genre_10769', 'genre_10749', 'genre_10751', 'genre_35', 'genre_27', 'genre_28', 'genre_18', 'genre_10402', 'genre_12', 'genre_80', 'genre_10752', 'youtube_views', 'youtube_likes', 'youtube_dislikes', 'cast_power', 'keyword_power'],outputCol='features')
    assembler = VectorAssembler(inputCols=['budget', 'vote_count', 'popularity', 'youtube_views', 'youtube_likes', 'keyword_power'],outputCol='features')
    scaler =StandardScaler().setInputCol("features").setOutputCol("scaled_features")
    regressors = ['GBT', 'LR', 'DTR', 'RF' ]
    best_model = ''
    best_r2 = 0
    for reg in regressors:
        print("Regressor : " + reg)
        regressor = create_regressor(reg)
        paramGrid = create_param_grid(reg, regressor)  
        pipeline = Pipeline(stages=[sqlTrans, assembler, scaler, regressor])
        r2_evaluator = RegressionEvaluator(predictionCol='prediction', labelCol='revenue', metricName='r2')
        tvs = TrainValidationSplit(estimator=pipeline,
		           estimatorParamMaps=paramGrid,
		           evaluator=r2_evaluator,
		           # 90% of the data will be used for training, 20% for validation.
		           trainRatio=0.90)
        model = tvs.fit(train)
        print("Validation Metrics: "+str(model.validationMetrics))
        prediction = model.transform(test)
        r2 = r2_evaluator.evaluate(prediction)
        print("R2 Train: ", r2_evaluator.evaluate(model.transform(train)))
        print("R2 Validation: ", r2_evaluator.evaluate(model.transform(validation)))
        print("R2 Test: ", r2_evaluator.evaluate(r2))
        if r2 > best_r2:
                best_r2 = r2
                best_model = model
        try:
            print("feature Importances: ")
            print(model.bestModel.stages[-1].featureImportances)
        except Exception as e:
            print("Feature Importances not available")
        print("\n\n")
                
    #save trained model to file when finalized
    print("saving model")
    model_path = output_dir + "/best_model"
    best_model.write().overwrite().save(model_path)

if __name__ == '__main__':
    input_dir = sys.argv[1]
    output_dir = sys.argv[2]
    spark = SparkSession.builder.appName("predictor_train").getOrCreate()
    assert spark.version >= "2.4"  # make sure we have Spark 2.4+
    spark.sparkContext.setLogLevel("WARN")
    sc = spark.sparkContext
    main(input_dir, output_dir)
