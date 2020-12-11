import sys
assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+
from pyspark.sql import SparkSession, functions, types, Row
from pyspark.ml.tuning import 

def main(input_dir, path):
    data = spark.read.parquet(input_dir + "/pred_revenue_test.parquet")
    # LOAD DATA FROM UI INTO SPARK DATAFRAME 
    model_path = path + '/best_model'
    tvsModelRead = TrainValidationSplitModel.read().load(model_path)
    prediction = tvsModelRead.transform(data)
    print(prediction.select('revenue').show())
    
if __name__ == '__main__':
    input_dir = sys.argv[1]
    output_dir = sys.argv[2]
    spark = SparkSession.builder.appName("predict").getOrCreate()
    assert spark.version >= "2.4"  # make sure we have Spark 2.4+
    spark.sparkContext.setLogLevel("WARN")
    sc = spark.sparkContext
    main(input_dir, path)
