import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

from pyspark.sql import SparkSession, functions, types, Row
spark = SparkSession.builder.appName('tmax model tester').getOrCreate()
assert spark.version >= '2.3' # make sure we have Spark 2.3+
spark.sparkContext.setLogLevel('WARN')

from pyspark.ml import PipelineModel
from pyspark.ml.evaluation import RegressionEvaluator
import datetime


def test_model():
    # get the data
    temp_res = {'budget': 100, 'vote_count': 100, 'popularity': 100, 'collection': True}
    sc_df = spark.createDataFrame(Row(**i) for i in [temp_res])
    sc_df.show()
    # load the model
    model = PipelineModel.load('./bestModel')
    
    # use the model to make predictions
    predictions = model.transform(test_tomorrow)
    predictions.show()
    # 1 element collected
    prediction = predictions.collect()[0].asDict()['prediction']

    # print tmax tomorrow
    print('Predicted tmax tomorrow:', prediction)

if __name__ == '__main__':
    test_model()
