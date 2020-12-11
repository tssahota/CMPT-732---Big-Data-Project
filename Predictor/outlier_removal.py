import sys
assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+
from pyspark.sql import SparkSession, functions, types, Row

def calculate_bounds(df, cols):
    bounds = {}
    for col in cols:
        quantiles = df.stat.approxQuantile(col, [0.25, 0.75], 0)
        q1 = quantiles[0]
        q3 = quantiles[1]
        IQR = q3 - q1
        min_limit = q1 - 1.5*IQR
        max_limit = q3 + 1.5*IQR
        res = {}
        res['min'] =  min_limit
        res['max'] =  max_limit
        bounds[col] =  res
    return bounds
    
def main(input_dir, output_dir):
    data = spark.read.parquet(input_dir + "/pred_revenue.parquet")
    data = data.where(data['budget'].isNotNull() & data['revenue'].isNotNull() & data['runtime'].isNotNull())
    print("Data Count before Outlier Removal:" + str(data.count))
    bounds = calculate_bounds(data, ['budget', 'vote_count', 'popularity'])
    filtered_data = data.filter( (data.budget > bounds['budget']['min']) & (data.budget < bounds['budget']['max']) & (data.vote_count > bounds['vote_count']['min']) & (data.vote_count < bounds['vote_count']['max']) & (data.popularity > bounds['popularity']['min']) & (data.popularity < bounds['popularity']['max']) )
    print("Data Count after Outlier Removal:" + str(filtered_data.count))
    print("Saving filtered data at : " + output_dir + "/pred_revenue_filtered.parquet")
    filtered_data.write.mode('overwrite').parquet(output_dir + "/pred_revenue_filtered.parquet")
    
if __name__ == '__main__':
    input_dir = sys.argv[1]
    output_dir = sys.argv[2]
    spark = SparkSession.builder.appName("data prep").getOrCreate()
    assert spark.version >= "2.4"  # make sure we have Spark 2.4+
    spark.sparkContext.setLogLevel("WARN")
    sc = spark.sparkContext
    main(input_dir, output_dir)
    
