import sys
assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+
from pyspark.sql import SparkSession, functions, types, Row
import pandas as pd
from pyspark.ml.feature import SQLTransformer
from sklearn import preprocessing
import numpy as np
import seaborn as sns
import matplotlib.pyplot as plt

def main(input_dir, output_dir):
    data = spark.read.parquet(input_dir + "/pred_revenue.parquet")
    youtube_data = spark.read.parquet(input_dir + "/youtube_data.parquet")
    joined = data.join(youtbe_data, on=['tmdb_id'], how='inner')
    query = "SELECT budget, popularity, revenue, vote_average, vote_count, runtime, dayofyear(release_date) AS dayofyear, avg_user_rating, \
    collection, genre_12, genre_878, genre_99, genre_16, genre_10770, genre_53, \
    genre_9648, genre_10769, genre_36, genre_10749, genre_14, genre_37, genre_28, genre_35, genre_27, genre_10751, genre_18, genre_10402, \
    genre_80, genre_10752, youtube_views, youtube_likes, youtube_dislikes FROM __THIS__ "
    sqlTrans = SQLTransformer(statement=query)
    df = sqlTrans.transform(joined)
    pandas_df = df.toPandas()
    scaler = preprocessing.StandardScaler()
    scaled = pd.DataFrame(scaler.fit_transform(pandas_df), columns=pandas_df.columns)
    scaled = scaled.astype('float64')
    fig, ax = plt.subplots(figsize=(24, 18))
    sns.heatmap(scaled.corr(method='pearson'), annot=True, fmt='.4f', cmap=plt.get_cmap('coolwarm'), cbar=False, ax=ax)
    ax.set_yticklabels(ax.get_yticklabels(), rotation="horizontal")
    plt.savefig(output_dir+'/correlation.png', bbox_inches='tight', pad_inches=0.0)
    plt.clf()
    boxplot = scaled.boxplot(column=scaled.columns, figsize=(12,10))
    plt.savefig(output_dir+'/boxplots.png')

if __name__ == '__main__':
    input_dir = sys.argv[1]
    output_dir = sys.argv[2]
    spark = SparkSession.builder.appName("feature_selection").getOrCreate()
    assert spark.version >= "2.4"  # make sure we have Spark 2.4+
    spark.sparkContext.setLogLevel("WARN")
    sc = spark.sparkContext
    main(input_dir, output_dir
        
