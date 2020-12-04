import sys
assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+
from pyspark.sql import SparkSession, functions, types, Row

#cleans movies_metadata.csv and stores parquet files
def clean_metadata(inputs, output_dir):
    movie_schema = types.StructType(
        [
            types.StructField("adult", types.StringType()),
            types.StructField("belongs_to_collection", types.StringType()),
            types.StructField("budget", types.StringType()),
            types.StructField("genres", types.StringType()),     
            types.StructField("homepage", types.StringType()),
            types.StructField("id", types.StringType()),
            types.StructField("imdb_id", types.StringType()),
            types.StructField("original_language", types.StringType()),
            types.StructField("original_title", types.StringType()),
            types.StructField("overview", types.StringType()),
            types.StructField("popularity", types.StringType()),
            types.StructField("poster_path", types.StringType()),
            types.StructField("production_companies", types.StringType()),
            types.StructField("production_countries", types.StringType()),
            types.StructField("release_date", types.StringType()),
            types.StructField("revenue", types.StringType()),
            types.StructField("runtime", types.StringType()),
            types.StructField("spoken_languages", types.StringType()),
            types.StructField("status", types.StringType()),
            types.StructField("tagline", types.StringType()),
            types.StructField("title", types.StringType()),
            types.StructField("video", types.StringType()),
            types.StructField("vote_average", types.StringType()),
            types.StructField("vote_count", types.StringType()),
        ]
    )
    movie_data = spark.read.csv(inputs, schema=movie_schema, header=True)
    movie_data = movie_data.select(movie_data['adult'].cast('boolean'),
                                   movie_data['belongs_to_collection'],
                                   movie_data['budget'].cast('int').alias('original_budget'),
                                   movie_data['genres'],  
                                   movie_data['homepage'],
                                   movie_data['id'].cast('int').alias('tmdb_id'),
                                   movie_data['imdb_id'],
                                   movie_data['original_language'],
                                   movie_data['original_title'],
                                   movie_data['overview'],
                                   movie_data['popularity'].cast('double'),
                                   movie_data['poster_path'],
                                   movie_data['production_companies'],
                                   movie_data['production_countries'],
                                   functions.to_date(movie_data['release_date'], 'MM/dd/yy').alias('release_date'),
                                   movie_data['revenue'].cast('long').alias('original_revenue'),
                                   movie_data['runtime'].cast('int'),
                                   movie_data['spoken_languages'],
                                   movie_data['status'],
                                   movie_data['tagline'],
                                   movie_data['title'],
                                   movie_data['video'].cast('boolean'),
                                   movie_data['vote_average'].cast('float'),
                                   movie_data['vote_count'].cast('int'))  
    #Convert 0 original_budget and original_revenue values to None
    movie_data = movie_data.select(movie_data['*'], functions.when(movie_data['original_budget'] != 0, movie_data['original_budget']).alias('budget'), \
                                                    functions.when(movie_data['original_revenue'] != 0, movie_data['original_revenue']).alias('revenue'))
    #Calculate profit
    movie_data = movie_data.withColumn("profit", movie_data['revenue'] - movie_data['budget'])

    #Convert JSON strings in belongs_to_collection, genres, production_companies, production_countries, spoken_languages 
    #columns to ArrayType containg array of collection_ids and genre_ids respectively
    json_id_schema = types.ArrayType(types.StructType([types.StructField("id", types.IntegerType())]))
    json_iso_3166_1_schema = types.ArrayType(types.StructType([types.StructField("iso_3166_1", types.StringType())]))
    json_iso_639_1_schema = types.ArrayType(types.StructType([types.StructField("iso_639_1", types.StringType())]))
    movie_data = movie_data.select(movie_data['*'], functions.from_json(movie_data['belongs_to_collection'], json_id_schema).getField('id').alias("collection_ids"),\
                                                    functions.from_json(movie_data['genres'], json_id_schema).getField('id').alias("genre_ids"),\
                                                    functions.from_json(movie_data['production_companies'], json_id_schema).getField('id').alias("production_company_ids"),
                                                    functions.from_json(movie_data['production_countries'], json_iso_3166_1_schema).getField('iso_3166_1').alias("prod_country_ids"),
                                                    functions.from_json(movie_data['spoken_languages'], json_iso_639_1_schema).getField('iso_639_1').alias("language_id"))
    movie_data.cache()
    #Create a seperate file with unique belongs_to_collection details
    json_collection_schema = types.ArrayType(types.StructType([types.StructField("id", types.IntegerType()), types.StructField("name", types.StringType())\
                                                              ,types.StructField("poster_path", types.StringType()), types.StructField("backdrop_path", types.StringType())]))
    collection_details = movie_data.select(functions.explode(functions.from_json(movie_data['belongs_to_collection'], json_collection_schema)).alias('collection_df')).distinct()
    collection_details = collection_details.select(collection_details['collection_df'].getField('id').alias('collection_ids'),\
                                                   collection_details['collection_df'].getField('name').alias('collection_name'),
                                                   collection_details['collection_df'].getField('poster_path').alias('collection_poster_path'),
                                                   collection_details['collection_df'].getField('backdrop_path').alias('collection_backdrop_path')).distinct()
    
    #Create a seperate file with unique genre details
    json_genre_schema = types.ArrayType(types.StructType([types.StructField("id", types.IntegerType()), types.StructField("name", types.StringType())]))
    genre_details = movie_data.select(functions.explode(functions.from_json(movie_data['genres'], json_genre_schema)).alias('genre_df')).distinct()
    genre_details = genre_details.select(genre_details['genre_df'].getField('id').alias('genre_id'), genre_details['genre_df'].getField('name').alias('genre_name'))

    #Create a seperate file with unique production company details
    json_prod_comp_schema = types.ArrayType(types.StructType([types.StructField("id", types.IntegerType()), types.StructField("name", types.StringType())]))
    comp_details = movie_data.select(functions.explode(functions.from_json(movie_data['production_companies'], json_prod_comp_schema)).alias('prod_comp_df')).distinct()
    comp_details = comp_details.select(comp_details['prod_comp_df'].getField('id').alias('company_id'), comp_details['prod_comp_df'].getField('name').alias('production_company'))

    #Create a seperate file with unique production country details
    json_prod_contr_schema = types.ArrayType(types.StructType([types.StructField("iso_3166_1", types.StringType()), types.StructField("name", types.StringType())]))
    country_details = movie_data.select(functions.explode(functions.from_json(movie_data['production_countries'], json_prod_contr_schema)).alias('country_df')).distinct()
    country_details = country_details.select(country_details['country_df'].getField('iso_3166_1').alias('country_id'), country_details['country_df'].getField('name').alias('country'))

    #Create a seperate file with unique genre details
    json_lang_schema = types.ArrayType(types.StructType([types.StructField("iso_639_1", types.StringType()), types.StructField("name", types.StringType())]))
    language_details = movie_data.select(functions.explode(functions.from_json(movie_data['spoken_languages'], json_lang_schema)).alias('language_df')).distinct()
    language_details = language_details.select(language_details['language_df'].getField('iso_639_1').alias('lang_id'), language_details['language_df'].getField('name').alias('language'))

    #Drop raw data for transformed columns
    movie_data = movie_data.drop('original_budget','original_revenue','genres','belongs_to_collection','production_companies','production_countries','spoken_languages')
    
    #Store as parquet files
    movie_data.write.mode('overwrite').parquet(output_dir + "/movies_metadata")
    genre_details.write.mode('overwrite').parquet(output_dir + "/genre_details")
    collection_details.write.mode('overwrite').parquet(output_dir + "/collection_details")
    comp_details.write.mode('overwrite').parquet(output_dir + "/comp_details")
    country_details.write.mode('overwrite').parquet(output_dir + "/country_details")
    '''language_details.write.mode('overwrite').parquet(output_dir + "/language_details")
    movie_data.write.csv(output_dir)
    genre_details.write.csv(output_dir)
    collection_details.write.csv(output_dir)
    comp_details.write.csv(output_dir)
    country_details.write.csv(output_dir)
    language_details.write.csv(output_dir)'''
    movie_data.show(10)
    genre_details.show(10)
    collection_details.show(10)
    comp_details.show(10)
    country_details.show(10)
    language_details.show(10)


def clean_credits(inputs):
    credits_schema = types.StructType(
        [
            types.StructField("cast", types.StringType()),
            types.StructField("crew", types.StringType()),
            types.StructField("tmdb_id", types.StringType()),
        ]
    )
    credits_data = spark.read.csv(inputs, schema=credits_schema, header=True)
    credits_data = credits_data.select(movie_data['adult'].cast('boolean'),
                                   movie_data['belongs_to_collection'],
                                   movie_data['budget'].cast('int'),
                                   movie_data['genres'],  
                                   movie_data['homepage'],
                                   movie_data['id'].cast('int').alias('tmdb_id'),
                                   movie_data['imdb_id'],
                                   movie_data['original_language'],
                                   movie_data['original_title'],
                                   movie_data['overview'],
                                   movie_data['popularity'].cast('double'),
                                   movie_data['poster_path'],
                                   movie_data['production_companies'],
                                   movie_data['production_countries'],
                                   functions.to_date(movie_data['release_date'], 'MM/dd/yy').alias('release_date'),
                                   movie_data['revenue'].cast('long'),
                                   movie_data['runtime'].cast('int'),
                                   movie_data['spoken_languages'],
                                   movie_data['status'],
                                   movie_data['tagline'],
                                   movie_data['title'],
                                   movie_data['video'].cast('boolean'),
                                   movie_data['vote_average'].cast('float'),
                                   movie_data['vote_count'].cast('int'))  


def main(inputs, output_dir):
          
    clean_metadata(inputs,output_dir)

if __name__ == '__main__':
    output_dir = sys.argv[2]
    inputs = sys.argv[1]

    spark = SparkSession.builder.appName("data prep").getOrCreate()
    assert spark.version >= "2.4"  # make sure we have Spark 2.4+
    spark.sparkContext.setLogLevel("WARN")
    sc = spark.sparkContext
    main(inputs, output_dir)