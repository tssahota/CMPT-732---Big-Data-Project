# CMPT-732---Big-Data-Project
Movie Data Analysis and Success Pediction

Raw Datasets
Contains unprocessed datasets (input files for data cleaning jobs) in .csv format:

Processed Data
Contains cleaned, tranasformed and aggregated data as parquet files.

Schemas: 
movie_data:
root
 |-- adult: boolean (nullable = true)
 |-- homepage: string (nullable = true)
 |-- tmdb_id: integer (nullable = true)
 |-- imdb_id: string (nullable = true)
 |-- original_language: string (nullable = true)
 |-- original_title: string (nullable = true)
 |-- overview: string (nullable = true)
 |-- popularity: double (nullable = true)
 |-- poster_path: string (nullable = true)
 |-- release_date: date (nullable = true)
 |-- runtime: integer (nullable = true)
 |-- status: string (nullable = true)
 |-- tagline: string (nullable = true)
 |-- title: string (nullable = true)
 |-- video: boolean (nullable = true)
 |-- vote_average: float (nullable = true)
 |-- vote_count: integer (nullable = true)
 |-- budget: integer (nullable = true)
 |-- revenue: long (nullable = true)
 |-- profit: long (nullable = true)
 |-- collection_ids: array (nullable = true)
 |    |-- element: integer (containsNull = true)
 |-- genre_ids: array (nullable = true)
 |    |-- element: integer (containsNull = true)
 |-- production_company_ids: array (nullable = true)
 |    |-- element: integer (containsNull = true)
 |-- prod_country_ids: array (nullable = true)
 |    |-- element: string (containsNull = true)
 |-- language_id: array (nullable = true)
 |    |-- element: string (containsNull = true)
 |-- movie_id: integer (nullable = true)
 |-- avg_user_rating: double (nullable = true)
 |-- cast_ids: string (nullable = true)
 |-- crew_ids: string (nullable = true)
 |-- keyword_ids: string (nullable = true)

genre_details:
root
 |-- genre_id: integer (nullable = true)
 |-- genre_name: string (nullable = true)

collection_details:
root
 |-- collection_ids: integer (nullable = true)
 |-- collection_name: string (nullable = true)
 |-- collection_poster_path: string (nullable = true)
 |-- collection_backdrop_path: string (nullable = true)

company_details:
root
 |-- company_id: integer (nullable = true)
 |-- production_company: string (nullable = true)

country_details:
root
 |-- country_id: string (nullable = true)
 |-- country: string (nullable = true)


language_details:
root
 |-- lang_id: string (nullable = true)
 |-- language: string (nullable = true)


Data_Cleaning
Make sure all double quotes (") in csv are changed to single quotes ('). If not, use Replace all to do so.
Make sure the dates are in format MM/dd/yyyy.


ETL_movie_metadata.py:
Cleans movies_metadata.csv and produces movie_data, genre_details, collection_details, company_details, country_details, language_details


Commands: 
1. ETL_movie_metadata.py:
spark-submit ETL_movie_metadata.py movies_metadata.csv <output_directory> overwrite 

Eg: spark-submit Data_Cleaning/ETL_movie_metadata.py Raw_Datasets/movies_metadata.csv Processed_Data overwrite

2. ETL_movieid_links.py:
spark-submit ETL_movieid_links.py links.csv <output_directory> overwrite 

3.ETL_user_ratings.py
spark-submit ETL_user_ratings.py ratings.csv <output_directory> overwrite

4. ETL_keywords.py
spark-submit ETL_keywords.py keywords.csv <output_directory> overwrite 

5. ETL_credits.py
spark-submit ETL_credits.py credits.csv <output_directory> overwrite

Prediction:
feature_engineering.py
/spark-submit feature_engineering.py <processed_data_directory> <training_data_directory>

# eric_analysis.py
spark-submit eric_analysis.py

# eric_ui.py
python eric_ui.py

# UI
pip install pandas
pip install dash
pip install pyarrow
pip install dash-bootstrap-components

cd web_dev
python main.py