# CMPT-732---Big-Data-Project
Movie Data Analysis and Success Pediction

Raw Datasets
Contains unprocessed datasets (input files for data cleaning jobs) in .csv format:

Processed Data
Contains cleaned, tranasformed and aggregated data as parquet files.

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
spark-submit eric_analysis.py <input_directory> <output_directory>
Recommended: spark-submit eric_analysis.py Processed_Data/ web-dev/apps/analysis_data/

# eric_ui.py
python eric_ui.py

# UI
pip install pandas
pip install dash
pip install pyarrow
pip install dash-bootstrap-components
pip install iso3166
pip install wordcloud

cd web_dev
python main.py