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

# Dash App (UI)

1. Change current directory to web_dev folder from the root of git folder
cd web_dev

2. Install necessary packages in Python
pip install pandas
pip install dash
pip install pyarrow
pip install dash-bootstrap-components
pip install Pillow
pip install iso3166

3. Run Dash App
python app.py

* Tested with python==3.6.0 in Anaconda virtual environment.

* Images files are in web_dev/img folder.

* web_dev/apps folder contain dash code of different pages.

* web_dev/apps/analysis_data contain processed data from spark which divide into corresponding task folder.