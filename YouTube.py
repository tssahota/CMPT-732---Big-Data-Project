import sys
assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+
from pyspark.sql import SparkSession, functions, types, Row
from pyspark.sql.functions import year, month, dayofmonth, concat, col, lit
from apiclient.discovery import build 

DEVELOPER_KEY = "AIzaSyDku6E5nukRmgakP6eMAqbLYgCrPTwvb6c" 
YOUTUBE_API_SERVICE_NAME = "youtube"
YOUTUBE_API_VERSION = "v3"
# creating Youtube Resource Object 
youtube_object = build(YOUTUBE_API_SERVICE_NAME, YOUTUBE_API_VERSION, 
                                        developerKey = DEVELOPER_KEY)
# creating youtube resource object 
# for interacting with API 
youtube = build(YOUTUBE_API_SERVICE_NAME,  
                     YOUTUBE_API_VERSION, 
            developerKey = DEVELOPER_KEY)

youtube_schema = types.StructType(
        [
            types.StructField("tmdb_id", types.StringType()),
            types.StructField("youtube_views", types.StringType()),
            types.StructField("youtube_likes", types.StringType()),
            types.StructField("youtube_dislikes", types.StringType()),     
        ]
    )

def multiple_video_details(ids):    
    # Call the videos.list method 
    # to retrieve video info 
    list_videos_byid = youtube.videos().list( 
             id = ids, 
      part = "id, statistics", 
                                  ).execute() 
 # extracting the results from search response 
    results = list_videos_byid.get("items", [])
    # empty list to store video details 
    videos = [] 
    max_views = 0
    ret_result = ''
    for result in results: 
        views = 0
        try:
            views = int(result['statistics']['viewCount'])
        except Exception as e:
            print("Exception while getting viewCount")
        if views > max_views:
            max_views = views
            ret_result = result
    return ret_result
    
def youtube_search_keyword(query, max_results):        
    # calling the search.list method to 
    # retrieve youtube search results 
    search_keyword = youtube_object.search().list(q = query, part = "id, snippet", 
                                               maxResults = max_results).execute() 
    # extracting the results from search response 
    results = search_keyword.get("items", []) 
    video_ids = [] 
    # extracting required info from each result object
    for result in results: 
        # video result object 
        if result['id']['kind'] == "youtube#video":
            video_ids.append(result["id"]["videoId"])
    stats = multiple_video_details(",".join(video_ids))  
    return stats
    
def youtube_api_call(row):   
    views = likes = dislikes = 0
    stats = youtube_search_keyword(str(row['searchby']),  max_results = 5)
    try:
        views = stats['statistics']['viewCount']
        likes = stats['statistics']['likeCount']
        dislikes =  stats['statistics']['dislikeCount']
    except Exception as e:
        print(e)   
    return row['tmdb_id'], views, likes, dislikes
    
def main(input_directory, output_directory):
    #remove null values for budget and revenue and split data into train/validation and test
    data = spark.read.parquet(input_directory + "/movies_aggregated_data.parquet")
    data = data.where(data['budget'].isNotNull() & data['revenue'].isNotNull() & data['runtime'].isNotNull() & data['title'].isNotNull() & data['release_date'].isNotNull() & data['imdb_id'].isNotNull())
    df = data.select("tmdb_id", concat(col("title"), lit(" "), year(col("release_date")), lit(" trailer")).alias('searchby'))
    result = df.rdd.map(youtube_api_call)
    result_df = spark.createDataFrame(data=result, schema=youtube_schema)
    result_df.write.mode('overwrite').parquet(output_directory + "/youtube_data.parquet")

if __name__ == '__main__':
    input_directory = sys.argv[1]
    output_directory = sys.argv[2]
    spark = SparkSession.builder.appName("user ratings").getOrCreate()
    assert spark.version >= "2.4"  # make sure we have Spark 2.4+
    spark.sparkContext.setLogLevel("WARN")
    sc = spark.sparkContext
    main(input_directory, output_directory)
