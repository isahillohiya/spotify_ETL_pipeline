
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import boto3

from awsglue.dynamicframe import DynamicFrame
from pyspark.sql import Window
from pyspark.sql.functions import col,explode  ,to_date,monotonically_increasing_id,row_number
from datetime import datetime
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
s3_bucket_path = "s3://spotify-etl-ap-south1-dev-sahil/raw_data/to_processed/"
s3_output_path = f"s3://spotify-etl-ap-south1-dev-sahil/transformed_data/"
source_dyf = glueContext.create_dynamic_frame_from_options(
    connection_type= 's3',
    connection_options = {'paths':[s3_bucket_path]},
    format = 'json',
    transformation_ctx = "source_dyf"
)
source_df = source_dyf.toDF()

def process_albums(df):
    df = df.withColumn("items", explode("items")).select(
        col("items.track.album.id").alias("album_id"),
        col("items.track.album.name").alias("album_name"),
        col("items.track.album.release_date").alias("release_date"),
        col("items.track.album.total_tracks").alias("total_tracks"),
        col("items.track.album.external_urls.spotify").alias("url")
    ).drop_duplicates(["album_id"])
    return df


def process_artists(df):
    # First, explode the items to get individual tracks
    df_items_exploded = df.select(explode(col("items")).alias("item"))
    
    # Then, explode the artists array within each item to create a row for each artist
    df_artists_exploded = df_items_exploded.select(explode(col("item.track.artists")).alias("artist"))
    
    # Now, select the artist attributes, ensuring each artist is in its own row
    df_artists = df_artists_exploded.select(
        col("artist.id").alias("artist_id"),
        col("artist.name").alias("artist_name"),
        col("artist.external_urls.spotify").alias("external_url")
    ).drop_duplicates(["artist_id"])
    
    return df_artists

def process_songs(df):
    # Explode the items array to create a row for each song
    df_exploded = df.select(explode(col("items")).alias("item"))
    
    # Extract song information from the exploded DataFrame
    df_exploded = df_exploded.withColumn("counter", monotonically_increasing_id())
    w = Window.orderBy("counter")
    df_exploded = df_exploded.withColumn("week_rank", row_number().over(w))
    df_songs = df_exploded.select(
            col("item.track.id").alias("song_id"),
            col("item.track.name").alias("song_name"),
            col("item.track.duration_ms").alias("duration_ms"),
            col("week_rank"),
            col("item.track.external_urls.spotify").alias("url"),
            col("item.track.popularity").alias("popularity"),
            col("item.added_at").alias("song_added"),
            col("item.track.album.id").alias("album_id"),
            col("item.track.artists")[0]["id"].alias("artist_id")
        ).drop_duplicates(["song_id"])
    
    # Convert string dates in 'song_added' to actual date types
    df_songs = df_songs.withColumn("song_added", to_date(col("song_added")))
    
    return df_songs


def write_to_s3(df, prefix, format_type="csv"):
    # Convert back to DynamicFrame
    dynamic_frame = DynamicFrame.fromDF(df, glueContext, "dynamic_frame")
    path = f"{s3_output_path}{prefix}"
    print(path)
    glueContext.write_dynamic_frame.from_options(
        frame = dynamic_frame,
        connection_type = "s3",
        connection_options = {"path": path},
        format = format_type
    )

#process data
album_df = process_albums(source_df)
artist_df = process_artists(source_df)
song_df = process_songs(source_df)

song_df.count()
dt = datetime.now().strftime("%Y-%m-%d")
write_to_s3(album_df, "album/album_transformed_{}".format(dt), "csv")
write_to_s3(artist_df, "artist/artist_transformed_{}".format(dt), "csv")
write_to_s3(song_df, "songs/songs_transformed_{}".format(dt), "csv")

def list_s3_objects(bucket, prefix):
    s3 = boto3.resource('s3')
    bucket = s3.Bucket(bucket)
    objects = [obj.key for obj in bucket.objects.filter(Prefix=prefix)]
    return objects
def move_and_delete_files(keys,Bucket):
    s3_resource = boto3.resource("s3")
    for key in keys:
        copy = {
            "Bucket" : Bucket,
            "Key" : key
        }
        # define the destination key
        destination_key = key.replace("to_processed", "processed") + key.split("/")[-1]
        
        s3_resource.meta.client.copy(copy, Bucket, destination_key)
        
        s3_resource.Object(Bucket, key).delete()
lst = list_s3_objects("spotify-etl-ap-south1-dev-sahil","raw_data/to_processed/")
move_and_delete_files(lst, "spotify-etl-ap-south1-dev-sahil")
job.commit()
