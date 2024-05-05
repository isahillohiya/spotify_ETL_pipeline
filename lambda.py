import json
import os
import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
import boto3
from datetime import datetime

def lambda_handler(event, context):
    
    cilent_id = os.environ.get('client_id')
    client_secret = os.environ.get('client_secret')
    
    client_credentials_manager = SpotifyClientCredentials(client_id=cilent_id, client_secret=client_secret)
    sp = spotipy.Spotify(client_credentials_manager = client_credentials_manager)
    playlists = sp.user_playlists('spotify')
    
    playlist_link = os.environ.get("playlist_link")
    playlist_URI = playlist_link.split("/")[-1].split("?")[0]
    
    spotify_data = sp.playlist_tracks(playlist_URI)   
    
    cilent = boto3.client('s3')
    
    filename = "spotify_raw_" + str(datetime.now()) + ".json"
    
    bucket_name = os.environ.get("bucket_name")
    cilent.put_object(
        Bucket=bucket_name,
        Key="raw_data/to_processed/" + filename,
        Body=json.dumps(spotify_data)
        )
    print("lambda execution complate")
    
    glue= boto3.client("glue")
    glue_job_name = "spotify_transformations"
    
    try:
        run_id = glue.start_job_run(JobName=glue_job_name)
        status = glue.get_job_run(JobName=glue_job_name,RunId=run_id['JobRunId'])
        print("Job staus",status['JobRun']['JobRunState'])
    except Exception as e:
        print("got error in glue",e)
