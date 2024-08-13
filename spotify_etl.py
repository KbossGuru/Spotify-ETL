import spotipy
import pandas as pd
from spotipy.oauth2 import SpotifyClientCredentials
import config
import boto3
import io
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# AWS credentials (ensure these are stored securely)
AWS_ACCESS_KEY_ID = config.AWS_ACCESS_KEY_ID
AWS_SECRET_ACCESS_KEY = config.AWS_SECRET_ACCESS_KEY
S3_BUCKET_NAME = 'spotifypipelinebucket'
S3_FILE_KEY = 'spotifydata.csv'

#spotify API credentials
CLIENT_ID = config.CLIENT_ID
CLIENT_SECRET = config.CLIENT_SECRET

#Authenticate with Spotify API
client_credentials_manager = SpotifyClientCredentials(client_id= CLIENT_ID, client_secret= CLIENT_SECRET)
sp = spotipy.Spotify(client_credentials_manager= client_credentials_manager)

#Get the tracks from a playlist
def fetch_top_tracks(playlist_id):
    try:
        results = sp.playlist_tracks(playlist_id)
        tracks = results['items']
        while results['next']:
            results = sp.next(results)
            tracks.extend(results['items'])
        return tracks
    except Exception as e:
        logger.error(f"Error fetching top tracks: {e}")
        raise

#playlist ID for intended playlist of use
Top_50_Nigeria = '37i9dQZEVXbKY7jLzlJ11V'

#fetch top tracks from playlist
top_tracks = fetch_top_tracks(Top_50_Nigeria)

#Get artiste genre
def get_artist_genre(artist_id):
    try:
        artist = sp.artist(artist_id)
        return artist['genres']
    except Exception as e:
        logger.error(f"Error fetching artist genre: {e}")
        raise

# Extract desired data from the playlist and load 
def extract_and_store_data():
    try:
        track_data = []
        for idx, item in enumerate(top_tracks):
            track = item['track']
            track_id = track['id']
            track_name = track['name']
            artist_name = track['artists'][0]['name']
            artist_id = track['artists'][0]['id']
            album_name = track['album']['name']
            release_date = track['album']['release_date']
            num_plays = track['popularity']
            genre = get_artist_genre(artist_id)
            album_cover_url = track['album']['images'][0]['url']
            track_length_ms = track['duration_ms']
            track_length_min = track_length_ms / 60000  

            #put all the data into the track data list
            track_data.append({
                'Release Date': release_date,
                'Song': track_name,
                'Artist': artist_name,
                'Album Name': album_name,
                'Popularity(/100)': num_plays,
                'Genre': genre,
                'Album cover': album_cover_url,
                'Track Length': track_length_min
            })

        #Save the resulting data into a CSV file
        df = pd.DataFrame(track_data)
        return df
    except Exception as e:
        logger.error(f"Error in data extraction and storage: {e}")
        raise

#transform the column to the required format
def transform_data():
    try:
        df = extract_and_store_data()
        df['Track Length'] = round(df['Track Length'], 2)
        return df
    except Exception as e:
        logger.error(f"Error in data transformation: {e}")
        raise

def load_data_to_s3():
    try:
        df = transform_data()
        csv_buffer = io.StringIO()
        df.to_csv(csv_buffer, index=False)

        s3_resource = boto3.resource('s3',
                                    aws_access_key_id=AWS_ACCESS_KEY_ID,
                                    aws_secret_access_key=AWS_SECRET_ACCESS_KEY)
        
        s3_resource.Object(S3_BUCKET_NAME, S3_FILE_KEY).put(Body=csv_buffer.getvalue())
        print(f"Data saved to S3: {S3_BUCKET_NAME}/{S3_FILE_KEY}")
    except Exception as e:
        logger.error(f"Error loading data to S3: {e}")
        raise

