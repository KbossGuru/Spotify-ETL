import spotipy
import pandas as pd
from spotipy.oauth2 import SpotifyClientCredentials
import os

#spotify API credentials
CLIENT_ID = '94bb8fcecdef444ba4104d0167cf00fe'
CLIENT_SECRET = '0358eda5d6934213961b4d00672339ee'

#Authenticate with Spotify API
client_credentials_manager = SpotifyClientCredentials(client_id= CLIENT_ID, client_secret= CLIENT_SECRET)
sp = spotipy.Spotify(client_credentials_manager= client_credentials_manager)

#Get the tracks from a playlist
def fetch_top_tracks(playlist_id):
    results = sp.playlist_tracks(playlist_id)
    tracks = results['items']
    while results['next']:
        results = sp.next(results)
        tracks.extend(results['items'])
    return tracks

#playlist ID for intended playlist of use
Top_50_Nigeria = '37i9dQZEVXbKY7jLzlJ11V'

#fetch top tracks from playlist
top_tracks = fetch_top_tracks(Top_50_Nigeria)

#Get artiste genre
def get_artist_genre(artist_id):
    artist = sp.artist(artist_id)
    return artist['genres']

# Extract desired data from the playlist and load 
def extract_and_store_data():
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
    output_path = "top_50_naija.csv"
    print(f"Saving CSV to {output_path}")
    df.to_csv(output_path, index=False)
    print(f"CSV saved successfully to {output_path}")


if __name__ == "__main__":
    extract_and_store_data()