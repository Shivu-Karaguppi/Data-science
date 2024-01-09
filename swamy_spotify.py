import spotipy
from spotipy.oauth2 import SpotifyClientCredentials

client_id='f77396169db54abbbf875d54b1c473c7'
client_secret='fde71c32db2c4d1d85f801c8e8d4e7a0'

# Authenticate with the Spotify API
client_credentials_manager = SpotifyClientCredentials(client_id=client_id, client_secret=client_secret)
sp = spotipy.Spotify(client_credentials_manager=client_credentials_manager)

# Get the top artists on Spotify currently
top_artists = sp.category_playlists(category_id='toplists')['playlists']['items'][0]
tracks = sp.playlist_tracks(top_artists['id'])['items']

artists_today = []

for track in tracks:
    artist = track['track']['artists'][0]['name']
    if artist not in artists_today:
        artists_today.append(artist)
    if len(artists_today) == 10:
        break

print("Top 10 Artists on Spotify today:")
for i, artist in enumerate(artists_today, start=1):
    print(f"{i}. {artist}")