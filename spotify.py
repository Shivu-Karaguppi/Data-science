client_id='f77396169db54abbbf875d54b1c473c7'
client_secret='fde71c32db2c4d1d85f801c8e8d4e7a0'

import base64
import requests

# client_id = 'CLIENT_ID'
# client_secret = 'CLIENT_SECRET'

# Encode client_id and client_secret in base64
credentials = base64.b64encode(f"{client_id}:{client_secret}".encode('utf-8')).decode('utf-8')

# Set up the authentication options
auth_options = {
    'url': 'https://accounts.spotify.com/api/token',
    'headers': {
        'Authorization': 'Basic ' + credentials
    },
    'data': {
        'grant_type': 'client_credentials',
        'scope': "user-library-read playlist-modify-private",
    }
}

# Make the POST request
response = requests.post(**auth_options)

if response.status_code == 200:
    global token
    token = response.json().get('access_token')
    print(f'Token: {token}')
else:
    print(f'Error: {response.status_code}')
    print(response.text)
global artx
artx='Arjit Singh'

def artist_given(token,artist_name=artx):
    headers = {
    'Authorization': f'Bearer {token}',
    }

    params = {
    'q': artist_name,
    'type': 'artist',
        }
    response = requests.get('https://api.spotify.com/v1/search', headers=headers, params=params)
    artists = response.json()['artists']['items']
    artist_id = artists[0]['id']
    return artist_id

import requests
access_token = token

# Replace 'ARTIST_ID' with the desired artist ID
artist_id = artist_given(token)

# Set up the request headers
global headers
headers = {
    'Authorization': f'Bearer {access_token}',
}

# Set up the request URL
url = f'https://api.spotify.com/v1/artists/{artist_id}/top-tracks'

# Set up the request parameters
params = {
    'country': 'ES',  # Replace with the desired country code
}

# Make the GET request to get the top tracks for the artist
response = requests.get(url, headers=headers, params=params)

# Print the names of the top tracks
if response.status_code == 200:
    top_tracks = response.json()['tracks']
    for track in top_tracks:
        print(track['name'])
else:
    print(f'Error: {response.status_code}')
    print(response.text)


def get_artist(artist_name):
        # artx='Juice WRLD'
        url = f'https://api.spotify.com/v1/search?q={artx}&type=artist&limit=1'
        response = requests.get(url, headers=headers)
        artist_data = response.json()

        id = artist_data['artists']['items'][0]['id']
        name = artist_data['artists']['items'][0]['name']
        followers = artist_data['artists']['items'][0]['followers']['total']
        try:
            genre = artist_data['artists']['items'][0]['genres'][0]
        except:
            genre = "NA"
        popularity = artist_data['artists']['items'][0]['popularity']

        redefined_artist_data = {"id": id,
                                 "name": name,
                                 "followers": followers,
                                 "genre": genre,
                                 "popularity": popularity}
        return redefined_artist_data

print(get_artist('Juice WRLD'))

