import requests
import json

import os
from dotenv import load_dotenv
from datetime import date
load_dotenv(dotenv_path=".env")

API_KEY = os.getenv("API_KEY")
CHANNEL_HANDLE = "MrBeast"
max_results = 50
def get_playlist_id():
    try:
        url = f"https://youtube.googleapis.com/youtube/v3/channels?part=contentDetails&forHandle={CHANNEL_HANDLE}&key={API_KEY}"

        response = requests.get(url)

        data = response.json()


        channel_items = data["items"][0]
        channel_playlistId = channel_items["contentDetails"]["relatedPlaylists"]["uploads"]

        return channel_playlistId
    except requests.exceptions.RequestException as e:
        raise e    
playlist_id = get_playlist_id()

def get_video_ids(playlist_id):
    video_Ids = []
    pageToken = None
    base_url = f"https://youtube.googleapis.com/youtube/v3/playlistItems?part=contentDetails&maxResults={max_results}&playlistId={playlist_id}&key={API_KEY}"

    try:
        while True:
            url = base_url

            if pageToken:
                url += f"&pageToken={pageToken}"
            response = requests.get(url)
            response.raise_for_status()
            data = response.json()

            for item in data["items"]:
                video_id = item["contentDetails"]["videoId"]
                video_Ids.append(video_id)
            pageToken = data.get("nextPageToken")
            
            if not pageToken:
                break
        return video_Ids        
    
    except requests.exceptions.RequestException as e:
        raise e    
#https://youtube.googleapis.com/youtube/v3/videos?part=contentDetails&part=snippet&part=statistics&key=[YOUR_API_KEY]

def extract_video_data(video_ids):
    extracted_data = []
    def batch_list(video_ids, batch_size):
        for i in range(0, len(video_ids), batch_size):
            yield video_ids[i:i + batch_size]
    try:
        for batch in batch_list(video_ids, max_results):
            video_ids_str = ",".join(batch)        
            url = f"https://youtube.googleapis.com/youtube/v3/videos?part=contentDetails&part=snippet&id={video_ids_str}&part=statistics&key={API_KEY}"        
            response = requests.get(url)
            response.raise_for_status()
            data = response.json()
            for item in data.get("items", []):
                video_id = item['id']
                snippet = item['snippet']
                contentDetails = item['contentDetails']
                statistics = item['statistics']
        
                video_data ={
                    "video_id": video_id,
                    "title": snippet["title"],
                    "publishedAt": snippet["publishedAt"],
                    "duration": contentDetails["duration"],
                    "viewCount": statistics.get('viewCount', None),
                    "likeCount": statistics.get('likeCount', None),
                    "commentCount": statistics.get('commentCount', None)
                }
                extracted_data.append(video_data)
        return extracted_data
    except requests.exceptions.RequestException as e:
        raise e
    return extracted_data
def save_to_json(data):
    file_path = f"./data/YT_date{date.today()}.json"
    with open(file_path, "w", encoding="utf-8") as json_outfile:
        json.dump(data, json_outfile, indent=4, ensure_ascii=False)

if __name__ == "__main__":
    playlist_id = get_playlist_id()
    video_ids = get_video_ids(playlist_id)
    video_data =extract_video_data(video_ids)
    save_to_json(video_data)    
