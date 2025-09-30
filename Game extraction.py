import requests
import json
import time
import polars as pl
from datetime import date
import os


def get_all_app_ids():
    url         = "https://api.steampowered.com/ISteamApps/GetAppList/v2/"
    response    = requests.get(url)
    response.raise_for_status()
    all_apps    = response.json()["applist"]["apps"]
    return [all_apps[app]["appid"] for app in range(len(all_apps))]


def get_app_details(appid: int, country_code: str = "nl", language: str = "english"):
    """
    Input:
    - appid (int):            The id of the app/game.
    - country_code (str):     Two-letter country code (ISO 3166-1 alpha-2), for respective currency and regional availability.
    - language (str):         Language of the text fields.

    Returns:
    - Information regarding the input appid, in JSON format.

    """
    url         = "https://store.steampowered.com/api/appdetails"
    params      = {"appids": appid, "cc": country_code, "l": language}
    response    = requests.get(url, params=params)
    response.raise_for_status()
    app_data    = response.json()
    app_key     = str(appid)
    if (app_key in app_data) and (app_data[app_key]["success"]):  # if the id was actually called and a success, return the data
        return app_data[app_key]["data"]
    return None


# Store parsed game data in one big dictionary
all_game_data           = {}

# Store category and genre data in a list of dictionaries
all_game_categories     = []
game_category_tag       = []
all_game_genres         = []
game_genre_tag          = []

# Used to avoid storing duplicate categories and genres 
unique_game_categories  = set()
unique_game_genres      = set()


# Extracting data:
all_app_ids = get_all_app_ids()

for app_id in all_app_ids:
    app_details = get_app_details(app_id)
    if app_details:
        all_game_data[app_id] = {
            "name":             app_details.get("name"),
            "type":             app_details.get("type"),
            "release_date":     app_details.get("release_date", {}).get("date"),  # HACK: handles games with no release date
            "is_free":          app_details.get("is_free"),
            "price_overview":   app_details.get("price_overview"),
            "metacritic_score": app_details.get("metacritic", {}).get("score"),
        }
        
        for category in app_details.get("categories", []):
            category_id, category_name = category["id"], category["description"]
            if category_id not in unique_game_categories:
                all_game_categories.append({
                    "category_id":      category_id, 
                    "category_name":    category_name
                })
                unique_game_categories.add(category_id)

            game_category_tag.append({
                "game_id":      app_id,
                "category_id":  category_id,
            })

        for genre in app_details.get("genres", []):
            genre_id, genre_name = genre["id"], genre["description"]
            if genre_id not in unique_game_genres:
                all_game_genres.append({
                    "genre_id":     genre_id, 
                    "genre_name":   genre_name
                })
                unique_game_genres.add(genre_id)

            game_genre_tag.append({
                "game_id":  app_id,
                "genre_id": genre_id,
            })

    time.sleep(1)



# TODO: write to a better file structure for easier Load process

# Writing the data to Parquet files:
date_today = date.today().isoformat

data_tables = {
    "games":                all_game_data,
    "categories":           all_game_categories,
    "game_category_tag":    game_category_tag,
    "genres":               all_game_genres,
    "game_genre_tag":       game_genre_tag,
}

for name, records in data_tables.items():
    # Move on to the next table if there are no records
    if not records:
        continue
    folder = f"data/{name}/extraction_date = {today}"
    os.makedirs(folder, exist_ok=True)
    pl.DataFrame(records).write_parquet(f"{folder}/{name}.parquet")


# TODO: decide on update frequency and implement upsert strategy when needed
