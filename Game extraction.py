import requests
import json
import time


# Apps are games within the API
def get_all_apps():
    url = "https://api.steampowered.com/ISteamApps/GetAppList/v2/"
    response = requests.get(url)
    response.raise_for_status()
    return response.json()["applist"]["apps"]


def get_app_details(appid: int, country_code: str = "nl", language: str = "english"):
    """
    Input:
    - appid (int):            The id of the app/game.
    - country_code (str):     Two-letter country code (ISO 3166-1 alpha-2), for respective currency and regional availability.
    - language (str):         Language of the text fields.

    Returns:
    - Information regarding the input appid, in JSON format.

    """
    url = "https://store.steampowered.com/api/appdetails"
    params = {"appids": appid, "cc": country_code, "l": language}
    response = requests.get(url, params=params)
    response.raise_for_status()
    app_data = response.json()
    if (str(appid) in app_data) and (app_data[str(appid)]["success"]):
        return app_data[str(appid)]["data"]
    return None


# data exploration (since there is little to no documentation)
id = 4031890  # Borderlands 2
url = f"https://store.steampowered.com/api/appdetails/?appids={id}"
response3 = requests.get(url)
response3.raise_for_status()
test3 = response3.json()[str(id)]["data"]
test3.keys()

test3["categories"]
