import requests
import pandas as pd
import json

def basic_dataset():
    url = "https://us-housing-market-data.p.rapidapi.com/getZipcode"

    querystring = {"zipcode":"02043"}

    headers = {
        "x-rapidapi-key": "ffe040bd17mshd652bea9bae3e76p15c295jsnf76576625ece",
        "x-rapidapi-host": "us-housing-market-data.p.rapidapi.com"
    }

    response = requests.get(url, headers=headers, params=querystring)
    json_data = response.json()

    with open('Data_Files/rapidapi_us_housing.json', 'w') as f:
        json.dump(json_data, f, indent=4)

    for key, value in json_data.items():
        print(f"{key}: {value}")

def enriched_dataset():

    url = "https://us-housing-market-data.p.rapidapi.com/getZipcodeEnriched"

    querystring = {"zipcode": "02043"}

    headers = {
        "x-rapidapi-key": "ffe040bd17mshd652bea9bae3e76p15c295jsnf76576625ece",
        "x-rapidapi-host": "us-housing-market-data.p.rapidapi.com"
    }

    response = requests.get(url, headers=headers, params=querystring)

    json_data = response.json()

    with open('Data_Files/rapidapi_us_housing_enriched.json', 'w') as f:
        json.dump(json_data, f, indent=4)

enriched_dataset()