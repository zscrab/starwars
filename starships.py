import pymongo
import requests
import json
from pprint import pprint
starships_all = requests.get("https://swapi.dev/api/starships").json()
starships_link = "https://swapi.dev/api/starships"
client = pymongo.MongoClient()
db = client["starwars"]
db.starships.drop()
ss_c = db["starships"]


def api_scrape(link):
    api_response = requests.get(link)
    api_results = json.loads(api_response.content)
    for i in api_results['results']:
        yield i
    if 'next' in api_results and api_results['next'] is not None:
        next_page = api_scrape(api_results['next'])
        for page in next_page:
            yield page


def create_collection():
    for result in api_scrape("https://swapi.dev/api/starships"):
        ss_c.insert_one(result)


def link_pilots():
    for doc in ss_c:
        if ['pilots'] is not None:
            for pilot in ['pilots']:
                ss_c.aggregate([{"$lookup":
                                     {"from": "characters", "localField": "pilot", "foreignField": "_id",
                                      "as": "matched_pilot"}
                                 }])


create_collection()
link_pilots()
