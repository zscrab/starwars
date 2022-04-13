import pymongo
import requests
import json

client = pymongo.MongoClient()
db = client["starwars"]
# drop db.starships and create a new db.starships
db.starships.drop()
ss_c = db["starships"]


def api_scrape(link):
    """a function to scrape an api and turn the pages of the results.
    I used yield instead of return so that I could iterate over the function
    and used (link) so in theory it would work with any api"""
    api_response = requests.get(link)
    api_results = json.loads(api_response.content)
    # for loop with 'yield' so my second function can iterate over this one
    for i in api_results['results']:
        yield i
    # if the next page link is not none follow this link, yield so I can iterate
    if 'next' in api_results and api_results['next'] is not None:
        next_page = api_scrape(api_results['next'])
        for page in next_page:
            yield page


def create_collection():
    """function to create and upload the collection to my db"""
    for result in api_scrape("https://swapi.dev/api/starships"):
        if result["pilots"] is not None:
            n = 0
            while n < len(result["pilots"]):
                for pilot in result["pilots"]:
                    # here I replace the pilots section while looking up the pilot _id via the api
                    # this would not be possible if I did not use yield to iterate
                    result["pilots"][n] = db.characters.find_one({"name": requests.get(pilot).json()['name']})["_id"]
                    n += 1
        # inserting one result at a time to my db due to yield
        ss_c.insert_one(result)


# running the function


create_collection()


