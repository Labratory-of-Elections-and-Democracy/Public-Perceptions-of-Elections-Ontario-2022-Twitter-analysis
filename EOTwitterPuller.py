import os
import pickle
import traceback
from datetime import datetime
import time

import pandas as pd
import tweepy
from tqdm import tqdm, trange

import API_KEYS


def main(auth, api, client):
    HASHTAGS = [
        "#VoteSelfie",
        "#OntarioElection2022",
        "#ontarioelection",
        "#OntarioVotes",
        "#ONVotes",
        "#ElxnON",
        "#ONElxn",
        "#OnPoli",
        "#ONelxn22",
        "#aoda",
        "#cndpoli",
        "#FirstNationsVote",
        "#50knotok",
        "#50kisnotok"
    ]
    USERS = [
        "@ElectionsON",
        "@AndreaHorwath",
        "@StevenDelDuca",
        "@MikeSchreiner",
        "@fordnation",
        "@ElectionsCan_E",
        "@ElectionsCan_F",
        "@cef_cce"
        "@ElectionsONfr"
    ]
    PHRASES = [
        "Rig rigging",
        "Missing card",
        "Voting card",
        "Voter card",
        "Polling station",
        "Poll",
        "Poll worker",
        "Lines",
        "Long lines",
        "Wait times",
        "Vote",
        "Voting",
        "Where to vote",
        "When to vote",
        "Advance polls",
        "Terrible",
        "Inaccessible",
        "Reduced",
        "Far",
        "Legal",
        "Late",
        "Location",
        "Right to vote",
        "Riding",
        "Party",
        "parties",
        "Participate",
        "Democracy",
        "Fail",
        "Elector",
        "Election",
        "Station",
        "Security",
        "Risk",
        "Closing",
        "Concerned Ontarian",
        "Results",
        "Turnout",
        "Complaint",
        "Suppression",
        "Irregularities",
        "Voter information",
        "Count",
        "Covid",
        "ID",
        "Open polls",
        "Elector",
        "Manipulate",
        "Cheat",
        "Great",
        "Awesome",
        "Nice",
        "Quick",
        "By mail",
    ]
    START_DATE = datetime.strptime("2022-04-26", "%Y-%m-%d")
    ELECTION_DATE = datetime.strptime("2022-06-02", "%Y-%m-%d")
    END_DATE = datetime.strptime("2022-06-09", "%Y-%m-%d")

    for USER in USERS:
        # if USER not in {'@fordnation'}:
        get_tweets(auth, api, client, START_DATE, END_DATE, tweets_from=USER)
        get_tweets(auth, api, client, START_DATE, END_DATE, tweets_to=USER)
        get_tweets(auth, api, client, START_DATE, END_DATE, user=USER)
    for HASHTAG in HASHTAGS:
        get_tweets(auth, api, client, START_DATE, END_DATE, hashtag=HASHTAG)


def automkdir(filename):
    if bool(os.path.dirname(filename)):
        os.makedirs(os.path.dirname(filename), exist_ok=True)


def pickle_dumper(file_to_try_to_dump, data):
    automkdir(file_to_try_to_dump)
    with open(file_to_try_to_dump, 'wb') as f:
        pickle.dump(data, f)

def query_wrapper(basic_quey_text, client, start, end, retweets=False, PickleFile=None, outfile=None):
    query = f"{basic_quey_text}"
    last_date=''
    if not retweets:
        query += " -is:retweet"
    print(f"Searching \'{query}\'")
    query_time = datetime.now()
    pages = tweepy.Paginator(client.search_all_tweets,
                             query=query,
                             end_time=end,
                             start_time=start,
                             user_fields=['username', 'public_metrics', 'description', 'location'],
                             tweet_fields=['created_at', 'geo', 'public_metrics', 'text'],
                             expansions=['author_id', 'entities.mentions.username', "geo.place_id"],
                             max_results=100)

    for idx, tweets in enumerate(pages):
        time.sleep(1.5)
        pickle_dumper(PickleFile.format(idx=idx), tweets)
        includes = tweets.includes
        users = includes["users"]
        users = {_user["id"]: _user for _user in users}
        places = includes.get("places", None)
        if places is not None:
            places = {places["id"]: places for places in places}
        else:
            places = dict()
        while True:
            try:
                outdata = []
                for tweet in tqdm(tweets.data, desc=f"Searching {query}", postfix=f"{last_date=}"):
                    outdata.append(
                        dict(
                            tweet_id=tweet.id,
                            author_id=tweet.author_id,
                            username=str(users.get(tweet.author_id, "None")),
                            tweet_text=tweet.text,
                            geodata=None if tweet.geo is None else tweet.geo.get('place_id'),
                            geoplace=None if tweet.geo is None else str(
                                places.get(tweet.geo.get('place_id'), "None")),
                            retweets=tweet.public_metrics["retweet_count"],
                            likes=tweet.public_metrics["like_count"],
                            quotes=tweet.public_metrics["quote_count"],
                            impressions=tweet.public_metrics["impression_count"],
                            created_at=tweet.created_at,
                            query=query,
                            query_time=query_time
                        )
                    )
                last_date = f" - {outdata[-1]['created_at']}"
                df = pd.DataFrame(outdata)
                automkdir(outfile.format(idx=idx))
                df.to_csv(outfile.format(idx=idx), index=False, encoding='utf-8')
            except:
                traceback.print_exc()
                print("Some Error, trying again in 5 min")
                for _ in trange(60 * 5):
                    time.sleep(1)
            else:
                break
def get_tweets(auth, api, client, start, end, hashtag=None, user=None, retweets=False, tweets_to=None, tweets_from=None):
    if hashtag is not None:
        PickleFile = f"G:/LED_LAB/HollyMaddyPickles/ByHashtag_{hashtag.replace('#', '')}_idx" + "{idx}.pickle"
        OutFile = f"G:/LED_LAB/HollyMaddyEOProject/ByHashtag_{hashtag.replace('#', '')}/TweetsFrom_{hashtag.replace('#', '')}" + "_idx_{idx}.csv"
        query_wrapper(hashtag, client, start, end, retweets=False, PickleFile=PickleFile, outfile=OutFile)
        # query_wrapper(hashtag, client, start, end, retweets=False, PickleFile=PickleFile, outfile=OutFile)
        # query = f"{hashtag}"
        # if not retweets:
        #     query += " -is:retweet"
        # print(f"Searching \'{query}\'")
        # query_time = datetime.now()
        # pages = tweepy.Paginator(client.search_all_tweets,
        #                          query=query,
        #                          end_time=end,
        #                          start_time=start,
        #                          user_fields=['username', 'public_metrics', 'description', 'location'],
        #                          tweet_fields=['created_at', 'geo', 'public_metrics', 'text'],
        #                          expansions=['author_id', 'entities.mentions.username', "geo.place_id"],
        #                          max_results=100)
        #
        # for idx, tweets in enumerate(pages):
        #     time.sleep(1.5)
        #     pickle_dumper(f"G:/LED_LAB/HollyMaddyPickles/HashTag_{hashtag.replace('#', '')}_idx{idx}.pickle", tweets)
        #     includes = tweets.includes
        #     users = includes["users"]
        #     users = {_user["id"]: _user for _user in users}
        #     places = includes.get("places", None)
        #     if places is not None:
        #         places = {places["id"]: places for places in places}
        #     else:
        #         places = dict()
        #     while True:
        #         try:
        #             outdata = []
        #             for tweet in tqdm(tweets.data, desc=f"Searching {hashtag}"):
        #                 outdata.append(
        #                     dict(
        #                         tweet_id=tweet.id,
        #                         author_id=tweet.author_id,
        #                         username=str(users.get(tweet.author_id, "None")),
        #                         tweet_text=tweet.text,
        #                         geodata=None if tweet.geo is None else tweet.geo.get('place_id'),
        #                         geoplace=None if tweet.geo is None else str(
        #                             places.get(tweet.geo.get('place_id'), "None")),
        #                         retweets=tweet.public_metrics["retweet_count"],
        #                         likes=tweet.public_metrics["like_count"],
        #                         quotes=tweet.public_metrics["quote_count"],
        #                         impressions=tweet.public_metrics["impression_count"],
        #                         created_at=tweet.created_at,
        #                         query=query,
        #                         query_time=query_time
        #                     )
        #                 )
        #             df = pd.DataFrame(outdata)
        #             outfile = f"G:/LED_LAB/HollyMaddyEOProject/ByHashtag_{hashtag.replace('#', '')}/ByHashtag_{hashtag.replace('#', '')}_idx_{idx}.csv"
        #             automkdir(outfile)
        #             df.to_csv(outfile, index=False, encoding='utf-8')
        #         except:
        #             traceback.print_exc()
        #             print("Some Error, trying again in 5 min")
        #             for _ in trange(60 * 5):
        #                 time.sleep(1)
        #         else:
        #             break
    if user is not None:
        PickleFile = f"G:/LED_LAB/HollyMaddyPickles/Mentioning_{user.replace('@', '')}_idx" + "{idx}.pickle"
        OutFile = f"G:/LED_LAB/HollyMaddyEOProject/Mentioning_{user.replace('@', '')}/Mentioning_{user.replace('@', '')}" + "_idx_{idx}.csv"
        query_wrapper(user, client, start, end, retweets=False, PickleFile=PickleFile, outfile=OutFile)
    if tweets_to is not None:
        user = tweets_to
        user2 = f"to:{user.replace('@', '')}"
        PickleFile = f"G:/LED_LAB/HollyMaddyPickles/TweetsTo_{user.replace('@', '')}_idx" + "{idx}.pickle"
        OutFile = f"G:/LED_LAB/HollyMaddyEOProject/TweetsTo_{user.replace('@', '')}/TweetsTo_{user.replace('@', '')}" + "_idx_{idx}.csv"
        query_wrapper(user2, client, start, end, retweets=False, PickleFile=PickleFile, outfile=OutFile)
    if tweets_from is not None:
        user = tweets_from
        user2 = f"from:{user.replace('@', '')}"
        PickleFile = f"G:/LED_LAB/HollyMaddyPickles/TweetsFrom_{user.replace('@', '')}_idx" + "{idx}.pickle"
        OutFile = f"G:/LED_LAB/HollyMaddyEOProject/TweetsFrom_{user.replace('@', '')}/TweetsFrom_{user.replace('@', '')}" + "_idx_{idx}.csv"
        query_wrapper(user2, client, start, end, retweets=False, PickleFile=PickleFile, outfile=OutFile)



if __name__ == '__main__':
    auth = tweepy.OAuthHandler(API_KEYS.TWITTER_KEYS.API_KEY, API_KEYS.TWITTER_KEYS.API_KEY_SECRET)
    auth.set_access_token(API_KEYS.TWITTER_KEYS.ACCESS_TOKEN, API_KEYS.TWITTER_KEYS.ACCESS_TOKEN_SECRET)

    api = tweepy.API(auth, wait_on_rate_limit=True)
    client = tweepy.Client(bearer_token=API_KEYS.TWITTER_KEYS.BEARER_TOKEN, wait_on_rate_limit=True)

    main(auth, api, client)
