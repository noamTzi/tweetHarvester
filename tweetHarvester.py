import requests
import expansions

import os
import json
import pandas as pd
from time import time, sleep

# keys - add keys here
user_keys = {'CONSUMER_KEY': '',
             'CONSUMER_SECRET': '',
             'ACCESS_TOKEN': '',
             'ACCESS_TOKEN_SECRET': '',
             'BEARER_TOKEN': ''
             }

query_params = {
    'query': '[word to search]',
    'tweet.fields': 'attachments,author_id,context_annotations,'
                    'conversation_id,created_at,entities,'
                    'geo,id,in_reply_to_user_id,lang,'
                    'possibly_sensitive,'
                    'public_metrics,referenced_tweets,'
                    'reply_settings,source,text,withheld',
    # 'start_time': '2021-03-30T12:00:01.000Z',  # sandbox date
    # 'end_time': '2021-03-31T23:59:59.000Z',  # sandbox date
    'start_time': '2006-03-22T00:00:01.000Z',  # production date
    'end_time': '2010-12-31T23:59:59.000Z',  # production date
    'expansions': 'geo.place_id,author_id',
    'user.fields': 'created_at,id,location,name,protected,public_metrics,username,verified,withheld',
    'place.fields': 'full_name,id,country,country_code,geo,name,'
                    'place_type',
    # 'max_results': 10,  # sandbox max 1
    # 'max_results': 100,  # sandbox max 2
    'max_results': 500,  # production max
    'next_token': None
}

# SANDBOX - recent tweets search
# search_url = "https://api.twitter.com/2/tweets/search/recent"

# PRODUCTION - full-archive search
search_url = "https://api.twitter.com/2/tweets/search/all"

# os environment
os.environ['BEARER_TOKEN'] = user_keys['BEARER_TOKEN']
META_LOCATION = './meta_files'
META_GEN_NAME = '/meta_'
META_SUFFIX = '.txt'

# JSON_LOCATION = './json_files'
# JSON_GEN_NAME = '/recents_unfriend_tweets_'
# JSON_SUFFIX = '.json'

JSON_LOCATION = './json_files'
JSON_GEN_NAME = '/all_unfriend_tweets_'
JSON_SUFFIX = '.json'


# tweeter authentication
def auth():
    """
    Authentication preparation.
    :return: the current bearer token to send to the twitter API.
    """
    return os.environ.get("BEARER_TOKEN")


def create_headers(bearer_token):
    """
    Create the GET request header.
    :param bearer_token: current bearer token.
    :return: dictionary with the request headers.
    """
    headers = {"Authorization": "Bearer {}".format(bearer_token)}
    return headers


def connect_to_endpoint(in_url, headers, params):
    """
    Establish a connection to the twitter API.
    :param in_url: the archive to search in.
    :param headers: GET request headers.
    :param params: search parameters of the query.
    :return: JSON formatted response with search results.
    """
    response = requests.request("GET", in_url, headers=headers,
                                params=params)
    # print(response.status_code)
    if response.status_code != 200:  # if the request didn't get an
        # answer
        print(response.status_code)
        print(response.text)
        return False

    return response.json()


def make_df(response):
    """
    Make a data frame object out of the JSON object
    :param response: a JSON formatted object of tweets.
    :return: a dataframe
    """
    df = pd.json_normalize(response)
    return df


def write_to_meta_file(meta_file, meta):
    meta_line = f'\n{str(meta["oldest_id"])},' \
                f'\n{str(meta["newest_id"])},' \
                f'\n{str(meta["next_token"])},\n' \
                f'{str(meta["result_count"])} tweets were added'
    meta_file.writelines(meta_line)


def tweets_counter_controller(current, addition):
    current += addition
    return current


def pretty_flatten(json_response):
    return expansions.flatten(json_response)


def harvest_tweets(file_number, tweets_file_path, meta_file_path,
                   next_token=None):
    """
    Pull tweets and and put them in a text file.
    :return: None
    """
    headers = create_headers(os.environ['BEARER_TOKEN'])
    if next_token is not None:
        query_params['next_token'] = next_token

    if not os.path.exists(tweets_file_path):
        json_response = connect_to_endpoint(search_url, headers,
                                            query_params)

    # if on sandbox mode comment all following lines in
    # harvest_tweets function
        with open(tweets_file_path, mode='a') as json_file:
            json_file.write(json.dumps(json_response))
            json_file.write("\n")

        # digest metadata about this run
        meta_file = open(meta_file_path, mode="a")
        meta_file.writelines(f"{meta_file_path}\n"
                             f"[Oldest,newest,next_token]\n")
        write_to_meta_file(meta_file, json_response["meta"])
        tweets_counter = tweets_counter_controller(0,
                                                   json_response["meta"][
                                                       "result_count"])
        next_token = json_response["meta"]["next_token"]
        round_counter = 1

    # in production change round_counter to 500
    while next_token and round_counter <= 499:
        sleep(1)
        next_token = json_response["meta"]["next_token"]
        query_params['next_token'] = json_response["meta"][
            "next_token"]
        json_response = connect_to_endpoint(search_url,
                                            headers,
                                            query_params)
        while json_response is False:
            sleep(120)
            json_response = connect_to_endpoint(search_url,
                                                headers,
                                                query_params)

        with open(tweets_file_path, mode='a') as json_file:
            json_file.write(json.dumps(json_response))
            json_file.write("\n")

        # digest metadata about this run
        write_to_meta_file(meta_file, json_response["meta"])
        tweets_counter = tweets_counter_controller(tweets_counter,
                                                   json_response[
                                                       "meta"][
                                                       "result_count"])
        # print(f'finished tweet harvest #{round_counter}')
        round_counter += 1

    json_file.close()
    meta_file.writelines(f"\nClosed json file ["
                         f"{tweets_file_path}].\n")
    meta_file.writelines(f"\n{tweets_counter} tweets were pulled "
                         f"into this file,\n"
                         f"{round_counter} calls were made to "
                         f"build this file.\n")
    meta_file.close()
    if not json_response["meta"]["next_token"]:
        return True, json_response["meta"]["next_token"]
    return False, json_response["meta"]["next_token"]


def main():
    bearer_token = auth()
    run_again = True
    files_counter = 0
    started = False

    while run_again:
        tweets_file_path = JSON_LOCATION + JSON_GEN_NAME \
                           + str(files_counter) \
                           + JSON_SUFFIX
        meta_file_path = META_LOCATION + META_GEN_NAME \
                         + str(files_counter) \
                         + META_SUFFIX

        if started:
            done, next_token_to_remember = harvest_tweets(
                files_counter,
                tweets_file_path,
                meta_file_path,
                next_token_to_remember)
        else:
            started = True
            done, next_token_to_remember = harvest_tweets(files_counter,
                                                    tweets_file_path,
                                                    meta_file_path)
        print(f'finished with file #{files_counter}')
        files_counter += 1

        # TODO: change round_counter to big number
        if files_counter >= 5:
            run_again = False  # sandbox turn off switch
        if done:
            run_again = False
        else:
            print("finished a file, now I sleep")
            sleep(960)  # 16 minutes


main()
