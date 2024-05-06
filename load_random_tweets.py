#!/usr/bin/python3

# imports
import sqlalchemy
from sqlalchemy.sql import text
import os
import datetime
import zipfile
import io
import json
import logging
logging.basicConfig(
    level=logging.DEBUG,
    format=f'%(asctime)s.%(msecs)03d - {os.getpid()} - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
)
import sys
import random
import nltk
from nltk.corpus import words
from datetime import datetime, timedelta

nltk.download('words')
# Get the list of English words from the NLTK corpus
english_words = words.words()

def generate_random_tweet_text():
    # Choose random words to construct the tweet
    num_words = random.randint(5, 12)  # Random number of words in the tweet
    tweet_words = [random.choice(english_words) for _ in range(num_words)]

    # Construct the tweet by joining the words
    tweet = ' '.join(tweet_words)

    return tweet


def generate_random_hashtag():
    # Select two random words from the NLTK corpus
    hashtag = random.choice(english_words)

    return "#" + hashtag


def generate_random_datetime():
    # Generate a random year before 2000
    year = random.randint(1, 1999)
    
    # Generate a random month and day
    month = random.randint(1, 12)
    day = random.randint(1, 28)  # Assume 28 days for simplicity
    
    # Generate a random hour, minute, and second
    hour = random.randint(0, 23)
    minute = random.randint(0, 59)
    second = random.randint(0, 59)
    
    # Create a datetime object with the generated values
    random_datetime = datetime(year, month, day, hour, minute, second)
    
    return random_datetime


def insert_tweet(connection,tweet):
    '''
    Insert the tweet into the database.

    Args:
        connection: a sqlalchemy connection to the postgresql db
        tweet: a dictionary representing the json tweet object

    NOTE:
    This function cannot be tested with standard python testing tools because it interacts with the db.
    '''
    # insert tweet within a transaction;
    # this ensures that a tweet does not get "partially" loaded
    with connection.begin() as trans:
        

        ########################################
        # insert into the users table
        ########################################

        # create the user
        sql = sqlalchemy.sql.text(
            '''
            INSERT INTO users (id_users, screen_name, name) 
            VALUES (:id_users, :screen_name, :name)
            ''')

        sql = sql.bindparams(id_users=tweet['user']['id'],
                             screen_name=tweet['user']['screen_name'],
                             name=tweet['user']['name'])
        connection.execute(sql)

        ########################################
        # insert into the tweets table
        ########################################

        # insert the tweet
        sql = sqlalchemy.sql.text(
            '''
            INSERT INTO tweets (id_tweets, id_users, created_at, text, lang)
            VALUES (:id_tweets, :id_users, :created_at, :text, :lang)
            ''')

        sql= sql.bindparams(
            id_tweets=tweet['id'],
            id_users=tweet['user']['id'],
            created_at=tweet['created_at'],
            text=tweet['text'],
            lang=tweet['lang']
        )

        connection.execute(sql)

        # insert hashtags
        
        for hashtag in tweet['entities']['hashtags']:

            sql = sqlalchemy.sql.text(
                '''
                INSERT INTO tweet_tags (id_tweets, tag)
                VALUES (:id_tweets, :tag)
                ''')

            sql = sql.bindparams(
                id_tweets=tweet['id'],
                tag=hashtag
            )

            connection.execute(sql)


################################################################################
# main functions
################################################################################

if __name__ == '__main__':
    
    # process command line args
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--db',required=True)
    parser.add_argument('--process_num', type=int,required=True)
    parser.add_argument('--num_tweets', type=int,required=True)
    parser.add_argument('--print_every',type=int,default=1000)
    args = parser.parse_args()

    # create database connection
    engine = sqlalchemy.create_engine(args.db, connect_args={
        'application_name': 'load_random_tweets.py',
        })
    connection = engine.connect()

    start_id = (args.process_num-1)*(args.num_tweets//10)
    stop_id = args.process_num*(args.num_tweets//10)

    for i in range(start_id, stop_id):

        hashtags = [generate_random_hashtag() for _ in range(random.randint(1, 4))]

        tweet = { 
            "id": i,
            "created_at": generate_random_datetime(),
            "text": generate_random_tweet_text() + " " + " ".join(hashtags),
            "user": {
                "id": i,
                "name": "Fake User " + str(i),
                "screen_name": "fake-user-" + str(i)
            },  
            "entities": {
                "hashtags": list(set(hashtags))
            },   
            "lang": 'en'
        }   

        insert_tweet(connection,tweet)
    
    sql = sqlalchemy.sql.text(
        ''' 
        SELECT SETVAL('users_id_users_seq', (SELECT MAX(id_users) FROM users));
        SELECT SETVAL('tweets_id_tweets_seq', (SELECT MAX(id_tweets) FROM tweets));
        REFRESH MATERIALIZED VIEW tweet_tags_counts;
        ''')

    connection.execute(sql)
    
    connection.close()

