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

################################################################################
# helper functions
################################################################################


def remove_nulls(s):
    r'''
    Postgres doesn't support strings with the null character \x00 in them, but twitter does.
    This helper function replaces the null characters with an escaped version so that they can be loaded into postgres.
    Technically, this means the data in postgres won't be an exact match of the data in twitter,
    and there is no way to get the original twitter data back from the data in postgres.

    The null character is extremely rarely used in real world text (approx. 1 in 1 billion tweets),
    and so this isn't too big of a deal.
    A more correct implementation, however, would be to *escape* the null characters rather than remove them.
    This isn't hard to do in python, but it is a bit of a pain to do with the JSON/COPY commands for the denormalized data.
    Since our goal is for the normalized/denormalized versions of the data to match exactly,
    we're not going to escape the strings for the normalized data.

    >>> remove_nulls('\x00')
    ''
    >>> remove_nulls('hello\x00 world')
    'hello world'
    '''
    if s is None:
        return None
    else:
        return s.replace('\x00','')


#def get_id_urls(url, connection):
#    '''
#    Given a url, return the corresponding id in the urls table.
#    If no row exists for the url, then one is inserted automatically.
#
#    NOTE:
#    This function cannot be tested with standard python testing tools because it interacts with the db.
#    '''
#    sql = sqlalchemy.sql.text('''
#    insert into urls 
#        (url)
#        values
#        (:url)
#    on conflict do nothing
#    returning id_urls
#    ;
#    ''')
#    res = connection.execute(sql,{'url':url}).first()
#
#    # when no conflict occurs, then the query above inserts a new row in the url table and returns id_urls in res[0];
#    # when a conflict occurs, then the query above does not insert or return anything;
#    # we need to run a select statement to put the already existing id_urls into res[0]
#    if res is None:
#        sql = sqlalchemy.sql.text('''
#        select id_urls 
#        from urls
#        where
#            url=:url
#        ''')
#        res = connection.execute(sql,{'url':url}).first()
#
#    id_urls = res[0]
#    return id_urls


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
        # skip tweet if it's already inserted
        sql=sqlalchemy.sql.text('''
            SELECT id_tweets 
            FROM tweets
            WHERE id_tweets = :id_tweets
            ''')
        res = connection.execute(sql,{
            'id_tweets':tweet['id'],
            })  
        if res.first() is not None:
            return


        ########################################
        # insert into the users table
        ########################################

        # create/update the user
        sql = sqlalchemy.sql.text('''
            INSERT INTO users (id_users, screen_name, name)
            VALUES (:id_users, :screen_name, :name)
            ON CONFLICT DO NOTHING
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

        ########################################
        # insert into the tweet_tags table
        ########################################

        try:
            hashtags = tweet['extended_tweet']['entities']['hashtags'] 
            cashtags = tweet['extended_tweet']['entities']['symbols'] 
        except KeyError:
            hashtags = tweet['entities']['hashtags']
            cashtags = tweet['entities']['symbols']

        tags = [ '#'+hashtag['text'] for hashtag in hashtags ] + [ '$'+cashtag['text'] for cashtag in cashtags ]

        for tag in tags:
            sql=sqlalchemy.sql.text('''
                INSERT INTO tweet_tags (id_tweets, tag)
                VALUES (:id_tweets, :tag)
                ON CONFLICT DO NOTHING
                ''')
            sql = sql.bindparams(id_tweets=tweet['id'], tag=remove_nulls(tag))
            connection.execute(sql)

################################################################################
# main functions
################################################################################

if __name__ == '__main__':
    
    # process command line args
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--db',required=True)
    parser.add_argument('--inputs',nargs='+',required=True)
    parser.add_argument('--print_every',type=int,default=1000)
    parser.add_argument('--max_tweets', type=int,default=-1)
    args = parser.parse_args()

    # create database connection
    engine = sqlalchemy.create_engine(args.db, connect_args={
        'application_name': 'load_tweets.py',
        })
    connection = engine.connect()

    tweets_inserted = 0
    stop = False
    # loop through the input file
    # NOTE:
    # we reverse sort the filenames because this results in fewer updates to the users table,
    # which prevents excessive dead tuples and autovacuums
    for filename in sorted(args.inputs, reverse=True):
        with zipfile.ZipFile(filename, 'r') as archive: 
            print(datetime.datetime.now(),filename)
            for subfilename in sorted(archive.namelist(), reverse=True):
                with io.TextIOWrapper(archive.open(subfilename)) as f:
                    for i,line in enumerate(f):
                        # load and insert the tweet
                        tweet = json.loads(line)
                        insert_tweet(connection,tweet)
                        tweets_inserted += 1
                        # print message
                        if i%args.print_every==0:
                            print(datetime.datetime.now(),filename,subfilename,'i=',i,'id=',tweet['id'])

                        if args.max_tweets != -1 and tweets_inserted >= args.max_tweets:
                            stop = True
                            break
                if stop:
                    break
        if stop:
            break
    print(tweets_inserted, "tweets inserted")
