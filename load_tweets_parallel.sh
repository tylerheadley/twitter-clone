#!/bin/sh

echo '================================================================================'
echo 'load tweets postgres'
echo '================================================================================'

if [ "$1" = "--local_tweets" ]; then
    files=$(find data/*)
    
    filecount=$(find data/* | wc -l)

    tweets_per_file=$(($2 / $filecount))

    time parallel python3 load_tweets.py --db=postgresql://postgres:pass@localhost:12346 --inputs={} --max_tweets=$tweets_per_file ::: $files

    echo "TOTAL: $(($tweets_per_file * $filecount)) tweets inserted"
elif [ "$1" = "--random_tweets" ]; then
    processes=$(seq 10)

    time parallel python3 load_random_tweets.py --db=postgresql://postgres:pass@localhost:12346 --process_num={} --num_tweets=$2 --print_every=100 ::: $processes
else
    echo "Usage: ./$0 --[data source] --[number of tweets (optional)]"
    echo "data source options: "
    echo "\t\"--lambda_server_tweets\" (for use on the lambda server only; ~1.1 billion tweets available)"
    echo "\t\"--local_tweets\" (accesses 100000 tweets stored locally from January 1-10, 2021)"
    echo "\t\"--random_tweets\" (generates random strings of words; number of tweets required)"
fi
