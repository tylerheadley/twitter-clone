#!/bin/sh

files=$(find data/*)

echo '================================================================================'
echo 'load tweets postgres'
echo '================================================================================'
time parallel python3 load_tweets.py --db=postgresql://postgres:pass@localhost:12346 --inputs={} ::: $files
