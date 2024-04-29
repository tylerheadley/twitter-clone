--SELECT count(*) FROM tweet_tags;
SELECT count(*) FROM (SELECT DISTINCT * FROM tweet_tags)t;

