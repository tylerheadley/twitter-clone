--SELECT count(*) FROM tweet_mentions;
SELECT count(*) FROM (SELECT DISTINCT * FROM tweet_mentions)t;
