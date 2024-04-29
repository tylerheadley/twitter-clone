/*
 * Calculates how commonly a hashtag is used each day.
 */
SELECT
    date_trunc('day',created_at) as day,
    count(*) as count
FROM tweet_tags
INNER JOIN tweets ON tweet_tags.id_tweets = tweets.id_tweets
WHERE
    tag = '#coronavirus'
GROUP BY day
ORDER BY day DESC;
