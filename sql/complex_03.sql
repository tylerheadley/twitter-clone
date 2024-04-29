/*
 * Calculates how commonly hashtag #coronavirus is used in each country.
 */
SELECT 
    country_code,
    count(*) as count
FROM tweet_tags
INNER JOIN tweets ON tweet_tags.id_tweets = tweets.id_tweets
WHERE
    tag = '#coronavirus'
GROUP BY country_code
ORDER BY count DESC,country_code;
