/*
 * Selects the text of tweets that are written in English and contain the hashtag #coronavirus 
 */
SELECT
    tweets.id_tweets,
    length(text)
FROM tweet_tags
INNER JOIN tweets ON tweet_tags.id_tweets = tweets.id_tweets
WHERE
    lang = 'en' AND
    tag = '#coronavirus' 
ORDER BY tweets.id_tweets;
