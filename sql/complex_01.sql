/*
 * Calculates the hashtags that are commonly used with the hashtag #coronavirus
 */
SELECT lower(t1.tag) as tag,count(distinct t1.id_tweets) as count
FROM tweet_tags t1
INNER JOIN tweet_tags t2 ON t1.id_tweets = t2.id_tweets
WHERE
    lower(t2.tag)='#coronavirus'
GROUP BY (1)
ORDER BY count DESC,tag;


