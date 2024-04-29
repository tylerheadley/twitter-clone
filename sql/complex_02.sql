/*
 * Like the query in 01.sql that calculates the hashtags that are commonly used with the hashtag #coronavirus,
 * but also returns the total number of times each hashtag is used.
 */
WITH tweet_tags_distinct AS (
    SELECT DISTINCT * FROM tweet_tags
)
SELECT t1.tag, shared_count, total_count
FROM (
    SELECT lower(t1.tag) as tag,count(*) as shared_count
    FROM tweet_tags_distinct t1
    INNER JOIN tweet_tags_distinct t2 ON t1.id_tweets = t2.id_tweets
    WHERE
        lower(t2.tag)='#coronavirus'
    GROUP BY (1)
    LIMIT 100
) t1
INNER JOIN (
    SELECT 
        lower(tag) as tag,
        count(*) as total_count
        FROM tweet_tags_distinct
        GROUP BY (1)
    ) t3 ON t1.tag = t3.tag
ORDER BY shared_count DESC,tag
;
