CREATE EXTENSION rum;

\set ON_ERROR_STOP on

BEGIN;

CREATE TABLE users (
    id_users BIGSERIAL PRIMARY KEY,
    screen_name TEXT UNIQUE NOT NULL,
    name TEXT NOT NULL,
    password TEXT
);
CREATE INDEX idx_username_password ON users(screen_name, password);

CREATE TABLE tweets (
    id_tweets BIGSERIAL PRIMARY KEY,
    id_users BIGINT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    text TEXT NOT NULL,
    lang TEXT,
    FOREIGN KEY (id_users) REFERENCES users(id_users)
);
CREATE INDEX idx_tweets_id_users ON tweets(id_users); -- not sure if this helps the join
CREATE INDEX idx_created_at on tweets(created_at);
CREATE INDEX idx_tweets_fts ON tweets USING rum(to_tsvector('english', text));

CREATE TABLE tweet_tags (
    id_tweets BIGINT,
    tag TEXT,
    PRIMARY KEY (id_tweets, tag),
    FOREIGN KEY (id_tweets) REFERENCES tweets(id_tweets)
);
CREATE INDEX idx_tweet_tags_tag_id_tweets ON tweet_tags (tag, id_tweets);

CREATE MATERIALIZED VIEW tweet_tags_counts AS
SELECT tag, COUNT(id_tweets) AS count_tags
FROM tweet_tags
GROUP BY tag;

CREATE INDEX idx_tweet_tags_counts ON tweet_tags_counts (count_tags desc, tag);

COMMIT;
