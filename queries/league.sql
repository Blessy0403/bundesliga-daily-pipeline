SELECT
    CASE WHEN e.entity_type = 'team' THEN e.name ELSE (e.tags->>0) END AS team_name,
    e.id AS entity_id,
    e.name AS entity_name,
    e.entity_type,
    COUNT(DISTINCT ts.id) AS total_mentions,
    COUNT(DISTINCT CASE WHEN ts.sentiment = 'positive' THEN ts.id END) AS positive_mentions,
    COUNT(DISTINCT CASE WHEN ts.sentiment = 'negative' THEN ts.id END) AS negative_mentions,
    COUNT(DISTINCT CASE WHEN ts.sentiment = 'neutral' THEN ts.id END) AS neutral_mentions,
    AVG(ts.confidence) AS avg_confidence,
    COALESCE(SUM(COALESCE(vt.retweet_count, 0)), 0) AS total_retweets,
    COALESCE(SUM(COALESCE(vt.like_count, 0)), 0) AS total_likes,
    COALESCE(SUM(COALESCE(vt.reply_count, 0)), 0) AS total_replies,
    COUNT(DISTINCT vt.id) AS tweet_count
FROM entities e
INNER JOIN tweet_about_entities tae ON tae.entity_id = e.id
INNER JOIN twitter_sentiments ts ON ts.tweet_about_entity_id = tae.id
INNER JOIN vw_tweets vt ON vt.id = tae.tweet_id
WHERE e.ingestion_active = true
  AND vt.tweet_date >= %s
  AND vt.tweet_date < %s
  AND (e.entity_type = 'team' OR e.tags IS NOT NULL)
  AND (
        e.tags::text LIKE %s
     OR e.tags::text LIKE %s
  )
GROUP BY (CASE WHEN e.entity_type = 'team' THEN e.name ELSE (e.tags->>0) END), e.id, e.name, e.entity_type
ORDER BY team_name, total_mentions DESC;