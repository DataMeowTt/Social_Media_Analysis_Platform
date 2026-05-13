-- Tổng quan mức độ ảnh hưởng của top users theo brand
SELECT
  author_username AS author_username,
  top_brands AS top_brands,
  SUM(author_followers) AS follower,
  SUM(avg_engagement) AS "engagement score",
  SUM(viral_tweet_count) AS "number tweets viral"
FROM (
  SELECT
    a.author_username,
    a.author_followers,
    a.author_is_blue_verified,
    a.author_influence_ratio,
    p.total_tweets,
    ROUND(p.avg_engagement, 4) AS avg_engagement,
    p.viral_tweet_count,
    p.dominant_sentiment,
    a.top_brands
  FROM analytics.agg_author_perf AS p
  JOIN analytics.dim_authors AS a
    ON a.author_id = p.author_id
  WHERE
    a.is_bot_flag = FALSE AND NOT a.top_brands IS NULL
  ORDER BY
    p.avg_engagement DESC
  LIMIT 10
) AS virtual_table
GROUP BY
  1,
  2
ORDER BY
  "engagement score" DESC