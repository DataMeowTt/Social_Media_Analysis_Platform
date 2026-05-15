SELECT
  top_brand AS top_brand,
  author_username AS author_username,
  LOG(10.0, SUM(avg_engagement_score) + 1) AS "log(10.0, sum(avg_engagement_score) + 1)",
  SUM(total_tweets) AS "sum(total_tweets)"
FROM (
  WITH author_stats AS (
    SELECT
      author_id,
      author_username,
      COUNT(*) AS total_tweets,
      ROUND(AVG(engagement_score), 4) AS avg_engagement_score
    FROM analytics.fact_tweets
    WHERE
      NOT engagement_score IS NULL
    GROUP BY
      author_id,
      author_username
  ), brand_mentions AS (
    SELECT
      author_id,
      brand_name,
      ROW_NUMBER() OVER (PARTITION BY author_id ORDER BY COUNT(*) DESC) AS rn
    FROM analytics.fact_tweets
    CROSS JOIN UNNEST(all_brands) AS t(brand_name)
    WHERE
      NOT all_brands IS NULL
    GROUP BY
      author_id,
      brand_name
  )
  SELECT
    a.author_username,
    a.total_tweets,
    a.avg_engagement_score,
    b.brand_name AS top_brand
  FROM author_stats AS a
  INNER JOIN brand_mentions AS b
    ON a.author_id = b.author_id AND b.rn = 1
  ORDER BY
    a.avg_engagement_score DESC
  LIMIT 10
) AS virtual_table
GROUP BY
  1,
  2