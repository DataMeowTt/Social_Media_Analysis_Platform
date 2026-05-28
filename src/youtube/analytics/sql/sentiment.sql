SELECT
  brand_mentioned,
  sentiment,
  COUNT(*) AS total_comments
FROM fct_comment_sentiment
GROUP BY
  brand_mentioned,
  sentiment
ORDER BY
  brand_mentioned,
  total_comments DESC
