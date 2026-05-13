-- Tổng quan sentiment theo brand (7 hoặc 30 ngày gần nhất)
SELECT
    brand_name,
    SUM(total_mentions)                                                          AS total_mentions,
    SUM(positive_count)                                                          AS positive_count,
    SUM(neutral_count)                                                           AS neutral_count,
    SUM(negative_count)                                                          AS negative_count,
    ROUND(100.0 * SUM(positive_count) / NULLIF(SUM(total_mentions), 0), 2)      AS positive_pct,
    ROUND(100.0 * SUM(negative_count) / NULLIF(SUM(total_mentions), 0), 2)      AS negative_pct,
    ROUND(AVG(avg_sentiment_score), 4)                                           AS avg_sentiment,
    ROUND(AVG(avg_engagement_score), 4)                                          AS avg_engagement,
    SUM(viral_count)                                                             AS total_viral
FROM analytics.agg_brand_daily
WHERE date >= CURRENT_DATE - INTERVAL '30' DAY
-- WHERE date >= CURRENT_DATE - INTERVAL '7' DAY
GROUP BY brand_name
ORDER BY total_mentions DESC;
