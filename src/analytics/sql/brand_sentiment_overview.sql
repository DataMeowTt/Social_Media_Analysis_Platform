-- Tổng quan sentiment theo brand (30 ngày gần nhất)
SELECT
    primary_brand,
    COUNT(*)                                                                             AS total_mentions,

    -- Sentiment counts
    SUM(CASE WHEN sentiment_label = 'positive' THEN 1 ELSE 0 END)                       AS positive_count,
    SUM(CASE WHEN sentiment_label = 'neutral'  THEN 1 ELSE 0 END)                       AS neutral_count,
    SUM(CASE WHEN sentiment_label = 'negative' THEN 1 ELSE 0 END)                       AS negative_count,

    -- Tỉ lệ sentiment trên tổng sentiment của chính brand đó
    ROUND(
        SUM(CASE WHEN sentiment_label = 'positive' THEN 1 ELSE 0 END) * 100.0
        / NULLIF(SUM(CASE WHEN sentiment_label IN ('positive','neutral','negative') THEN 1 ELSE 0 END), 0),
    2) AS positive_pct,
    ROUND(
        SUM(CASE WHEN sentiment_label = 'neutral' THEN 1 ELSE 0 END) * 100.0
        / NULLIF(SUM(CASE WHEN sentiment_label IN ('positive','neutral','negative') THEN 1 ELSE 0 END), 0),
    2) AS neutral_pct,
    ROUND(
        SUM(CASE WHEN sentiment_label = 'negative' THEN 1 ELSE 0 END) * 100.0
        / NULLIF(SUM(CASE WHEN sentiment_label IN ('positive','neutral','negative') THEN 1 ELSE 0 END), 0),
    2) AS negative_pct,

    -- Bot ratio
    SUM(CASE WHEN is_bot = TRUE THEN 1 ELSE 0 END)                                      AS bot_count,
    ROUND(
        SUM(CASE WHEN is_bot = TRUE THEN 1 ELSE 0 END) * 100.0 / NULLIF(COUNT(*), 0),
    2) AS bot_pct

FROM analytics.fact_tweets
WHERE date >= CURRENT_DATE - INTERVAL '30' DAY
  AND primary_brand IS NOT NULL
GROUP BY primary_brand
ORDER BY total_mentions DESC
