CREATE OR REFRESH LIVE TABLE gold_layer.trending_posts
COMMENT "Top 10 trending posts in the last week "
AS
SELECT 
    c.post_id,
    p.title,
    c.comments,
    c.score,
    c.author,
    c.subreddit,
    c.created_utc,
    ROW_NUMBER() OVER (ORDER BY c.score DESC) as trending_rank
FROM silver_layer.comments_enrch as c join silver_layer.posts_enrch as p
on c.post_id = p.post_id
WHERE c.created_utc >= CURRENT_TIMESTAMP() - INTERVAL 7 DAYS
QUALIFY trending_rank <= 10;