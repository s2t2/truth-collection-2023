

Update downstream views:

```sql

CREATE TABLE `tweet-research-shared.truth_2023.timeline_statuses` as (
  SELECT * FROM `tweet-collector-py.truth_2023_production.timeline_statuses`
)
```

Analysis queries:

```sql
SELECT status_id, content, created_at
FROM `tweet-collector-py.truth_2023_production.timeline_statuses`
WHERE upper(username) = 'BELANNF'
ORDER BY created_at DESC
```

```sql
SELECT count(distinct status_id) as status_count
FROM `tweet-collector-py.truth_2023_production.timeline_statuses` -- 7,994,046
WHERE reply_status_id IS NULL -- 4635015
```

```sql
-- whoa massive amounts of posts per user
SELECT upper(username), count(distinct status_id) as status_count
FROM `tweet-collector-py.truth_2023_production.timeline_statuses` -- 7,994,046
WHERE reply_status_id IS NULL -- 4635015
GROUP BY 1
ORDER BY 2 DESC
LIMIT 100


```

Media in posts most replied to:

```sql
SELECT  -- reply_status_id mention_id
    t.reply_status_id
    ,r.media_type
    ,r.media_url
    ,count(distinct t.status_id) as status_count
FROM `tweet-collector-py.truth_2023_production.timeline_statuses` t
LEFT JOIN (
    SELECT distinct status_id, media_type, media_url
    FROM `tweet-collector-py.truth_2023_production.timeline_statuses`
) r on t.reply_status_id = r.status_id
GROUP BY t.reply_status_id, 2, 3
ORDER BY 4 DESC
-- 110947643204739466 : 233
-- 108211822140637685 : 204
-- 110963528175482652 : 129

```
