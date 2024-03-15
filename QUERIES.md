

## Warehouse


Copy "timeline_statuses" table as "timeline_statuses_20230906", representing a preliminary collection run we did in 2023.

Copy tables into shared environment:

```sql
CREATE TABLE IF NOT EXISTS `tweet-research-shared.truth_2023.timeline_statuses_20230906` as (
    SELECT *
    FROM `tweet-collector-py.truth_2023_production.timeline_statuses_20230906`
    -- LIMIT 1000
    ORDER BY created_at DESC
)
```

```sql
-- make flat table row per status per tag
CREATE TABLE IF NOT EXISTS `tweet-research-shared.truth_2023.timeline_status_tags_flat_20230906` as (
    SELECT DISTINCT status_id, user_id, upper(tag) as tag
    FROM `tweet-collector-py.truth_2023_production.timeline_statuses_20230906`,
        UNNEST(tags) AS tag
    --WHERE  upper(username) = "REALDONALDTRUMP"
    --LIMIT 10
    ORDER BY user_id, status_id
)
```

```sql

-- DROP TABLE IF EXISTS `tweet-research-shared.truth_2023.user_details_20230906`;

CREATE TABLE IF NOT EXISTS `tweet-research-shared.truth_2023.user_details_20230906` as (

    SELECT
        t.user_id
        ,count(distinct upper(t.username)) as username_count
        ,string_agg(distinct upper(t.username), " | ") as screen_names

        , count(distinct t.status_id) as status_count
        ,count(distinct case when t.reply_status_id IS NOT NULL then t.status_id end) reply_count
        -- reply_user_id
        ,count(distinct case when t.mention_id IS NOT NULL then t.status_id end) mention_count
        -- mention_username

        , date(min(t.created_at)) as created_min
        , date(max(t.created_at)) as created_max

        ,date(min(t.collected_at)) as collected_min
        ,date(max(t.collected_at)) as collected_max

        ,count(distinct(upper(t.lang))) as langs_count
        ,string_agg(distinct upper(t.lang), " | ") as langs

        ,count(distinct t.group_id) as groups_count
        --,array_agg(distinct t.group_id IGNORE NULLS) as group_ids
        ,string_agg(distinct upper(t.group_slug) , " | ") as group_slugs

        ,count(distinct t.media_url) as media_count
        ,string_agg(distinct t.media_type , " | " ORDER BY t.media_type) as media_types

        --,array_concat_agg(tags) as tags
        -- join to flat table instead:
        ,count(DISTINCT tg.tag) as tags_count
        ,string_agg(DISTINCT tg.tag, " | ") as tags

    FROM `tweet-collector-py.truth_2023_production.timeline_statuses_20230906` t
    LEFT JOIN `tweet-research-shared.truth_2023.timeline_status_tags_flat_20230906`  tg ON tg.status_id = t.status_id
    --WHERE upper(username) = "REALDONALDTRUMP"
    GROUP BY 1
    ORDER BY 2 DESC
    --LIMIT 1
)
```


```sql

--CREATE TABLE `tweet-research-shared.truth_2023.timeline_statuses` as (
--  SELECT * FROM `tweet-collector-py.truth_2023_production.timeline_statuses`
--)
```

## Analysis

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

Languages:


```sql
-- SELECT status_id, count(distinct lang) as lang_count
-- FROM `tweet-collector-py.truth_2023_production.timeline_statuses_20230906` t
-- GROUP BY 1
-- ORDER BY 2 DESC
-- HAVING lang_count > 1
-- each status is in a single language. good.

SELECT case when  lang="" then null else upper(lang) end lang, -- there are some empty strings, coalescing them as null
    count(distinct user_id) as user_count
    , count(distinct status_id) as status_count
FROM `tweet-collector-py.truth_2023_production.timeline_statuses_20230906`
GROUP BY 1
ORDER BY 2 desc
LIMIT 10
```


lang	| user_count	| status_count
--- | --- | ---
EN	| 472	| 4,272,301
NULL 	| 470	| 3,551,877
NO	| 390	| 9,852
FY	| 386	| 13,355
LB	| 375	| 6,534
NL	| 375	| 6,986
DE	| 373	| 7,277
IT	| 355	| 4,450
AF	| 352	| 5,453
DA	| 347	| 5,255
CA	| 344	| 4,807
FR	| 337	| 4,183
LA	| 332	| 2,582
JV	| 330	| 3,077
PT	| 328	| 4,200


Top tags:

```sql
-- top tags (the ones used by the most users)
SELECT 
  RTRIM(LTRIM(tag)) as tag
  ,count(distinct user_id) as user_count -- TRUTH with 242, MAGA with 225, 
  ,count(distinct status_id) as status_count
FROM `tweet-research-shared.truth_2023.timeline_status_tags_flat_20230906`
GROUP BY tag
ORDER BY user_count desc
LIMIT 250
```

Mentions:

```sql
SELECT 
  count(distinct status_id) as status_count  -- how many posts mention this user?
  , count(distinct user_id) as user_count -- how many users mention this user?
FROM `tweet-research-shared.truth_2023.timeline_statuses_20230906` 
WHERE upper(mention_username) = upper('realdonaldtrump')
```

