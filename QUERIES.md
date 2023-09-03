

Update downstream views:

```sql

CREATE TABLE `tweet-research-shared.truth_2023.timeline_statuses` as (
  SELECT * FROM `tweet-collector-py.truth_2023_production.timeline_statuses`
)
```
