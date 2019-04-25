--- This is the "Days Apart" analysis as described in our 2019 Spark+AI Summit talk.
--- It will report a summary of all S3 GET operations, grouped by the "requester" and "log_name",
--- that occurred where the time between POST and GET exceeded "days_apart".
---
--- Things you may/will need to modify to match your org's needs:
---     log_name: Our regexp assumes a path name like "logs/RELEVANT_NAME_HERE/YYYY/MM/DD/filename.tgz"
---     requester: Our query assumes you want to lump all assumed instance profiles,
---         that accessed a given log_name on a single day, together. (You probably want this as is.)
---     FROM: This matches the table creation name. Warning! Double quotes, not backticks here.
---     days_apart: We iteratively tune this number to try out different possible results.
---
--- Feel free to improve on this query - or write new ones! Make a pull request if you do. :)

WITH tmp_workspace AS (
    SELECT
       regexp_replace(requester, '/i-.*') AS requester,
       regexp_extract(key, 'logs/([^/]*)/.*', 1) AS log_name,
       date_parse(array_join(regexp_extract_all(key, '/(\d+)', 1), '-'), '%Y-%m-%d') AS dt_written,
       date_trunc('day', request_time) AS dt_read,

       date_diff('day',
                 date_parse(array_join(regexp_extract_all(key, '/(\d+)', 1), '-'), '%Y-%m-%d'),
                 date_trunc('day', request_time)
                ) AS days_apart,
       bytes_sent
    FROM "example-s3-access-logs-table"
    WHERE
        operation = 'REST.GET.OBJECT'
        AND http_status < 300
)
SELECT
    requester,
    log_name,
    count(*) AS access_count,
    sum(bytes_sent) AS total_bytes
FROM tmp_workspace WHERE
   days_apart > 400
GROUP BY 1, 2
ORDER BY access_count DESC
