--- Run this to add (append) partitions to an existing external table.
--- It may be faster - or easier to iterate via - than MSCK REPAIR TABLE (`load_all_partitions.sql`).
--- Warning! Backticks are essential if you have special characters like hyphens in your name.
--- Change the TABLE to match the one you created.
--- Set the datetime (dt) to the date you've converted in *both* places.
--- Update the S3 path accordingly.

ALTER TABLE `example-s3-access-logs-table`
    ADD PARTITION (dt='YYYY-MM-DD')
    LOCATION 's3://some-bucket-name-TODO-change-me/teams/metrics-data/s3_server_side_access_logs/example-s3-access-logs-table/dt=YYYY-MM-DD'
