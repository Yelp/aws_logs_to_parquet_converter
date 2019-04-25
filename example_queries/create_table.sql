--- Run this to create a compatible table in AWS Athena.
--- After you've created the table, you'll need to add data to it.
--- Change the following:
---    Name of the table (first line below).
---       Warning! Backticks are essential if you have special characters like hyphens in your name.
---    Location of your Parquet files (last line in the script)
--- Warning! Athena snarfs your *personal* IAM keypair when executing queries.
---    Therefore, your logged in user *must* be able to write to this location.
---    Athena will specify an error if the specified bucket is not real.
---    Athena will *not* specify an error if the bucket is real, but you do not have
---       permissions to write to it. Obviously, this has never affected us in any way. :)

CREATE EXTERNAL TABLE `example-s3-access-logs-table`(
  `bucket_owner` string COMMENT '',
  `s3_bucket` string COMMENT '',
  `request_time` timestamp COMMENT '',
  `remote_ip` string COMMENT '',
  `requester` string COMMENT '',
  `request_id` string COMMENT '',
  `operation` string COMMENT '',
  `key` string COMMENT '',
  `request` string COMMENT '',
  `http_status` int COMMENT '',
  `error_code` string COMMENT '',
  `bytes_sent` bigint COMMENT '',
  `object_size` bigint COMMENT '',
  `total_time` bigint COMMENT '',
  `turn_around_time` bigint COMMENT '',
  `referrer` string COMMENT '',
  `user_agent` string COMMENT '',
  `version_id` string COMMENT '',
  `error_line` string COMMENT '')
PARTITIONED BY (
  `dt` string)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  's3://some-bucket-name-TODO-change-me/teams/metrics-data/s3_server_side_access_logs/example-s3-access-logs-table'
