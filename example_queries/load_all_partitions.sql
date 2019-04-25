--- This will load all partitions into the external table according to its configuration.
--- It may take a while, depending on how many tables there are to load.
--- Note the backticks - they are essential for the same reason as in the previous query.
--- If you want to load a subset of partitions, please see `load_partition.sql` for an example.

MSCK REPAIR TABLE `example-s3-access-logs-table`
