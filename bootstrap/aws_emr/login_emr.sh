# Use the following command to SSH to the EMR master node.
# This is necessary to execute the spark-submit command.
aws emr ssh --cluster-id <cluster_id> --key-pair-file <path_to_key_file>
