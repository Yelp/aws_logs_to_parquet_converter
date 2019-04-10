# Introduction

AWS S3 Server Side Logging allows owners of S3 bucket to analyze the access 
requests to the S3 buckets. However, the access logs is in a row format and 
uncompressed. To effectively analyze the logs, you want to compact the raw
logs into a columnar format. With columnar format, many query engines such
as SparkSQL and Prestodb can retrieve data via SQL syntax effectively. 

This project contains a Pyspark script that can be run inside Spark cluster.
The script will compact each day's data into snappy-compressed Parquet files.

Yelp uses this approach to effectively analyze large amount of our S3 server
access logs.

# Bootstraping (if you do not have a spark environment)

We recommend you consult AWS documentation spin up a emphermal AWS EMR cluster
to run Spark program. inside the bootstrap/aws_emr folder, there is a series of
script that get you started.

1. Pick an existing s3 bucket and modify the upload_* script to upload both the 
bootstrap script and AWS logs to conversion script.

2. Use the same s3 bucket and modify the launch_emr_cluster.sh script. Then use
the launch_emr_cluster script to bootstrap the cluster.

Currently, the script launches a 1 m3.xlarge as EMR Master and 2 m2.xlarge EMR
Slaves. You account will incur chages from EMR. After you are done, you should
shutdown your EMR cluster because the actual data are stored in S3 anyway.

3. Once the EMR cluster is bootstraped, note the cluster id and appropriate key
pair and update the login_emr.sh script to login the master node.

4. download the conversion script you uploaded from s3 and execute the follow
section by running spark-submit.

Note: At Yelp, we mostly use our Platform-as-a-service PaaSTA to execute on our
Spark programs. So we package docker image to deploy and run our spark programs.

# Bootstraping (if you have a spark environment)

Once you deploy this script into your spark cluster, you should use
spark-submit to run the script.

spark-submit scripts/oss_s3_server_side_logging_compacter.py  \
        --aws-config <path to JSON file with your credentials> \
        --min-date 2019-02-10 \
        --max-date 2019-02-11 \
        --source-access-log-bucket <YOUR ACCESS LOG BUCKET> \
        --source-bucket <YOUR BUCKET that originates the logs> \
        --destination-log-bucket <S3 bucket that will hold the compacted log> \
        --destination-log-prefix <S3 prefix to use to store the lgos> \
        --num-output-files 15

An example of the aws-config

$ cat something.key

{
        "accessKeyId": "YOUR_AWS_ACCESS_KEY",
        "secretAccessKey": "YOUR_AWS_SECRET_KEY",
        "region": "us-west-2"
}

Note: the keys in clear at the time of execution is bad... However, depends
on your deployment setup, you can eliminate it based on AWS EC2 Instance
profile or role. In the future, we like to abstract away the s3 client
module usage to plumb in your choice of authentication to s3.

# High level Dependency

* Only works with AWS S3 Server Side Logging
* Tested only with Python 3.6
* Tested only with Spark 2.3.2

# Development

If you like to submit pull-request on this repository, we try to automate 
some tasks, please use the following procedure.

$ make venv
$ make requirements
$ . bin/venv/activate
$ pre-commit run -a  # we shouldn't need to scan all files, but i am still
debugging a setup issue

# Alternatives

1. AWS Labs Athena Glue Service docs (https://github.com/awslabs/athena-glue-service-logs)
"Warning These jobs can incur a significant amount of cost depending on how much data you have in your source location!"
