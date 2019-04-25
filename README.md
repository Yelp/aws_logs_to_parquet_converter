# Introduction

AWS S3 Server Side Logging allows owners of S3 buckets to analyze access
requests made to the S3 buckets. However, S3 access logs are made available in
a raw format that is both uncompressed and splayed across an unpredictable
number of objects. To effectively analyze the logs, we compact the raw logs
provided by Amazon into a columnar format. By using a columnar format, many
query engines such as Spark SQL and PrestoDB can execute queries and retrieve
data via SQL syntax.

This project contains a PySpark script that can be run inside a Spark cluster.
The script will compact each day's data into Snappy-compressed Parquet files.

Yelp uses this approach to effectively and efficiently analyze our S3 server
access logs. We use this tool to successfully query up to ten years of S3
access logs describing tens of petabytes of S3 objects.

# Bootstrapping (if you do not have a Spark environment)

We recommend you consult AWS's documentation to spin up an ephemeral AWS EMR
cluster.  You can then use that to run our Spark program. Inside the
`bootstrap/aws_emr` folder, you will find a series of scripts that should get
you started.

1. Pick an existing S3 bucket and modify the `upload_*` script to upload both
the bootstrap script and AWS logs to the conversion script.

2. Use the same S3 bucket and modify the `launch_emr_cluster.sh` script. Then,
use the `launch_emr_cluster` script to bootstrap the cluster.

Currently, the script launches one `m3.xlarge` node as an EMR master and two
`m2.xlarge` nodes as EMR slaves. You account will incur charges from EMR. After
you are done, you should shut down your EMR cluster. The actual data you have
converted is stored in S3 anyway!

3. Once your EMR cluster is bootstrapped, make a note of the cluster's ID and
key pair. Update the `login_emr.sh` script to log in to the master node.

4. Download the conversion script you uploaded from S3 and execute the
following section by running the `spark-submit` command.

_Note: At Yelp, we mostly use our platform-as-a-service (PaaSTA) to execute any
Spark programs we wish to run. As a result, we package Docker images to deploy
and run our Spark programs._

# Bootstrapping (if you have a Spark environment)

Once you deploy this script into your Spark cluster, you should use
`spark-submit` to run it.

```
spark-submit scripts/oss_s3_server_side_logging_compacter.py  \
        --aws-config <path to JSON file with your credentials> \
        --min-date 2019-02-10 \
        --max-date 2019-02-11 \
        --source-access-log-bucket <YOUR ACCESS LOG BUCKET> \
        --source-bucket <YOUR BUCKET that originates the logs> \
        --destination-log-bucket <S3 bucket that will hold the compacted log> \
        --destination-log-prefix <S3 prefix to use to store the compacted log> \
        --num-output-files 15
```

An example of the `aws-config` file:

```
$ cat something.key

{
        "accessKeyId": "YOUR_AWS_ACCESS_KEY",
        "secretAccessKey": "YOUR_AWS_SECRET_KEY",
        "region": "us-west-2"
}
```

_Note: Cleartext keys at execution time are not ideal from a security
perspective. However, depending on your deployment setup, you can eliminate
them by utilizing an AWS EC2 Instance profile and/or assuming a role. In the
future, we would like to abstract away the S3 client module usage so that we
each can plug in our AWS authentication mode of choice._


# High-level Dependencies

* Only compatible with AWS S3 Server Side Logging
* Tested only with Python 3.6
* Tested only with Spark 2.3.2

# Development

If you would like to submit a pull request to this repository, please use the
following procedure:

```bash
$ make venv
$ make requirements
$ . bin/venv/activate
$ pre-commit run -a
```

# Alternatives

1. AWS Labs Athena Glue Service Docs
(https://github.com/awslabs/athena-glue-service-logs) _Warning: These jobs can
incur a significant amount of cost depending on how much data you have in your
source location!_
