#!/bin/env python
import argparse
import datetime
import json
import re
from urllib.parse import urlparse

import boto3
from dateutil import parser
from pyspark.sql import Row
from pyspark.sql import SparkSession
from pyspark.sql.functions import to_timestamp
from pyspark.sql.types import IntegerType
from pyspark.sql.types import LongType
from pyspark.sql.types import StringType
from pyspark.sql.types import StructField
from pyspark.sql.types import StructType

# From https://aws.amazon.com/premiumsupport/knowledge-center/analyze-logs-athena/
# See Also https://docs.aws.amazon.com/AmazonS3/latest/dev/LogFormat.html
S3_ACCESS_LOG_PATTERN = re.compile(r'(?P<owner>\S+) (?P<bucket>\S+) (?P<time>\[[^]]*\]) (?P<ip>\S+) ' +
                                   r'(?P<requester>\S+) (?P<reqid>\S+) (?P<operation>\S+) (?P<key>\S+) ' +
                                   r'(?P<request>"[^"]*"|-) (?P<status>\S+) (?P<error>\S+) (?P<bytes>\S+) ' +
                                   r'(?P<size>\S+) (?P<totaltime>\S+) (?P<turnaround>\S+) (?P<referrer>"[^"]*"|-) ' +
                                   r'(?P<useragent>"[^"]*"|-) (?P<version>\S)')


def get_aws_key_and_secret(aws_creds_file_path):
    """ Given a filename containing AWS credentials (see README.md),
        return a 2-tuple (access key, secret key).
    """
    with open(aws_creds_file_path, 'r') as f:
        creds_dict = json.load(f)
    return creds_dict['accessKeyId'], creds_dict['secretAccessKey']


def parse_s3_access_time(s):
    try:
        s = s[s.find('[') + 1:s.find(' ')]
        return parser.parse(s.replace(':', ' ', 1))
    except Exception:
        return None


def parse_apache_log_line(logline):
    """ Given a log line, return a row containing the Apache Access Log info. """
    match = S3_ACCESS_LOG_PATTERN.search(logline)
    if match is None:
        return Row(
            bucket_owner=None,
            s3_bucket=None,
            request_time_string=None,
            remote_ip=None,
            requester=None,
            request_id=None,
            operation=None,
            key=None,
            request=None,
            http_status=None,
            error_code=None,
            bytes_sent=None,
            object_size=None,
            total_time=None,
            turn_around_time=None,
            referrer=None,
            user_agent=None,
            version_id=None,
            error_line=logline
        )
    return Row(
        bucket_owner=match.group('owner'),
        s3_bucket=match.group('bucket'),
        request_time_string=parse_s3_access_time(
            match.group('time')).isoformat(),
        remote_ip=match.group('ip'),
        requester=match.group('requester'),
        request_id=match.group('reqid'),
        operation=match.group('operation'),
        key=None if match.group('key') == '-' else match.group('key'),
        request=match.group('request'),
        http_status=None if match.group(
            'status') == '-' else int(match.group('status')),
        error_code=None if match.group(
            'error') == '-' else match.group('error'),
        bytes_sent=None if match.group(
            'bytes') == '-' else int(match.group('bytes')),
        object_size=None if match.group(
            'size') == '-' else int(match.group('size')),
        total_time=None if match.group(
            'totaltime') == '-' else int(match.group('totaltime')),
        turn_around_time=None if match.group(
            'turnaround') == '-' else int(match.group('turnaround')),
        referrer=None if match.group(
            'referrer') == '"-"' else match.group('referrer'),
        user_agent=None if match.group(
            'useragent') == '"-"' else match.group('useragent'),
        version_id=None if match.group(
            'version') == '-' else match.group('version'),
        error_line=None,
    )


S3_ACCESS_LOG_OUTPUT_SCHEMA = StructType(
    [
        StructField("bucket_owner", StringType(), True),
        StructField("s3_bucket", StringType(), True),
        StructField("request_time_string", StringType(), True),
        StructField("remote_ip", StringType(), True),
        StructField("requester", StringType(), True),
        StructField("request_id", StringType(), True),
        StructField("operation", StringType(), True),
        StructField("key", StringType(), True),
        StructField("request", StringType(), True),
        StructField("http_status", IntegerType(), True),
        StructField("error_code", StringType(), True),
        StructField("bytes_sent", LongType(), True),
        StructField("object_size", LongType(), True),
        StructField("total_time", LongType(), True),
        StructField("turn_around_time", LongType(), True),
        StructField("referrer", StringType(), True),
        StructField("user_agent", StringType(), True),
        StructField("version_id", StringType(), True),
        StructField("error_line", StringType(), True),
    ]
)


def list_bucket_with_prefix(s3_client, bucket, prefix):
    """ Paginated listing of contents within an S3 bucket prefix. """
    token = None
    more_keys = True
    keys = []
    while more_keys:
        kwargs = {
            'Bucket': bucket,
            'Prefix': prefix,
        }
        if token is not None:
            kwargs['ContinuationToken'] = token
        response = s3_client.list_objects_v2(
            **kwargs
        )
        token = response.get('NextContinuationToken', None)
        more_keys = (token is not None)
        keys.extend([content['Key'] for content in response['Contents']])
    return keys


def get_s3a_paths(s3_client, bucket, prefix):
    """ Mutate S3 object keys into a fully qualified s3a URI. """
    return ['s3a://{}/{}'.format(bucket, key) for key in list_bucket_with_prefix(s3_client, bucket, prefix)]


def s3_read_file(s3_path, aws_access_key_id=None, aws_secret_access_key=None):
    session = boto3.Session(aws_access_key_id=aws_access_key_id,
                            aws_secret_access_key=aws_secret_access_key)
    s3_client = session.client('s3', 'us-west-2')
    parse_result = urlparse(s3_path)
    bucket = parse_result.netloc
    key = parse_result.path[1:]
    stream = s3_client.get_object(Bucket=bucket, Key=key)['Body']
    rows = []
    whole_file = stream.read().decode('utf-8')
    for line in whole_file.split('\n'):
        line = line.strip()
        if not line:
            continue
        result = parse_apache_log_line(line)
        if result:
            rows.append(result)
    return rows


def convert_s3_access_logs_to_parquet(
    aws_access_key_id=None,
    aws_secret_access_key=None,
    source_access_log_bucket=None,
    source_bucket=None,
    partition_key=None,
    destination_log_bucket=None,
    destination_log_prefix=None,
    num_partitions=5
):
    spark = (
        SparkSession
        .builder
        #
        # .appName('s3_server_side_log_compacter')
        # While this setting is already default, we want to be conservative and  guard
        #   against changes in future Spark versions, for as long as we use the
        #   version 2 fileoutputcommitter.
        .config('spark.speculation', 'false')
        # Note: We want to use timestamp with millis for Parquet
        .config('spark.sql.parquet.int64AsTimestampMillis', 'true')
        .getOrCreate()
    )
    spark._jsc.hadoopConfiguration().set(
        'mapreduce.fileoutputcommitter.algorithm.version',
        '2',
    )
    spark._jsc.hadoopConfiguration().set(
        'fs.s3a.access.key',
        aws_access_key_id,
    )
    spark._jsc.hadoopConfiguration().set(
        'fs.s3a.secret.key',
        aws_secret_access_key,
    )
    session = boto3.Session(aws_access_key_id=aws_access_key_id,
                            aws_secret_access_key=aws_secret_access_key, region_name='us-west-2')
    s3_client = session.client('s3', 'us-west-2')
    s3_logs_paths = get_s3a_paths(
        s3_client, source_access_log_bucket, '{}/{}-'.format(source_bucket, partition_key))
    s3_path_rdd = spark.sparkContext.parallelize(s3_logs_paths, numSlices=100)
    contents_rdd = s3_path_rdd.flatMap(lambda s3_path: s3_read_file(
        s3_path, aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key))
    access_logs_df = spark.createDataFrame(
        contents_rdd, S3_ACCESS_LOG_OUTPUT_SCHEMA)

    # The following patches request_time into a proper timestamp.
    access_logs_df = access_logs_df.withColumn('request_time', to_timestamp(
        access_logs_df.request_time_string, format="yyyy-MM-dd'T'HH:mm:ss"))
    access_logs_df = access_logs_df.select(
        'bucket_owner',
        's3_bucket',
        'request_time',
        'remote_ip',
        'requester',
        'request_id',
        'operation',
        'key',
        'request',
        'http_status',
        'error_code',
        'bytes_sent',
        'object_size',
        'total_time',
        'turn_around_time',
        'referrer',
        'user_agent',
        'version_id',
        'error_line'
    )
    sort_keys = ['request_time']
    partition_columns_names = []
    destination = 's3a://{destination_log_bucket}/{destination_log_prefix}/{source_bucket}/dt={partition_key}/'.format(
        source_bucket=source_bucket,
        partition_key=partition_key,
        destination_log_bucket=destination_log_bucket,
        destination_log_prefix=destination_log_prefix,
    )

    # Hotfix: Sorting issue.
    # 1. Repartition to ensure we create our desired number of Parquet files.
    # 2. Sort, solely within each partition, to at least get row groups sorted within Parquet.
    access_logs_df \
        .repartition(num_partitions) \
        .sortWithinPartitions(sort_keys) \
        .write \
        .option("compression", "snappy") \
        .partitionBy(partition_columns_names) \
        .parquet(destination)
    try:
        spark.stop()
    except Exception as e:
        print(e)


def date_iterator(start_date, end_date_exclusive):
    date_so_far = start_date
    while(date_so_far < end_date_exclusive):
        yield date_so_far
        date_so_far = date_so_far + datetime.timedelta(days=1)


def start_conversion(
    start_date_string,
    end_date_string,
    aws_config_path,
    source_access_log_bucket,
    source_bucket,
    destination_log_bucket,
    destination_log_prefix,
    num_output_files,
):
    yyyy_mm_dd_template = '%Y-%m-%d'
    start_date = datetime.datetime.strptime(
        start_date_string, yyyy_mm_dd_template)
    end_date = datetime.datetime.strptime(end_date_string, yyyy_mm_dd_template)
    dates = list(date_iterator(start_date, end_date))
    for a_date in dates:
        access_key, secret_key = get_aws_key_and_secret(aws_config_path)
        convert_s3_access_logs_to_parquet(
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            source_access_log_bucket=source_access_log_bucket,
            source_bucket=source_bucket,
            partition_key=a_date.strftime(yyyy_mm_dd_template),
            destination_log_bucket=destination_log_bucket,
            destination_log_prefix=destination_log_prefix,
            num_partitions=num_output_files,
        )


class S3ServerSideLoggingCompacter(object):

    def __init__(self):
        super(S3ServerSideLoggingCompacter, self).__init__()
        self.default_run_yesterday = True

    def custom_options(self, parser):
        parser.add_argument(
            '--aws-config',
            default='',
            help='Path to the AWS config file. Default: %(default)s',
        )
        parser.add_argument(
            '--source-access-log-bucket',
            default='your-s3-bucket-where-s3-access-logs-are',
            help='The S3 bucket containing S3 server side logging files. Default: %(default)s',
        )
        parser.add_argument(
            '--source-bucket',
            default='your-s3-bucket-where-the-original-contents-are',
            help='The S3 bucket being monitored. Default: %(default)s',
        )
        parser.add_argument(
            '--destination-log-bucket',
            default='your-s3-bucket-where-to-store-the-parquet-files',
            help='The S3 bucket storing compacted S3 server side logging files. Default: %(default)s',
        )
        parser.add_argument(
            '--destination-log-prefix',
            default='teams/metrics-data/s3_server_side_access_logs',
            help='S3 prefix used to store compacted log files. Default: %(default)s',
        )
        parser.add_argument(
            '--num-output-files',
            default=10,
            type=int,
            help='Number of output files. Default: %(default)s',
        )
        parser.add_argument(
            '--min-date',
            help='Starting date (inclusive).',
        )
        parser.add_argument(
            '--max-date',
            help='Ending date (exclusive).',
        )

    def start(self):
        parser = argparse.ArgumentParser()
        self.custom_options(parser)
        args = parser.parse_args()
        self.run(args)

    def run(self, args):
        try:
            start_conversion(
                args.min_date,
                args.max_date,
                args.aws_config,
                args.source_access_log_bucket,
                args.source_bucket,
                args.destination_log_bucket,
                args.destination_log_prefix,
                args.num_output_files,
            )
        except Exception as e:
            print(e)


if __name__ == '__main__':
    S3ServerSideLoggingCompacter().start()
