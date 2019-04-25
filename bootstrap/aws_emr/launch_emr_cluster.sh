#!/bin/bash

CLUSTER_NAME="${USER}_aws_logs_to_parquet_converter"
BUCKET='your-bucket-name'


aws emr create-cluster --applications Name=Spark \
--ec2-attributes '{  
  "KeyName":"EMR",
  "InstanceProfile":"your-instance-profile",
  "SubnetId":"your-subnet-id",
  "EmrManagedSlaveSecurityGroup":"your-emr-managed-slave-security-group",
  "EmrManagedMasterSecurityGroup":"your-emr-managed-master-security-group"
}'  \
--service-role mrjob-emr --release-label emr-5.23.0 \
--name "${CLUSTER_NAME}" --instance-groups '[  
  {  
    "InstanceCount":1,
    "InstanceGroupType":"MASTER",
    "InstanceType":"m3.xlarge",
    "Name":"Master instance group - 1"
  },
  {  
    "InstanceCount":2,
    "EbsConfiguration":{  
      "EbsBlockDeviceConfigs":[  
        {  
          "VolumeSpecification":{  
            "SizeInGB":840,
            "VolumeType":"gp2"
          },
          "VolumesPerInstance":1
        }
      ],
      "EbsOptimized":false
    },
    "InstanceGroupType":"CORE",
    "InstanceType":"m3.xlarge",
    "Name":"Core instance group - 2"
  }
]'  \
--configurations '[
  {
     "Classification": "spark-env",
     "Configurations": [
       {
         "Classification": "export",
         "Properties": {
            "PYSPARK_PYTHON": "/usr/bin/python3"
          }
       }
    ]
  }
]' \
--bootstrap-actions \
Path=s3://${BUCKET}/${USER}/bootstrap_emr.sh,Args=[] \
--region us-west-2 \
--enable-debugging --log-uri s3://${BUCKET}/${USER}/emr_logs
