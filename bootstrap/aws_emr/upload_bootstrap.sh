#!/bin/bash

# We will upload the bootstrap script to a S3 bucket that can be used to 
# install the necessary system packages in all EMR nodes.
aws s3 cp bootstrap_emr.sh s3://<your-s3-bucket>/$USER/bootstrap_emr.sh
