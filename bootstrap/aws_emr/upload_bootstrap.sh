#!/bin/bash

# We will upload the bootstrap script to a s3 bucket that can be use to 
# install necessary system packages in all EMR nodes
aws s3 cp bootstrap_emr.sh s3://<your-s3-bucket>/$USER/bootstrap_emr.sh
