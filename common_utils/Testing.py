import boto3
import pandas as pd
from dataRetrival import read_from_cloud
import json
cred_path = "/home/razz/Rajesh/ExtraTransLoad/Credentials/awsKeys.json"

df = read_from_cloud(cred_path)
print(df.head())

