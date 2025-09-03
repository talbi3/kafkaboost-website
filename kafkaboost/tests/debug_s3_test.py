#!/usr/bin/env python3
"""
Debug test to identify S3ConfigManager issues
"""

import os
import boto3
from botocore.exceptions import ClientError, NoCredentialsError
from kafkaboost.aws_config import AWSConfig
from kafkaboost.s3_config_manager import S3ConfigManager

def check_aws_credentials():
    """Check if AWS credentials are properly configured."""
    print("=== AWS Credentials Check ===")
    
    # Check environment variables
    aws_access_key = os.getenv('AWS_ACCESS_KEY_ID')
    aws_secret_key = os.getenv('AWS_SECRET_ACCESS_KEY')
    aws_session_token = os.getenv('AWS_SESSION_TOKEN')
    
    print(f"AWS_ACCESS_KEY_ID: {'✅ Set' if aws_access_key else '❌ Not set'}")
    print(f"AWS_SECRET_ACCESS_KEY: {'✅ Set' if aws_secret_key else '❌ Not set'}")
    print(f"AWS_SESSION_TOKEN: {'✅ Set' if aws_session_token else '❌ Not set'}")
    
    # Try to create a basic S3 client
    try:
        s3_client = boto3.client('s3', region_name='us-east-1')
        # Try a simple operation
        s3_client.list_buckets()
        print("✅ AWS credentials are working - can list buckets")
        return True
    except NoCredentialsError:
        print("❌ No AWS credentials found")
        return False
    except ClientError as e:
        print(f"❌ AWS credentials error: {e}")
        return False
    except Exception as e:
        print(f"❌ Unexpected error: {e}")
        return False

def check_s3_bucket_access():
    """Check if we can access the S3 bucket."""
    print("\n=== S3 Bucket Access Check ===")
    
    bucket_name = 'kafkaboostwebsitef5b2ed53e119465ebb1961d042ec48af3c1-dev'
    
    try:
        s3_client = boto3.client('s3', region_name='us-east-1')
        
        # Check if bucket exists
        s3_client.head_bucket(Bucket=bucket_name)
        print(f"✅ Bucket '{bucket_name}' exists and is accessible")
        
        # List objects in bucket
        response = s3_client.list_objects_v2(Bucket=bucket_name, MaxKeys=10)
        if 'Contents' in response:
            print(f"✅ Found {len(response['Contents'])} objects in bucket:")
            for obj in response['Contents']:
                print(f"   - {obj['Key']} (size: {obj['Size']} bytes)")
        else:
            print("⚠️  Bucket is empty")
        
        return True
        
    except ClientError as e:
        error_code = e.response['Error']['Code']
        if error_code == 'NoSuchBucket':
            print(f"❌ Bucket '{bucket_name}' does not exist")
        elif error_code == 'AccessDenied':
            print(f"❌ Access denied to bucket '{bucket_name}'")
        else:
            print(f"❌ Error accessing bucket: {e}")
        return False
    except Exception as e:
        print(f"❌ Unexpected error: {e}")
        return False

def test_user_config_lookup():
    """Test finding user configuration files."""
    print("\n=== User Config Lookup Test ===")
    
    user_id = "5428b428-20a1-7051-114f-c24ede151b86"
    bucket_name = 'kafkaboostwebsitef5b2ed53e119465ebb1961d042ec48af3c1-dev'
    
    try:
        s3_client = boto3.client('s3', region_name='us-east-1')
        
        # List all objects and look for user files
        response = s3_client.list_objects_v2(Bucket=bucket_name, MaxKeys=1000)
        
        if 'Contents' in response:
            user_files = []
            for obj in response['Contents']:
                if user_id in obj['Key']:
                    user_files.append(obj)
            
            if user_files:
                print(f"✅ Found {len(user_files)} files for user {user_id}:")
                for file in user_files:
                    print(f"   - {file['Key']} (modified: {file['LastModified']})")
                
                # Get the latest file
                latest_file = max(user_files, key=lambda x: x['LastModified'])
                print(f"✅ Latest file: {latest_file['Key']}")
                
                # Try to read the file content
                try:
                    response = s3_client.get_object(Bucket=bucket_name, Key=latest_file['Key'])
                    content = response['Body'].read().decode('utf-8')
                    import json
                    config = json.loads(content)
                    print(f"✅ Config content: {len(config)} keys")
                    print(f"   Config: {json.dumps(config, indent=2)}")
                except Exception as e:
                    print(f"❌ Error reading config file: {e}")
                
            else:
                print(f"❌ No files found for user {user_id}")
                print("   Available files:")
                for obj in response['Contents']:
                    print(f"     - {obj['Key']}")
        else:
            print("❌ No objects found in bucket")
        
    except Exception as e:
        print(f"❌ Error in user config lookup: {e}")

def main():
    print("=== S3ConfigManager Debug Test ===")
    
    # Step 1: Load AWS configuration
    print("\n1. Loading AWS configuration...")
    try:
        aws_config = AWSConfig.from_file('kafkaboost/aws_config.json')
        print("✅ AWS configuration loaded successfully")
        print(f"   S3 Bucket: {aws_config.get_s3_bucket()}")
        print(f"   Region: {aws_config.get_region()}")
    except Exception as e:
        print(f"❌ Failed to load AWS configuration: {str(e)}")
        return
    
    # Step 2: Check AWS credentials
    credentials_ok = check_aws_credentials()
    
    # Step 3: Check S3 bucket access
    if credentials_ok:
        bucket_ok = check_s3_bucket_access()
        
        # Step 4: Test user config lookup
        if bucket_ok:
            test_user_config_lookup()
    
    # Step 5: Test S3ConfigManager
    print("\n=== S3ConfigManager Test ===")
    user_id = "5428b428-20a1-7051-114f-c24ede151b86"
    
    try:
        s3_manager = S3ConfigManager(
            user_id=user_id,
            auto_save_local=True,
            local_file_path="debug_config.json",
            aws_config_file="kafkaboost/aws_config.json"
        )
        
        print(f"✅ S3ConfigManager created successfully!")
        print(f"   User ID: {s3_manager.get_user_id()}")
        print(f"   S3 Key: {s3_manager.get_s3_key()}")
        
        config = s3_manager.get_config()
        print(f"✅ Config retrieved: {len(config)} keys")
        print(f"   Config content: {config}")
        
    except Exception as e:
        print(f"❌ Error with S3ConfigManager: {str(e)}")

if __name__ == "__main__":
    main()
