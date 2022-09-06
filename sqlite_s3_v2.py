import os
import boto3
import datetime
from datetime import date, datetime, time, timedelta,timezone
from http import client

from decouple import config


class SQLite_S3_Export_Manager(object):
    
    def __init__(self):
        """  
        """
        self.database_directory = config('DATABASE_DIRECTORY')
        self.bucket = config('BUCKET_NAME')
        self.aws_access_key = config('AWS_ACCESS_KEY_ID')
        self.aws_secret_key = config('AWS_SECRET_KEY')
        self.region = config('AWS_REGION')
        
    def init_connection(self):
        """
        Initializes S3 connection.
        """
        s3_client = boto3.client(
            's3',
            region_name=self.region,
            aws_access_key_id=self.aws_access_key,
            aws_secret_access_key= self.aws_secret_key
            )
        s3_resource = boto3.resource(
            's3',
            region_name=self.region,
            aws_access_key_id=self.aws_access_key,
            aws_secret_access_key= self.aws_secret_key

        )

        return {"client":s3_client,"resource":s3_resource}
    
    def remove_old_db(self, conn):
        """
        Removes database files that are older than specified date range.
        Date range here 30days
        """
    
        client = conn['client']
        vault = client.list_objects(Bucket=self.bucket)
        for k in vault['Contents']:
            t_now = datetime.datetime.now(timezone.utc)
            bucket_date = k['LastModified']
            time_diff = t_now - bucket_date

            if time_diff.days >= 30:
                client.delete_object(Bucket=self.bucket,Key=k['Key'])
    
    def backup_db(self, conn):
        """
        Retrieve the file to backup.  Appends _(today's date) to original
        filename.s3
        
        Example:
        test.db would become test.db_2010-03-01
        """
        bucket = client.list_objects(Bucket=self.bucket)
        db_file = self.find_files
        file_name = db_file['filename']
        key_name = db_file['filename'] + "_%s" % date.today()
      
        try:
            s3 = conn['resource']
            s3.meta.client.upload_file(
                file_name = file_name,
                bucket = bucket,
                key_name = key_name,
                ExtraArgs = {
                    'ACL':'private',
                    'ServerSideEncryption':'aws:kms',
                    'SSEKMSKeyId':'alias/aws/s3'
                }
            )
            # s3 = conn['client']
            # with open(file_name,'rb') as data:
            #     s3.upload_fileobj(data,Bucket=bucket,Key=key_name)
        except:
            pass
            # Handle errors returned from AWS here

    def find_files(self):
        """
        Walk through the vault directory to find every *.sqlite3 file.  If *.sqlite3
        file is found append it's path to the file_list.
        """
        for root, dirs, files in os.walk(self.database_directory):
            for d in self.ignore:
                if d in dirs:
                    dirs.remove(d)
            for f in files:
                extension = os.path.splitext(f)[1]
                if extension == ".sqlite3":
                    full_path = os.path.join(root, f)
                    data = {'path': full_path, 'filename': f}
            
        return data


if __name__=="__main__":
    manager = SQLite_S3_Export_Manager()
    conn = manager.init_connection()
    manager.remove_old_db(conn)
    manager.backup_db(conn)