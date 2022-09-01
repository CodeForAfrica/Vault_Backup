import os
from datetime import date, time, timedelta

from decouple import config

from boto.s3.connection import S3Connection
from boto.s3.key import Key
from boto.exception import S3ResponseError

class SQLite_S3_Export_Manager(object):
    
    def __init__(self):
        """  
        """
        self.database_directory = config('DATABASE_DIRECTORY')
        self.bucket = config('BUCKET_NAME')
        self.aws_access_key = config('AWS_ACCESS_KEY_ID')
        self.aws_secret_key = config('AWS_SECRET_KEY')
        self.database_expiry_date = date.today() - timedelta(days=30)
        
    def get_connection(self):
        """
        Initializes S3 connection.
        """
        return S3Connection(self.aws_access_key, self.aws_secret_key)
        
    def remove_old_db(self, conn):
        """
        Removes database files that are older than specified date range.
        We chose to keep files from the first day of every month for our
        archive.
        """
        bucket = conn.get_bucket(self.bucket)
        keys = bucket.list()
        for k in keys:
            kd = k.name[-10:]
            key_date = date(int(kd[:4]), int(kd[5:7]), int(kd[-2:]))
            if key_date < self.database_expiry_date and key_date.day != 1:
                k.delete()
                
    def backup_db(self, conn):
        """
        Retrieve the file to backup.  Appends _(today's date) to original
        filename.  This allows the remove_old_db function to check file dates.
        
        Example:
        test.db would become test.db_2010-03-01
        """
        bucket = conn.get_bucket(self.bucket)
        db_file = self.find_files
        file_name = db_file['filename']
        key_name = db_file['filename'] + "_%s" % date.today()
        key = bucket.new_key(key_name)
        key.set_metadata('Content-Type', 'text/plain')

        try:
            s3 = boto3.resource('s3')
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
        except S3ResponseError:
            pass
            # Handle errors returned from AWS here
                
    def find_files(self):
        """
        Walk through the given directory to find every *.db file.  If *.db
        file is found append it's path to the file_list.
        """
        for root, dirs, files in os.walk(self.database_directory):
            for d in self.ignore:
                if d in dirs:
                    dirs.remove(d)
            for f in files:
                extension = os.path.splitext(f)[1]
                if extension == ".db":
                    full_path = os.path.join(root, f)
                    data = {'path': full_path, 'filename': f}
            
        return data
            
        
if __name__=="__main__":
    manager = SQLite_S3_Export_Manager()
    conn = manager.get_connection()
    manager.remove_old_db(conn)
    manager.backup_db(conn)
    

