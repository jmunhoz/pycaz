# Copyright (c) 2018 Javier M. Mellid <jmunhoz@igalia.com>
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.

import json
import time

from .process import Process
from .s3_objects import *
from .db import *
from . import logger
from . import config
from . import wks

class AWSCliDataSourceStrategy:

    def __init__(self):
        db = SQLiteAccess()
        db.open_default()
        db.health_or_die()
        self.db = db

    def __str__(self):
        return 'AWSCliDataSourceStrategy'

    def set_params(self, params):
        self.params = params

    def _get_master_gateway(self):
        return config.MASTER_GATEWAY

    def _get_archive_gateway(self):
        return config.ARCHIVE_GATEWAY

    def _get_tmp_file(self):
        return config.TMP_FILE

    def _get_bucket_name(self):
        # open db
        db = SQLiteAccess()
        db.open_default()
        db.health_or_die()
        bucket_name = db.get_bucket_name()
        if bucket_name is None:
            show_error_and_abort('Not bucket data found. Do you need to run --fetch-bucket?')
        return bucket_name

    def execute(self, command):
        logger.debug('Execute aws cli data source strategy : {}'.format(command))
        # fetch-bucket
        # aws --endpoint=http://rgw1:8000 s3api list-object-versions --bucket my-bucket
        if command == 'fetch-bucket':
            archive_gateway = self._get_archive_gateway()
            bucket_name = self.params[0]
            cmd = 'aws --endpoint={} s3api list-object-versions --bucket {}'.format(archive_gateway, bucket_name)
            try:
                 return Process().execute(cmd)
            except:
                 show_error_and_abort('Error fetching bucket')
        # get-object-from-archive
        # aws --endpoint=ARCHIVE_GATEWAY s3api get-object --bucket BUCKET_NAME --key KEY --version-id VERSION_ID /tmp/OUTPUT_FILE.raw
        if command == 'get-object-from-archive':
            archive_gateway = self._get_archive_gateway()
            bucket_name = self._get_bucket_name()
            key = self.params[0]
            version_id = self.params[1]
            file = self._get_tmp_file()
            cmd = "aws --endpoint={} s3api get-object --bucket {} --key {} --version-id='{}' {}".format(archive_gateway, bucket_name, key, version_id, file)
            try:
                 return Process().execute(cmd)
            except:
                 show_error_and_abort('Error retrieving object from archive zone')
        # put-object-in-master
        # aws --endpoint=MASTER_GATEWAY s3api put-object --bucket BUCKET_NAME --key KEY --body /tmp/OUTPUT_FILE.raw
        if command == 'put-object-in-master':
            master_gateway = self._get_master_gateway()
            bucket_name = self._get_bucket_name()
            key = self.params[0]
            file = self._get_tmp_file()
            cmd = 'aws --endpoint={} s3api put-object --bucket {} --key {} --body {}'.format(master_gateway, bucket_name, key, file)
            try:
                 return Process().execute(cmd)
            except:
                 show_error_and_abort('Error storing object in master zone')
        # delete-object-in-archive
        # aws --endpoint=ARCHIVE_GATEWAY s3api delete-object --bucket BUCKET_NAME --key KEY
        if command == 'delete-object-in-archive':
            archive_gateway = self._get_archive_gateway()
            bucket_name = self._get_bucket_name()
            key = self.params[0]
            version_id = self.params[1]
            cmd = "aws --endpoint={} s3api delete-object --bucket {} --key {} --version-id='{}'".format(archive_gateway, bucket_name, key, version_id)
            return Process().execute(cmd)
            try:
                 return Process().execute(cmd)
            except:
                 show_error_and_abort('Error deleting object from archive zone')

        return None

class DataSourceManager:
    def __init__(self, data_source):
        self.data_source = data_source

    def execute(self, command):
        return self.data_source.execute(command)

class S3CommandResult:
    pass

class S3Command:

    s3_cmd_name = None
    s3_cmd_params = None

    def execute(self):
        strategy = AWSCliDataSourceStrategy()
        logger.debug('Enabled strategy : {}'.format(strategy))
        strategy.set_params(self.s3_cmd_params)
        dsm = DataSourceManager(strategy)
        return dsm.execute(self.s3_cmd_name)

    def set_params(self, params):
        self.s3_cmd_params = params

    def show_output(self):
        show_error_and_abort('S3Command requires output')

class S3CommandFactory:

    factories = {}

    def addFactory(id, s3CommandFactory):
        S3CommandFactory.factories.put[id] = s3CommandFactory
    addFactory = staticmethod(addFactory)

    def createS3Command(str):
        str = [s.capitalize() for s in str.split('-')]
        str.insert(0, 'S3Command')
        id  = ''.join(str)
        if id not in S3CommandFactory.factories:
            S3CommandFactory.factories[id] = eval(id + '.Factory()')
        return S3CommandFactory.factories[id].create()
    createS3Command = staticmethod(createS3Command)

class S3CommandInitDb(S3Command):

    s3_cmd_name = 'init-db'

    class Factory:
        def create(self):
            return S3CommandInitDb()

    def execute(self):
        db = SQLiteAccess()
        if db.exists():
            show_error_and_abort('Error initializing, db exists')
        try:
            db.set_up()
        except:
            show_error_and_abort('{} failed'.format(self.s3_cmd_name))

    def show_output(self):
        print('{} ok'.format(self.s3_cmd_name))

class S3CommandInfoDb(S3Command):

    s3_cmd_name = 'info-db'

    class Factory:
        def create(self):
            return S3CommandInfoDb()

    def execute(self):
        # open db
        db = SQLiteAccess()
        db.open_default()
        db.health_or_die()
        bucket_name = db.get_bucket_name()
        if bucket_name is None:
            show_error_and_abort('Not bucket data found. Do you need to run --fetch-bucket?')
        # show current bucket name
        print('bucket name : {}'.format(bucket_name))

    def show_output(self):
        print('{} ok'.format(self.s3_cmd_name))

class S3CommandPurgeDb(S3Command):

    s3_cmd_name = 'purge-db'

    class Factory:
        def create(self):
            return S3CommandPurgeDb()

    def execute(self):
        db = SQLiteAccess()
        if db.exists() is False:
            show_error_and_abort('Error db not available to purge')
        db.purge()

    def show_output(self):
        print('{} ok'.format(self.s3_cmd_name))

class S3CommandListObjects(S3Command):

    s3_cmd_name = 'list-objects'

    class Factory:
        def create(self):
            return S3CommandListObjects()

    def execute(self):
        # open db
        db = SQLiteAccess()
        if db.exists() is False:
            show_error_and_abort('Error db not exist. Do you need to run --init-db?')
        db.open_default()
        db.health_or_die()
        # retrieve and show ordered records
        for s3o in db.get_objects_by_last_modified():
            print(s3o.pretty_str())

    def show_output(self):
        print('{} ok'.format(self.s3_cmd_name))

class S3CommandListObjectsByKey(S3Command):

    s3_cmd_name = 'list-objects-by-key'

    class Factory:
        def create(self):
            return S3CommandListObjectsByKey()

    def execute(self):
        # open db
        db = SQLiteAccess()
        if db.exists() is False:
            show_error_and_abort('Error db not exist. Do you need to run --init-db?')
        db.open_default()
        db.health_or_die()
        # retrieve key
        if self.s3_cmd_params is None:
            show_error_and_abort('This command requires an object key')
        # retrieve and show ordered records by key
        for s3o in db.get_objects_by_key_and_last_modified(self.s3_cmd_params[0]):
            print(s3o.pretty_str())

    def show_output(self):
        print('{} ok'.format(self.s3_cmd_name))

class S3CommandListKeys(S3Command):

    s3_cmd_name = 'list-keys'

    class Factory:
        def create(self):
            return S3CommandListKeys()

    def execute(self):
        # open db
        db = SQLiteAccess()
        if db.exists() is False:
            show_error_and_abort('Error db not exist. Do you need to run --init-db?')
        db.open_default()
        db.health_or_die()
        # retrieve and show ordered records
        for k in db.get_all_keys():
            print(k)

    def show_output(self):
        print('{} ok'.format(self.s3_cmd_name))

class S3CommandFetchBucket(S3Command):

    s3_cmd_name = 'fetch-bucket'

    class Factory:
        def create(self):
            return S3CommandFetchBucket()

    def execute(self):

        # run s3 command
        r = super().execute()

        # open db
        db = SQLiteAccess()
        if db.exists() is False:
            show_error_and_abort('Error db not exist. Do you need to run --init-db?')
        db.open_default()
        db.health_or_die()

        # store bucket name in db
        if self.s3_cmd_params is None:
            show_error_and_abort('This command requires a bucket name.')
        if db.get_bucket_name() is not None:
            show_error_and_abort('Previous bucket data found. Do you need to run --purge-db?')
        try:
            db.add_bucket_name(self.s3_cmd_params[0])
        except:
            show_error_and_abort('Error adding bucket name in db')

        # store object versions in db
        try:
             for v in json.loads(r.stdout.decode('UTF-8'))['Versions']:
                 o = v['Owner']
                 s3o = S3Owner(0,
                               o['DisplayName'],
                               o['ID'])
                 s3v = S3Version(0,
                                 v['LastModified'],
                                 v['VersionId'],
                                 v['ETag'],
                                 v['StorageClass'],
                                 v['Key'],
                                 s3o,
                                 v['IsLatest'],
                                 v['Size'])
                 #print(s3v)
                 try:
                     db.add_object(s3v)
                 except:
                     show_error_and_abort('{} (versions) failed'.format(self.s3_cmd_name))
        except:
             # no object versions availables
             pass

        # store delete markers in db
        try:
             for v in json.loads(r.stdout.decode('UTF-8'))['DeleteMarkers']:

                 o = v['Owner']
                 s3o = S3Owner(0,
                               o['DisplayName'],
                               o['ID'])
                 s3dm = S3DeleteMarker(0,
                                  v['LastModified'],
                                  v['VersionId'],
                                  v['Key'],
                                  s3o,
                                  v['IsLatest'])
                 #print(s3dm)
                 try:
                     db.add_object(s3dm)
                 except:
                     show_error_and_abort('{} (delete markers) failed'.format(self.s3_cmd_name))
        except:
             # no delete markers availables
             pass

    def show_output(self):
        print('{} ok'.format(self.s3_cmd_name))

class S3CommandRecoverObject(S3Command):

    s3_cmd_name = 'recover-object'

    def _get_bucket_name(self):
        # open db
        db = SQLiteAccess()
        db.open_default()
        db.health_or_die()
        bucket_name = db.get_bucket_name()
        if bucket_name is None:
            show_error_and_abort('Not bucket data found. Do you need to run --fetch-bucket?')
        return bucket_name

    def _delete_all_table_content(self):
        # open db
        db = SQLiteAccess()
        db.open_default()
        db.health_or_die()
        db.delete_all_content()

    class Factory:
        def create(self):
            return S3CommandRecoverObject()

    def execute(self):

        # handle '-' bug if needed
        self.s3_cmd_params = wks.unfix(self.s3_cmd_params)

        # save working bucket name
        bucket_name = self._get_bucket_name()

        # recover object from archive zone
        c = S3Command()
        c.s3_cmd_name = 'get-object-from-archive'
        c.s3_cmd_params = self.s3_cmd_params
        r = c.execute()
        #print(r.stdout.decode('UTF-8'))

        # store recovered object in master zone
        c = S3Command()
        c.s3_cmd_name = 'put-object-in-master'
        c.s3_cmd_params = self.s3_cmd_params
        r = c.execute()
        #print(r.stdout.decode('UTF-8'))

        # delete db
        self._delete_all_table_content()

        # wait for object replication
        time.sleep(config.SYNC_WINDOW_TIME)

        # fetch bucket
        c = S3CommandFetchBucket()
        c.s3_cmd_params = [bucket_name]
        r = c.execute()

    def show_output(self):
        print('{} ok'.format(self.s3_cmd_name))

class S3CommandTrimObjects(S3Command):

    s3_cmd_name = 'trim-objects'

    def _delete_db_objects_below_date(self, tdate):
        # open db
        db = SQLiteAccess()
        db.open_default()
        db.health_or_die()
        db.delete_objects_below_date(tdate)

    def _trim_objects_in_storage_below_date(self, tdate):
        db = SQLiteAccess()
        db.open_default()
        db.health_or_die()
        for s3o in db.get_objects_by_last_modified_below_date(tdate, order='asc'):
            print('{} key = {}, version-id = {} ... '.format(s3o.last_modified, s3o.key, s3o.version_id), end='')
            # delete object in storage by version-id
            c = S3Command()
            c.s3_cmd_name = 'delete-object-in-archive'
            c.s3_cmd_params = [s3o.key, s3o.version_id]
            r = c.execute()
            #print(r.stdout.decode('UTF-8'))
            print('DELETED')

    class Factory:
        def create(self):
            return S3CommandTrimObjects()

    def execute(self):

        # get trimming date
        tdate = self.s3_cmd_params[0]

        # trim objects in storage
        self._trim_objects_in_storage_below_date(tdate)

        # trim objects in db
        self._delete_db_objects_below_date(tdate)
 
    def show_output(self):
        print('{} ok'.format(self.s3_cmd_name))

def show_error_and_abort(str):
    logger.debug(str)
    print(str)
    exit(-1)
