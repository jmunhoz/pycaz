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

import sqlite3
import os.path
import dateutil.parser

from .s3_objects import *
from . import logger
from . import config

class SQLiteAccess:

    db_name = config.DB_FILE
    database = None

    def open(self, filename):
        logger.debug('connecting {} sqlite db'.format(self.db_name))
        if self.database is not None:
            self.database.close()
        self.database = sqlite3.connect(filename, isolation_level='DEFERRED')
        self.database.row_factory = sqlite3.Row

    def open_default(self):
        self.open(self.db_name)

    def exists(self):
        return os.path.isfile(self.db_name)

    def purge(self):
        os.remove(self.db_name)

    def health_or_die(self):
        querys = [
        "SELECT NAME FROM BUCKET LIMIT 1",
        "SELECT COUNT(*) FROM OWNERS LIMIT 1",
        "SELECT COUNT(*) FROM OBJECTS LIMIT 1"
        ]
        try:
            for q in querys:
                row = self.database.execute(q).fetchone()
        except sqlite3.OperationalError:
            logger.debug('Error health_or_die()')
            print('Error db requires proper initilization')
            exit(-1)

    def delete_all_content(self):
        sql_ddl = """
        DELETE FROM BUCKET;
        DELETE FROM OWNERS;
        DELETE FROM OBJECTS;
        """
        self.open_default()
        logger.debug('deleting db tables content')
        try:
            self.database.executescript(sql_ddl)
        except sqlite3.OperationalError:
            logger.debug('Error db.delete_all_content()')
            raise
        logger.debug('db tables deleted')

    def delete_objects_below_date(self, tdate):
        # validate tdate
        try:
             dateutil.parser.parse(tdate)
        except:
             logger.debug('Error db.delete_objects_below_date() parsing trimming date')
             print('Error invalid date')
             exit(-1)

        sql_ddl = """
        delete from objects where last_modified < '{}';
        """.format(tdate);
        self.open_default()
        logger.debug('Deleting db tables content')
        try:
            self.database.executescript(sql_ddl)
        except sqlite3.OperationalError:
            logger.debug('Error db.delete_objects_below_date()')
            raise
        logger.debug('db objects deleted')

    def _begin_tx(self):
        self.database.execute('BEGIN')

    def _step_tx(self, sql_l):
        try:
            for (sql_str, values) in sql_l:
                sql_str_dbg = ' '.join(sql_str.replace('\n','').split())
                #logger.debug([sql_str_dbg,values])
                self.database.execute(sql_str, values)
            self.database.commit()
        except Exception:
            logger.debug('Rollback sql transaction')
            self.database.rollback()
            raise

    def _end_tx(self):
        try:
            self.database.commit()
        except Exception:
            logger.debug('Can not commit. Rollback sql transaction')
            self.database.rollback()
            raise

    def _execute_tx(self, sql_l):
        self._begin_tx()
        self._step_tx(sql_l)
        self._end_tx()

    def set_up(self):
        logger.debug('Setting up sqlite db')
        sql_ddl = """
        CREATE TABLE BUCKET (
        NAME TEXT PRIMARY KEY
        );
        CREATE TABLE OWNERS (
        XID INTEGER PRIMARY KEY AUTOINCREMENT,
        DISPLAY_NAME TEXT,
        ID TEXT
        );
        CREATE TABLE OBJECTS (
        XID INTEGER PRIMARY KEY AUTOINCREMENT,
        LAST_MODIFIED TEXT,
        VERSION_ID TEXT,
        ETAG TEXT,
        STORAGE_CLASS TEXT,
        KEY TEXT,
        OWNER_XID INTEGER,
        IS_LATEST INTEGER,
        SIZE INTEGER,
        TYPE CHAR,
        FOREIGN KEY(OWNER_XID) REFERENCES OWNERS(XID)
        );
        """
        self.open_default()
        logger.debug('Creating db tables')
        try:
            self.database.executescript(sql_ddl)
        except sqlite3.OperationalError:
            logger.debug('Error db.set_up()')
            raise
        logger.debug('db tables created')

    def get_owner_by_xid(self, xid):
        query_owner_by_xid = """
        SELECT * FROM OWNERS WHERE XID = ?
        """
        try:
            row = self.database.execute(query_owner_by_xid, (xid,)).fetchone()
        except sqlite3.OperationalError:
            logger.debug('Error db.get_owner_by_xid()')
            raise
        if row is None:
            return None
        return S3Owner(row['xid'], row['display_name'], row['id'])

    def get_owner_by_id(self, id):
        query_owner_by_id = """
        SELECT * FROM OWNERS WHERE ID = ?
        """
        try:
            row = self.database.execute(query_owner_by_id, (id,)).fetchone()
        except sqlite3.OperationalError:
            logger.debug('Error db.get_owner_by_id()')
            raise
        if row is None:
            return None
        return S3Owner(row['xid'], row['display_name'], row['id'])

    def get_object_by_version_id(self, version_id):
        query_object_by_version_id = """
        SELECT * FROM OBJECTS WHERE VERSION_ID = ?
        """
        try:
            row = self.database.execute(query_object_by_version_id, (version_id,)).fetchone()
        except sqlite3.OperationalError:
            logger.debug('Error db.get_object_by_version_id()')
            raise
        if row is None:
            return None
        s3o = self.get_owner_by_xid(row['owner_xid'])
        return S3Version(row['xid'],
                         row['last_modified'],
                         row['version_id'],
                         row['etag'],
                         row['storage_class'],
                         row['key'],
                         s3o,
                         row['is_latest'],
                         row['size'])

    def get_objects_by_key_and_last_modified(self, key, order='desc'):

        class RecordsIt(object):

            def __init__(self, parent, key, order):
                self.parent = parent
                self.query_objects_by_key_and_last_modified = """
                select * from objects where key = '{}' order by last_modified {};
                """.format(key,order)
                try:
                    self.cursor = self.parent.database.execute(self.query_objects_by_key_and_last_modified)
                except sqlite3.OperationalError:
                    logger.debug('Error db.get_objects_by_last_modified()')
                    raise

            def __iter__(self):
                return self

            def __next__(self):
              row = self.cursor.fetchone()
              if row is None:
                  raise StopIteration
              s3o = self.parent.get_owner_by_xid(row['owner_xid'])
              if row['type'] == 'V':
                  s3x = S3Version(row['xid'],
                                  row['last_modified'],
                                  row['version_id'],
                                  row['etag'],
                                  row['storage_class'],
                                  row['key'],
                                  s3o,
                                  row['is_latest'],
                                  row['size'])
              else:
                  s3x = S3DeleteMarker(row['xid'],
                                       row['last_modified'],
                                       row['version_id'],
                                       row['key'],
                                       s3o,
                                       row['is_latest'])
              return s3x

        return RecordsIt(self, key, order)


    def get_objects_by_last_modified(self, order='desc'):

        class RecordsIt(object):

            def __init__(self, parent, order):
                self.parent = parent
                self.query_objects_by_last_modified = """
                select * from objects order by last_modified {};
                """.format(order)
                try:
                    self.cursor = self.parent.database.execute(self.query_objects_by_last_modified)
                except sqlite3.OperationalError:
                    logger.debug('Error db.get_objects_by_last_modified()')
                    raise

            def __iter__(self):
                return self

            def __next__(self):
              row = self.cursor.fetchone()
              if row is None:
                  raise StopIteration
              s3o = self.parent.get_owner_by_xid(row['owner_xid'])
              if row['type'] == 'V':
                  s3x = S3Version(row['xid'],
                                  row['last_modified'],
                                  row['version_id'],
                                  row['etag'],
                                  row['storage_class'],
                                  row['key'],
                                  s3o,
                                  row['is_latest'],
                                  row['size'])
              else:
                  s3x = S3DeleteMarker(row['xid'],
                                       row['last_modified'],
                                       row['version_id'],
                                       row['key'],
                                       s3o,
                                       row['is_latest'])
              return s3x

        return RecordsIt(self, order)

    def get_objects_by_last_modified_below_date(self, date, order='desc'):

        class RecordsIt(object):

            def __init__(self, parent, order):
                self.parent = parent
                self.query_objects_by_last_modified_below_date = """
                select * from objects where last_modified < '{}' order by last_modified {};
                """.format(date, order)
                try:
                    self.cursor = self.parent.database.execute(self.query_objects_by_last_modified_below_date)
                except sqlite3.OperationalError:
                    logger.debug('Error db.get_objects_by_last_modified_below_date()')
                    raise

            def __iter__(self):
                return self

            def __next__(self):
              row = self.cursor.fetchone()
              if row is None:
                  raise StopIteration
              s3o = self.parent.get_owner_by_xid(row['owner_xid'])
              if row['type'] == 'V':
                  s3x = S3Version(row['xid'],
                                  row['last_modified'],
                                  row['version_id'],
                                  row['etag'],
                                  row['storage_class'],
                                  row['key'],
                                  s3o,
                                  row['is_latest'],
                                  row['size'])
              else:
                  s3x = S3DeleteMarker(row['xid'],
                                       row['last_modified'],
                                       row['version_id'],
                                       row['key'],
                                       s3o,
                                       row['is_latest'])
              return s3x

        return RecordsIt(self, order)

    def add_object(self, obj):

        insert_into_owners = """
        INSERT INTO OWNERS (XID,
                            DISPLAY_NAME,
                            ID)
                    VALUES (?,?,?);
        """

        insert_into_objects = """
        INSERT INTO OBJECTS (LAST_MODIFIED,
                             VERSION_ID,
                             ETAG,
                             STORAGE_CLASS,
                             KEY,
                             OWNER_XID,
                             IS_LATEST,
                             SIZE,
                             TYPE)
                    VALUES (?,?,?,?,?,?,?,?,?);
        """

        # S3Version
        obj_type = 'V'
        if isinstance(obj, S3DeleteMarker):
            # S3DeleteMarker
            obj_type = 'D'

        sql_l = []

        self._begin_tx()

        owner = self.get_owner_by_id(obj.owner.id)

        if owner is None:
            owner  = (None,
                      obj.owner.display_name,
                      obj.owner.id)
            sql_l.append((insert_into_owners,owner))
            self._step_tx(sql_l)
            sql_l = []
            owner = self.get_owner_by_id(obj.owner.id)
            assert(owner is not None)

        object = (obj.last_modified,
                  obj.version_id,
                  obj.etag,
                  obj.storage_class,
                  obj.key,
                  owner.xid,
                  obj.is_latest,
                  obj.size,
                  obj_type)

        sql_l.append((insert_into_objects,object))

        self._step_tx(sql_l)

        self._end_tx()

    def add_bucket_name(self, bucket_name):

        insert_into_bucket = """
        INSERT INTO BUCKET (NAME) VALUES (?);
        """

        sql_l = []

        self._begin_tx()

        bucket = (bucket_name,)

        sql_l.append((insert_into_bucket, bucket))

        self._step_tx(sql_l)

        self._end_tx()

    def get_bucket_name(self):

        query_bucket_name = """
        SELECT name FROM BUCKET
        """

        try:
            row = self.database.execute(query_bucket_name).fetchone()
        except sqlite3.OperationalError:
            logger.debug('Error db.get_bucket_name()')
            raise

        if row is None:
            return None
        return row['name']

    def get_all_keys(self, order='asc'):

        class RecordsIt(object):

            def __init__(self, parent, order):
                self.parent = parent
                self.query_objects_all_keys = """
                select distinct(key) from objects order by key {};
                """.format(order)
                try:
                    self.cursor = self.parent.database.execute(self.query_objects_all_keys)
                except sqlite3.OperationalError:
                    logger.debug('Error db.get_all_keys()')
                    raise

            def __iter__(self):
                return self

            def __next__(self):
              row = self.cursor.fetchone()
              if row is None:
                  raise StopIteration
              return row['key']

        return RecordsIt(self, order)
