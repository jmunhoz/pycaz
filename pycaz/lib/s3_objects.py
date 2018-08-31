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

class S3Owner:
    def __init__(self, xid, display_name, id):
        self.xid = xid
        self.display_name = display_name
        self.id = id

    def __str__(self):
        return "{} - {} - {}".format(self.xid,
                                     self.display_name,
                                     self.id)

class S3Object:
    pass

class S3Version(S3Object):
    xid = 0
    last_modified = ''
    version_id = ''
    etag = ''
    storage_class = ''
    key = ''
    owner = None
    is_latest = True
    size = 0

    def __init__(self, xid, last_modified, version_id, etag, storage_class, key, owner, is_latest, size):
        self.xid = xid
        self.last_modified = last_modified
        self.version_id = version_id
        self.etag = etag
        self.storage_class = storage_class
        self.key = key
        self.owner = owner
        self.is_latest = is_latest
        self.size = size

    '''
        "Versions": [
        {
            "LastModified": "2018-06-22T10:55:36.186Z",
            "VersionId": "zWwD51VVELeSCN-mgn61yevKf4ETZB.",
            "ETag": "\"b9c85244be9733bc79eca588db7bf306\"",
            "StorageClass": "STANDARD",
            "Key": "test-key-1",
            "Owner": {
                "DisplayName": "Test User",
                "ID": "testuser1"
            },
            "IsLatest": true,
            "Size": 151024
        },
    '''
    def __str__(self):
        return "{}:{}:{}:{}:{}:{}:{}:{}:{}:{}:{}".format(self.xid,
                                                         self.last_modified,
                                                         self.version_id,
                                                         self.etag,
                                                         self.storage_class,
                                                         self.key,
                                                         self.owner.xid,
                                                         self.owner.display_name,
                                                         self.owner.id,
                                                         self.is_latest,
                                                         self.size)
    def pretty_str(self):
        return "{}:{}:{}:{}:{}:{}:{}:{}:{}:{}".format('V',
                                                      '+' if self.is_latest is 1 else '-',
                                                      self.last_modified,
                                                      self.version_id,
                                                      self.key,
                                                      self.owner.display_name,
                                                      self.owner.id,
                                                      self.size,
                                                      self.etag,
                                                      self.storage_class)

class S3DeleteMarker(S3Version):
    def __init__(self, xid, last_modified, version_id, key, owner, is_latest):
        self.xid = xid
        self.last_modified = last_modified
        self.version_id = version_id
        self.key = key
        self.owner = owner
        self.is_latest = is_latest

    '''
    "DeleteMarkers": [
        {
            "Owner": {
                "DisplayName": "Test User",
                "ID": "testuser1"
            },
            "IsLatest": true,
            "VersionId": "0eU0xYMb3G2UiZVji4l8jhX-nL1tXqm",
            "Key": "test-key-1",
            "LastModified": "2018-06-22T11:22:31.955Z"
        },
    '''
    def __str__(self):
        return "{}:{}:{}:{}:{}:{}:{}:{}".format(self.xid,
                                                self.last_modified,
                                                self.version_id,
                                                self.key,
                                                self.owner.xid,
                                                self.owner.display_name,
                                                self.owner.id,
                                                self.is_latest)

    def pretty_str(self):
        return "{}:{}:{}:{}:{}:{}:{}".format('D',
                                             '+' if self.is_latest else '-',
                                             self.last_modified,
                                             self.version_id,
                                             self.key,
                                             self.owner.display_name,
                                             self.owner.id)
