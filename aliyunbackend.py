# -*- Mode:Python; indent-tabs-mode:nil; tab-width:4 -*-
#
# Maintainer Hugo <hugo@herrqin.com>
#
# Based on aliyunbackend.py (https://yq.aliyun.com/articles/60986)
# Based on b2backend.py (https://github.com/matthewbentley/duplicity_b2)
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
#
# The above copyright notice and this permission notice shall be included in
# all copies or substantial portions of the Software.
#
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.  IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
# THE SOFTWARE.

import os
import time
import hashlib
import base64

import duplicity.backend
from duplicity import globals
from duplicity import log
from duplicity.errors import FatalBackendException, BackendException
from duplicity import progress

class AliyunBackend(duplicity.backend.Backend):
    """
    Backend for Aliyun OSS Storage Serive
    """
    def __init__(self, parsed_url):
        duplicity.backend.Backend.__init__(self, parsed_url)

        # Import Aliyun OSS SDK for Python library.
        try:
            import oss2
        except ImportError:
            raise BackendException('Aliyun backend requires Aliyun OSS SDK for Python'
                                   'https://pypi.python.org/pypi/oss2')

        if 'ALIYUN_OSS_ENDPOINT' not in os.environ:
            raise BackendException('ALIYUN_OSS_ENDPOINT environment variable not set.')

        if 'ALIYUN_ACCESS_ID' not in os.environ:
            raise BackendException('ALIYUN_ACCESS_ID environment variable not set.')
            
        if 'ALIYUN_ACCESS_KEY' not in os.environ:
            raise BackendException('ALIYUN_ACCESS_KEY environment variable not set.')

        self.url_parts = [x for x in parsed_url.path.split('/') if x != '']

        if self.url_parts:
            self.bucket_name = self.url_parts.pop(0)
        else:
            raise BackendException('Aliyun OSS requires a bucket name.')

        self.endpoint = os.environ['ALIYUN_OSS_ENDPOINT']
        self.access_id = os.environ['ALIYUN_ACCESS_ID']
        self.access_key = os.environ['ALIYUN_ACCESS_KEY']

        self.scheme = parsed_url.scheme

        if self.url_parts:
            self.key_prefix = '%s/' % '/'.join(self.url_parts)
        else:
            self.key_prefix = ''

        self.straight_url = duplicity.backend.strip_auth_from_url(parsed_url)
        self.parsed_url = parsed_url

        self.resetConnection()
        self._listed_keys = {}

    def _close(self):
        del self._listed_keys
        self._listed_keys = {}
        self.bucket = None
        del self.bucket

    def resetConnection(self):
        import oss2
        self.bucket = None
        del self.bucket
        self.bucket = oss2.Bucket(oss2.Auth(self.access_id, self.access_key), self.endpoint, self.bucket_name)
        try:
            self.bucket.create_bucket("private")
        except Exception as e:
            log.FatalError("Could not create OSS bucket: %s"
                           % unicode(e.message).split('\n', 1)[0],
                           log.ErrorCode.connection_failed)

    def _retry_cleanup(self):
        self.resetConnection()

    def _put(self, source_path, remote_filename):
        if not self.bucket:
            raise BackendException("No connection to backend")

        log.Log("Putting file to %s" % remote_filename, 9)
        self._delete(remote_filename)
        digest = self.hex_md5_of_file(source_path)
        content_type = 'application/pgp-encrypted'
        remote_filename = self.full_filename(remote_filename)

        headers = {
            'Content-Type': content_type,
            'Content-MD5': digest,
            'Content-Length': str(os.path.getsize(source_path.name)),
        }
        upload_start = time.time()
        self.bucket.put_object_from_file(remote_filename, source_path.name, headers, progress_callback=None)
        upload_end = time.time()
        total_s = abs(upload_end - upload_start) or 1  # prevent a zero value!
        rough_upload_speed = os.path.getsize(source_path.name) / total_s
        log.Debug("Uploaded %s/%s at roughly %f bytes/second" %
                  (self.straight_url, remote_filename, rough_upload_speed))


    def _get(self, remote_filename, local_path):
        if not self.bucket:
            raise BackendException("No connection to backend")

        log.Log("Getting file %s" % remote_filename, 9)
        remote_filename = self.full_filename(remote_filename)
        self.bucket.get_object_to_file(remote_filename, local_path.name)
        
    def _list(self):
        if not self.bucket:
            raise BackendException("No connection to backend")

        return self.list_filenames_in_bucket()

    def list_filenames_in_bucket(self):
        filename_list = []
        marker = ""
        not_exausted=True
        while not_exausted:
            try:
                resp = self.bucket.list_objects(self.key_prefix, '/', marker)
                object_list, marker, not_exausted = resp.object_list, resp.next_marker, resp.is_truncated
                for k in object_list:
                    try:
                        filename = k.key.replace(self.key_prefix, '', 1)
                        filename_list.append(filename)
                        self._listed_keys[k.key] = k
                        log.Debug("Listed %s/%s" % (self.straight_url, filename))
                    except AttributeError:
                        log.Error("List AttributeError")
                        pass
                # if len(marker) == 0:
                #     break
            except Exception, e:
                log.Error("List bucket %s failed with %s" % (self.bucket_name, e))
                pass
        return filename_list

    def _delete(self, filename):
        if not self.bucket:
            raise BackendException("No connection to backend")
        self.bucket.delete_object(self.key_prefix + filename)

    def _query(self, filename):
        """
        Get size info of filename
        """
        log.Log("Querying file %s" % filename, 9)
        info = self.bucket.get_object_meta(self.full_filename(filename)).content_length
        if not info:
            return {'size': -1}

        return {'size': info}
        
    def full_filename(self, filename):
        if self.key_prefix:
            return self.key_prefix  + filename
        else:
            return filename

    @staticmethod
    def hex_md5_of_file(path):
        """
        Calculate the md5 of a file to upload
        """
        f = path.open()
        block_size = 1024 * 1024
        digest = hashlib.md5()
        while True:
            data = f.read(block_size)
            if len(data) == 0:
                break
            digest.update(data)
        f.close()
        return base64.b64encode(digest.digest())
        
    # Don't support query info
    #def _query(self, filename):

duplicity.backend.register_backend("oss", AliyunBackend)
duplicity.backend.register_backend("oss+http", AliyunBackend)
duplicity.backend.uses_netloc.extend(['oss'])
