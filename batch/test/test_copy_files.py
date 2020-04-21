import os
import unittest
import tempfile
import subprocess as sp
import uuid
import random
import string
import time
from shlex import quote as shq
import google.oauth2.service_account
from hailtop.auth import get_userinfo
from hailtop.utils.os import _glob

from batch.google_storage import GCS

# key_file = '/gsa-key/key.json'
# project = os.environ['PROJECT']
# user = get_userinfo()
# tmp_bucket = f'gs://{user["bucket_name"]}/test_copy_files'

key_file = '/Users/jigold/.hail/key.json'
project = 'hail-vdc'
tmp_bucket = f'gs://hail-jigold-59hi5/test_copy_files'

credentials = google.oauth2.service_account.Credentials.from_service_account_file(key_file)
gcs_client = GCS(None, project=project, credentials=credentials)


class RemoteTemporaryDirectory:
    def __init__(self):
        token = uuid.uuid4().hex[:6]
        self.name = tmp_bucket + f'/{token}'

    def __enter__(self):
        return self.name

    def __exit__(self, exc_type, exc_val, exc_tb):
        with Timer('remove remote temporary directory'):
            remove_remote_dir(self.name)


class Timer:
    def __init__(self, desc=None):
        self.start = None
        self.desc = desc

    def __enter__(self):
        self.start = time.time()

    def __exit__(self, exc_type, exc_val, exc_tb):
        print(f'took {self.desc} {time.time() - self.start}s')


def upload_files(src, dest):
    sp.check_output(['gsutil', '-m', '-q', 'cp', '-r', src, dest])


def move_files(src, dest):
    sp.check_output(['gsutil', '-m', '-q', 'mv', '-r', src, dest])


def remove_remote_dir(path):
    path = path.rstrip('/') + '/'
    sp.run(['gsutil', '-m', '-q', 'rm', '-r', path], stdout=sp.PIPE, stderr=sp.PIPE)


def touch_file(path, data=None):
    dir = os.path.dirname(path)
    if not os.path.isdir(dir):
        os.makedirs(dir, exist_ok=True)

    with open(path, 'w') as file:
        if data:
            file.write(data)


def cp_batch(src, dest, parallelism=1, min_partition_size='1Gi',
             max_upload_partitions=32, max_download_partitions=32):
    cmd = ['python3', '-u', '-m', 'batch.worker.copy_files',
           '--key-file', key_file,
           '--project', project,
           '--parallelism', str(parallelism),
           '--min-partition-size', min_partition_size,
           '--max-upload-partitions', str(max_upload_partitions),
           '--max-download-partitions', str(max_download_partitions),
           '-f', src, dest]
    print(' '.join(cmd))
    with Timer('batch sp'):
        result = sp.run(cmd, stdout=sp.PIPE, stderr=sp.STDOUT)
    return str(result.stdout), result.returncode


def cp_gsutil(src, dest):
    cmd = ['gsutil', '-m', '-q', 'cp', '-r', src, dest]
    print(' '.join(cmd))
    with Timer('gsutil sp'):
        result = sp.run(cmd, stdout=sp.PIPE, stderr=sp.STDOUT)
    return str(result.stdout), result.returncode


def glob_local_files(dir):
    files = _glob(dir, recursive=True)
    return {f.replace(dir, '') for f, _ in files}


def glob_remote_files(dir):
    blobs = gcs_client._glob_gs_files(dir, recursive=True)
    files = {'gs://' + blob.bucket.name + '/' + blob.name for blob in blobs}
    return {f.replace(dir, '') for f in files}


def _copy_to_local(src, dest):
    with Timer('copy batch with local temp dir'):
        with tempfile.TemporaryDirectory() as batch_dest_dir:
            batch_output, batch_rc = cp_batch(src, batch_dest_dir + dest)
            with Timer('glob local files'):
                batch_files = glob_local_files(batch_dest_dir)

    with Timer('copy gsutil with local temp dir'):
        with tempfile.TemporaryDirectory() as gsutil_dest_dir:
            gsutil_output, gsutil_rc = cp_gsutil(src, gsutil_dest_dir + dest)
            with Timer('glob local files'):
                gsutil_files = glob_local_files(gsutil_dest_dir)

    return {
        'batch': {'files': batch_files, 'output': batch_output, 'rc': batch_rc},
        'gsutil': {'files': gsutil_files, 'output': gsutil_output, 'rc': gsutil_rc}
    }


def _copy_to_remote(src, dest):
    with Timer('copy batch with remote temp dir'):
        with RemoteTemporaryDirectory() as batch_dest_dir:
            batch_output, batch_rc = cp_batch(src, batch_dest_dir + dest)
            with Timer('glob remote files'):
                batch_files = glob_remote_files(batch_dest_dir)

    with Timer('copy gsutil with remote temp dir'):
        with RemoteTemporaryDirectory() as gsutil_dest_dir:
            gsutil_output, gsutil_rc = cp_gsutil(src, gsutil_dest_dir + dest)
            with Timer('glob remote files'):
                gsutil_files = glob_remote_files(gsutil_dest_dir)

    return {
        'batch': {'files': batch_files, 'output': batch_output, 'rc': batch_rc},
        'gsutil': {'files': gsutil_files, 'output': gsutil_output, 'rc': gsutil_rc}
    }


def _run_copy_step(cp, src, dest):
    result = cp(src, dest)
    batch = result['batch']
    gsutil = result['gsutil']
    batch_error = batch['rc'] != 0
    gsutil_error = gsutil['rc'] != 0
    success = batch['files'] == gsutil['files'] and batch_error == gsutil_error
    return {'success': success, 'result': result}


def run_local_to_remote(src, dest):
    assert not src.startswith('gs://')
    return _run_copy_step(_copy_to_remote, src, dest)


def run_remote_to_local(src, dest):
    assert src.startswith('gs://')
    return _run_copy_step(_copy_to_local, src, dest)


def run_local_to_local(src, dest):
    assert not src.startswith('gs://')
    return _run_copy_step(_copy_to_local, src, dest)


def run_remote_to_remote(src, dest):
    assert src.startswith('gs://')
    return _run_copy_step(_copy_to_remote, src, dest)


def _run_batch_same_as_gsutil(local_dir, remote_dir, src, dest):
    return {
        'rr': run_remote_to_remote(f'{remote_dir}{src}', dest),
        'rl': run_remote_to_local(f'{remote_dir}{src}', dest),
        'lr': run_local_to_remote(f'{local_dir}{src}', dest),
        'll': run_local_to_local(f'{local_dir}{src}', dest)
    }


def _get_output(result, version, method):
    return result[version]['result'][method]['output']


def _get_files(result, version, method):
    return result[version]['result'][method]['files']


def _get_rc(result, version, method):
    return result[version]['result'][method]['rc']


def _get_batch_gsutil_files_same(result):
    return [result[t]['success'] for t in result]


class TestEmptyDirectory(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.local_dir = tempfile.TemporaryDirectory()

    @classmethod
    def tearDownClass(cls):
        cls.local_dir.cleanup()

    def assert_batch_same_as_gsutil(self, src, dest):
        result = {'ll': run_local_to_local(f'{self.local_dir.name}/', dest),
                  'lr': run_local_to_remote(f'{self.local_dir.name}/', dest)}
        assert all(_get_batch_gsutil_files_same(result)), str(result)
        return result

    def test_download_directory(self):
        r = self.assert_batch_same_as_gsutil('/', '/')
        assert 'FileNotFoundError' in _get_output(r, 'll', 'batch'), _get_output(r, 'll', 'batch')
        assert 'FileNotFoundError' in _get_output(r, 'lr', 'batch'), _get_output(r, 'lr', 'batch')

    def test_download_asterisk(self):
        r = self.assert_batch_same_as_gsutil('/*', '/')
        assert 'FileNotFoundError' in _get_output(r, 'll', 'batch'), _get_output(r, 'll', 'batch')
        assert 'FileNotFoundError' in _get_output(r, 'lr', 'batch'), _get_output(r, 'lr', 'batch')

    def test_download_double_asterisk(self):
        result = run_local_to_local(f'{self.local_dir.name}/**', '/')
        assert result['success'] != 0, str(result)
        assert '** not supported' in result['result']['batch']['output'], result['result']['batch']['output']


class TestSingleFileTopLevel(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.local_dir = tempfile.TemporaryDirectory()
        touch_file(cls.local_dir.name + '/data/a')
        token = uuid.uuid4().hex[:6]
        cls.remote_dir = f'{tmp_bucket}/{token}'
        upload_files(cls.local_dir.name, cls.remote_dir)

    @classmethod
    def tearDownClass(cls):
        cls.local_dir.cleanup()
        remove_remote_dir(cls.remote_dir)

    def assert_batch_same_as_gsutil(self, src, dest):
        result = _run_batch_same_as_gsutil(self.local_dir.name, self.remote_dir, src, dest)
        assert all(_get_batch_gsutil_files_same(result)), str(result)
        print(result)
        return result

    def test_download_file_by_name(self):
        self.assert_batch_same_as_gsutil('/data/a', '/')

    def test_download_file_not_exists(self):
        result = self.assert_batch_same_as_gsutil('/data/b', '/')
        assert 'FileNotFoundError' in _get_output(result, 'rr', 'batch'), _get_output(result, 'rr', 'batch')
        assert 'FileNotFoundError' in _get_output(result, 'rl', 'batch'), _get_output(result, 'rl', 'batch')
        assert 'FileNotFoundError' in _get_output(result, 'lr', 'batch'), _get_output(result, 'lr', 'batch')
        assert 'FileNotFoundError' in _get_output(result, 'll', 'batch'), _get_output(result, 'll', 'batch')

    def test_download_file_by_name_with_slash(self):
        self.assert_batch_same_as_gsutil('/data/a/', '/')

    def test_download_directory(self):
        self.assert_batch_same_as_gsutil('/data/', '/')

    def test_download_directory_without_slash(self):
        self.assert_batch_same_as_gsutil('/data', '')

    def test_download_single_wildcard(self):
        self.assert_batch_same_as_gsutil('/data/*', '/')

    def test_download_multiple_wildcards(self):
        self.assert_batch_same_as_gsutil('/*/*', '/')

    def test_download_top_level_directory(self):
        self.assert_batch_same_as_gsutil('/', '/')

    def test_download_file_by_name_with_rename(self):
        self.assert_batch_same_as_gsutil('/data/a', '/b')

    def test_download_file_by_wildcard_with_rename(self):
        self.assert_batch_same_as_gsutil('/data/*', '/b')

    def test_download_file_by_name_to_nonexistent_subdir(self):
        self.assert_batch_same_as_gsutil('/data/a', '/foo/b')

    def test_download_file_by_wildcard_to_nonexistent_subdir(self):
        self.assert_batch_same_as_gsutil('/data/*', '/foo/b')


class TestFileNestedInMultipleSubdirs(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.local_dir = tempfile.TemporaryDirectory()
        touch_file(cls.local_dir.name + '/data/a/b/c')
        token = uuid.uuid4().hex[:6]
        cls.remote_dir = f'{tmp_bucket}/{token}'
        upload_files(cls.local_dir.name, cls.remote_dir)

    @classmethod
    def tearDownClass(cls):
        cls.local_dir.cleanup()
        remove_remote_dir(cls.remote_dir)

    def assert_batch_same_as_gsutil(self, src, dest):
        result = _run_batch_same_as_gsutil(self.local_dir.name, self.remote_dir, src, dest)
        assert all(_get_batch_gsutil_files_same(result)), str(result)
        return result

    def test_download_file_by_name(self):
        self.assert_batch_same_as_gsutil('/data/a/b/c', '/')

    def test_download_file_by_name_with_rename(self):
        self.assert_batch_same_as_gsutil('/data/a/b/c', '/foo')

    def test_download_directory_recursively(self):
        self.assert_batch_same_as_gsutil('/data/', '/')

    def test_download_wildcard_subdir_without_slash(self):
        self.assert_batch_same_as_gsutil('/data/*/b', '/')

    def test_download_wildcard_subdir_with_slash(self):
        self.assert_batch_same_as_gsutil('/data/*/b/', '/')

    def test_download_double_wildcards(self):
        self.assert_batch_same_as_gsutil('/data/*/*/', '/')

    def test_download_double_wildcards_plus_file_wildcard(self):
        self.assert_batch_same_as_gsutil('/data/*/*/*', '/')


class TestDownloadMultipleFilesAtTopLevel(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.local_dir = tempfile.TemporaryDirectory()
        touch_file(cls.local_dir.name + '/data/a')
        touch_file(cls.local_dir.name + '/data/b')
        touch_file(cls.local_dir.name + '/data/c')
        token = uuid.uuid4().hex[:6]
        cls.remote_dir = f'{tmp_bucket}/{token}'
        upload_files(cls.local_dir.name, cls.remote_dir)

    @classmethod
    def tearDownClass(cls):
        cls.local_dir.cleanup()
        remove_remote_dir(cls.remote_dir)

    def assert_batch_same_as_gsutil(self, src, dest):
        result = _run_batch_same_as_gsutil(self.local_dir.name, self.remote_dir, src, dest)
        assert all(_get_batch_gsutil_files_same(result)), str(result)
        return result

    def test_download_file_by_name(self):
        self.assert_batch_same_as_gsutil('/data/a', '/')

    def test_download_file_with_rename(self):
        self.assert_batch_same_as_gsutil('/data/a', '/b')

    def test_download_file_asterisk(self):
        self.assert_batch_same_as_gsutil('/data/*', '/')

    def test_download_file_match_brackets(self):
        self.assert_batch_same_as_gsutil('/data/[ab]', '/b')

    def test_download_file_question_mark(self):
        self.assert_batch_same_as_gsutil('/data/?', '/')

    def test_download_file_double_question_marks(self):
        result = self.assert_batch_same_as_gsutil('/data/??', '/')
        assert 'FileNotFoundError' in _get_output(result, 'rr', 'batch'), _get_output(result, 'rr', 'batch')
        assert 'FileNotFoundError' in _get_output(result, 'rl', 'batch'), _get_output(result, 'rl', 'batch')
        assert 'FileNotFoundError' in _get_output(result, 'lr', 'batch'), _get_output(result, 'lr', 'batch')
        assert 'FileNotFoundError' in _get_output(result, 'll', 'batch'), _get_output(result, 'll', 'batch')

    def test_download_multiple_files_to_single_file(self):
        result = self.assert_batch_same_as_gsutil('/data/[ab]', '/b')
        assert 'NotADirectoryError' in _get_output(result, 'rl', 'batch'), _get_output(result, 'rl', 'batch')
        assert 'NotADirectoryError' in _get_output(result, 'll', 'batch'), _get_output(result, 'll', 'batch')

    def test_download_file_invalid_dest_path_with_slash(self):
        result = self.assert_batch_same_as_gsutil('/data/a', '/b/')
        assert 'skipping destination file ending with slash' in _get_output(result, 'rl', 'batch'), _get_output(result, 'rl', 'batch')

        # it's unclear why gsutil doesn't just create the directory like it does if the destination is remote
        # it's also unclear why you don't get the same error as for the remote->local case
        assert 'IsADirectoryError' in _get_output(result, 'll', 'batch'), _get_output(result, 'll', 'batch')

    def test_download_file_invalid_dest_dir_with_wildcard(self):
        result = self.assert_batch_same_as_gsutil('/data/*', '/b/')

        assert 'destination must name a directory when matching multiple files' in _get_output(result, 'rl', 'batch'), _get_output(result, 'rl', 'batch')
        assert 'NotADirectoryError' in _get_output(result, 'rl', 'batch'), _get_output(result, 'rl', 'batch')

        assert 'destination must name a directory when matching multiple files' in _get_output(result, 'll', 'batch'), _get_output(result, 'll', 'batch')
        assert 'NotADirectoryError' in _get_output(result, 'll', 'batch'), _get_output(result, 'll', 'batch')


class TestDownloadFileWithEscapedWildcards(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.local_dir = tempfile.TemporaryDirectory()
        touch_file(cls.local_dir.name + '/data/foo/bar/dog/a')
        touch_file(cls.local_dir.name + '/data/foo/baz/dog/h*llo')
        touch_file(cls.local_dir.name + '/data/foo/b?r/dog/b')
        token = uuid.uuid4().hex[:6]
        cls.remote_dir = f'{tmp_bucket}/{token}'
        upload_files(cls.local_dir.name, cls.remote_dir)

    @classmethod
    def tearDownClass(cls):
        cls.local_dir.cleanup()
        remove_remote_dir(cls.remote_dir)

    def assert_batch_same_as_gsutil(self, src, dest):
        result = _run_batch_same_as_gsutil(self.local_dir.name, self.remote_dir, src, dest)
        assert all(_get_batch_gsutil_files_same(result)), str(result)
        return result

    def test_download_all_files_recursively(self):
        self.assert_batch_same_as_gsutil('/data/foo/', '/')

    def test_download_directory_with_escaped_question_mark(self):
        # gsutil does not have a mechanism for copying a path with an escaped wildcard
        # CommandException: No URLs matched: /var/folders/f_/ystbcjb13z78n85cyz6_jpl9sbv79d/T/tmpz9l9v9tf/data/foo/b\?r/dog/ CommandException: 1 file/object could not be transferred.
        expected = {'/dog/b'}

        result = _run_batch_same_as_gsutil(self.local_dir.name, self.remote_dir, '/data/foo/b\\?r/dog/', '/')
        assert not all(_get_batch_gsutil_files_same(result)), str(result)

        assert _get_files(result, 'rr', 'batch') == expected, _get_output(result, 'rr', 'batch')
        assert _get_rc(result, 'rr', 'batch') == 0, _get_rc(result, 'rr', 'batch')

        assert _get_files(result, 'rl', 'batch') == expected, _get_output(result, 'rl', 'batch')
        assert _get_rc(result, 'rl', 'batch') == 0, _get_rc(result, 'rl', 'batch')

        assert _get_files(result, 'lr', 'batch') == expected, _get_output(result, 'lr', 'batch')
        assert _get_rc(result, 'lr', 'batch') == 0, _get_rc(result, 'lr', 'batch')

        assert _get_files(result, 'll', 'batch') == expected, _get_output(result, 'll', 'batch')
        assert _get_rc(result, 'll', 'batch') == 0, _get_rc(result, 'll', 'batch')

    def test_download_directory_with_nonescaped_question_mark(self):
        # gsutil refuses to copy a path with a wildcard in it
        # Cloud folder gs://hail-jigold-59hi5/testing-suite/9f347b/data/foo/b\?r/ contains a wildcard; gsutil does not currently support objects with wildcards in their name.
        expected = {'/dog/a', '/dog/b'}

        result = _run_batch_same_as_gsutil(self.local_dir.name, self.remote_dir, '/data/foo/b?r/dog/', '/')
        assert not all(_get_batch_gsutil_files_same(result)), str(result)

        assert _get_files(result, 'rr', 'batch') == expected, _get_output(result, 'rr', 'batch')
        assert _get_rc(result, 'rr', 'batch') == 0, _get_rc(result, 'rr', 'batch')

        assert _get_files(result, 'rl', 'batch') == expected, _get_output(result, 'rl', 'batch')
        assert _get_rc(result, 'rl', 'batch') == 0, _get_rc(result, 'rl', 'batch')

        assert _get_files(result, 'lr', 'batch') == expected, _get_output(result, 'lr', 'batch')
        assert _get_rc(result, 'lr', 'batch') == 0, _get_rc(result, 'lr', 'batch')

        assert _get_files(result, 'll', 'batch') == expected, _get_output(result, 'll', 'batch')
        assert _get_rc(result, 'll', 'batch') == 0, _get_rc(result, 'll', 'batch')


class TestDownloadFileWithSpaces(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.local_dir = tempfile.TemporaryDirectory()
        touch_file(cls.local_dir.name + '/data/foo/bar/dog/file with spaces.txt')
        touch_file(cls.local_dir.name + '/data/f o o/hello')
        token = uuid.uuid4().hex[:6]
        cls.remote_dir = f'{tmp_bucket}/{token}'
        upload_files(cls.local_dir.name, cls.remote_dir)

    @classmethod
    def tearDownClass(cls):
        cls.local_dir.cleanup()
        remove_remote_dir(cls.remote_dir)

    def assert_batch_same_as_gsutil(self, src, dest):
        result = _run_batch_same_as_gsutil(self.local_dir.name, self.remote_dir, src, dest)
        assert all(_get_batch_gsutil_files_same(result)), str(result)

    def test_download_file_with_spaces(self):
        self.assert_batch_same_as_gsutil('/data/foo/bar/dog/file with spaces.txt', '/')

    def test_directory_with_spaces(self):
        self.assert_batch_same_as_gsutil('/data/f o o/hello', '/')


class TestDownloadComplicatedDirectory(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.local_dir = tempfile.TemporaryDirectory()
        touch_file(cls.local_dir.name + '/data/foo/a/data1')
        touch_file(cls.local_dir.name + '/data/bar/a')
        touch_file(cls.local_dir.name + '/data/baz')
        touch_file(cls.local_dir.name + '/data/dog/dog/dog')
        token = uuid.uuid4().hex[:6]
        cls.remote_dir = f'{tmp_bucket}/{token}'
        upload_files(cls.local_dir.name, cls.remote_dir)

    @classmethod
    def tearDownClass(cls):
        cls.local_dir.cleanup()
        remove_remote_dir(cls.remote_dir)

    def assert_batch_same_as_gsutil(self, src, dest):
        result = _run_batch_same_as_gsutil(self.local_dir.name, self.remote_dir, src, dest)
        assert all(_get_batch_gsutil_files_same(result)), str(result)

    def test_download_all_files(self):
        self.assert_batch_same_as_gsutil('/data/', '/')

    def test_download_all_files_without_slash(self):
        self.assert_batch_same_as_gsutil('/data', '/')


class TestNonEmptyFile(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.local_dir = tempfile.TemporaryDirectory()
        cls.data = ''.join([random.choice(string.ascii_letters) for _ in range(16 * 1024)])

        with open(f'{cls.local_dir.name}/data', 'w') as f:
            f.write(cls.data)

        token = uuid.uuid4().hex[:6]
        cls.remote_dir = f'{tmp_bucket}/{token}'
        upload_files(cls.local_dir.name, cls.remote_dir)

    @classmethod
    def tearDownClass(cls):
        cls.local_dir.cleanup()
        remove_remote_dir(cls.remote_dir)

    def test_download_multiple_partitions(self):
        with tempfile.TemporaryDirectory() as dest_dir:
            output, _ = cp_batch(f'{self.remote_dir}/data', f'{dest_dir}/data', parallelism=4, min_partition_size='4Ki')
            with open(f'{dest_dir}/data', 'r') as f:
                assert f.read() == self.data, output

    def test_upload_multiple_partitions(self):
        with RemoteTemporaryDirectory() as remote_dest_dir:
            with tempfile.TemporaryDirectory() as local_dest_dir:
                output, _ = cp_batch(f'{self.local_dir.name}/data', f'{remote_dest_dir}/data', parallelism=4, min_partition_size='4Ki')
                cp_batch(f'{remote_dest_dir}/data', f'{local_dest_dir}/data')
                with open(f'{local_dest_dir}/data', 'r') as f:
                    assert f.read() == self.data, output


class TestDownloadFileDirectoryWithSameName(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.local_dir = tempfile.TemporaryDirectory()
        touch_file(cls.local_dir.name + '/data/foo/a')
        token = uuid.uuid4().hex[:6]
        cls.remote_dir = f'{tmp_bucket}/{token}'
        upload_files(cls.local_dir.name, cls.remote_dir)
        upload_files(cls.remote_dir + '/data/foo/a', cls.remote_dir + '/data/foo/a/b')

    @classmethod
    def tearDownClass(cls):
        cls.local_dir.cleanup()
        remove_remote_dir(cls.remote_dir)

    def assert_batch_same_as_gsutil(self, src, dest):
        result = _run_batch_same_as_gsutil(self.local_dir.name, self.remote_dir, src, dest)
        assert all(_get_batch_gsutil_files_same(result)), str(result)
        return result

    def test_download_file_by_name(self):
        self.assert_batch_same_as_gsutil('/data/foo/a', '/')

    def test_download_file_by_name_in_subdir(self):
        self.assert_batch_same_as_gsutil('/data/foo/a/b', '/')

    def test_download_directory_with_same_name_as_file(self):
        src = '/data/foo/a/'
        dest = '/'

        rr = run_remote_to_remote(f'{self.remote_dir}{src}', dest)
        rl = run_remote_to_local(f'{self.remote_dir}{src}', dest)

        assert rr['success'] and not rl['success']

        rl_batch_output = rl['result']['batch']['output']
        assert 'NotADirectory' in rl_batch_output or 'FileExistsError' in rl_batch_output, rl_batch_output

    def test_download_file_with_wildcard(self):
        src = '/data/*/a'
        dest = '/'

        rr = run_remote_to_remote(f'{self.remote_dir}{src}', dest)
        rl = run_remote_to_local(f'{self.remote_dir}{src}', dest)

        assert rr['success'] and not rl['success']

        rl_batch_output = rl['result']['batch']['output']
        assert 'NotADirectory' in rl_batch_output or 'FileExistsError' in rl_batch_output, rl_batch_output


class TestEmptyFileSlashWithSameNameAsDirectory(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        token = uuid.uuid4().hex[:6]
        cls.remote_dir = f'{tmp_bucket}/{token}'
        gcs_client._write_gs_file_from_string(f'{cls.remote_dir}/data/bar/', 'bar')
        gcs_client._write_gs_file_from_string(f'{cls.remote_dir}/data/foo/', '')
        gcs_client._write_gs_file_from_string(f'{cls.remote_dir}/data/foo/a', 'a')
        gcs_client._write_gs_file_from_string(f'{cls.remote_dir}/data/foo/b', 'b')

    @classmethod
    def tearDownClass(cls):
        remove_remote_dir(cls.remote_dir)

    def assert_batch_same_as_gsutil(self, src, dest):
        result = {'rr': run_remote_to_remote(f'{self.remote_dir}/data{src}', dest),
                  'rl': run_remote_to_local(f'{self.remote_dir}/data{src}', dest)}
        assert all(_get_batch_gsutil_files_same(result)), str(result)
        print(result)
        return result

    def test_download_single_file_with_slash(self):
        self.assert_batch_same_as_gsutil('/foo/', '/')

    def test_download_single_file_without_slash(self):
        self.assert_batch_same_as_gsutil('/foo/a', '/')

    def test_download_directory(self):
        # gsutil doesn't copy files that end in a slash
        # https://github.com/GoogleCloudPlatform/gsutil/issues/444
        expected = {'/data/foo/', '/data/foo/a', '/data/foo/b', '/data/bar/'}

        result = {'rr': run_remote_to_remote(f'{self.remote_dir}/data/', '/'),
                  'rl': run_remote_to_local(f'{self.remote_dir}/data/', '/')}

        assert not result['rr']['success'], str(result['rr'])
        assert _get_files(result, 'rr', 'batch') == expected, str(result['rr'])
        assert _get_rc(result, 'rr', 'batch') == 0, str(result['rr'])

        # We refuse to copy all the files because the file ends in a slash
        # gsutil ignores it with return code 0
        assert result['rl']['success'], str(result['rl'])
        assert _get_files(result, 'rl', 'batch') == {}, str(result['rl'])
        assert _get_rc(result, 'rl', 'batch') != 0, str(result['rl'])

    def test_download_directory_partial_name(self):
        result = run_remote_to_remote(f'{self.remote_dir}/foo', '/')
        assert result['success'], str(result)

    def test_download_wildcard(self):
        result = run_remote_to_remote(f'{self.remote_dir}/f*', '/')
        assert result['success'], str(result)
