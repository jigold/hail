from google.cloud import storage
import re
import functools


def get_geometric_mean(prod_of_means, num_of_means):
    return prod_of_means ** (1.0 / num_of_means)


FILE_PATH_REGEX = re.compile(r'gs://((?P<bucket>[^/]+)/)(?P<path>.*)')


def parse_file_path(regex, name):
    match = regex.fullmatch(name)
    return match.groupdict()


def enumerate_list_of_trials(list_of_trials):
    trial_indices = []
    wall_times = []
    within_group_idx = []
    for count, trial in enumerate(list_of_trials):
        wall_times.extend(trial)
        within_group_idx.extend([f'{j+1}' for j in range(len(trial))])
        temp = [count] * len(trial)
        trial_indices.extend(temp)
    res_dict = {
        'trial_indices': trial_indices,
        'wall_times': wall_times,
        'within_group_index': within_group_idx
    }
    return res_dict


class ReadGoogleStorage:
    def __init__(self, service_account_key_file=None):
        self.storage_client = storage.Client.from_service_account_json(service_account_key_file)

    def get_data_as_string(self, file_path):
        file_info = parse_file_path(FILE_PATH_REGEX, file_path)
        bucket = self.storage_client.get_bucket(file_info['bucket'])
        shorter_file_path = file_info['path']
        try:
            # get bucket data as blob
            blob = bucket.blob(shorter_file_path)
            # convert to string
            data = blob.download_as_string()
        except Exception:
            raise NameError()
        return data

    @functools.lru_cache(maxsize=128)
    def list_files_in_bucket(self, bucket_name):
        list_of_files = []
        bucket = self.storage_client.get_bucket(bucket_name)
        for blob in bucket.list_blobs():
            list_of_files.append('gs://' + bucket_name + '/' + blob.name)
        return list_of_files

    def list_files(self):
        list_of_files = []
        list_of_files.extend(self.list_files_in_bucket('hail-benchmarks'))
        list_of_files.extend(self.list_files_in_bucket('hail-benchmarks-2'))
        return list_of_files
