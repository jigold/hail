import json

from hailtop.batch_client.aioclient import Job

from .utils import cost_from_msec_mcpu
from .resources import cost_from_resources


class BatchFormatVersion:
    def __init__(self, format_version):
        self.format_version = format_version

    def has_full_spec_in_gcs(self):
        return self.format_version > 1

    def has_full_status_in_gcs(self):
        return self.format_version > 1

    def has_full_status_in_db(self):
        return self.format_version == 1

    def has_attempt_in_log_path(self):
        return self.format_version > 1

    def db_spec(self, spec):
        if self.format_version == 1:
            return spec

        secrets = spec.get('secrets')
        if secrets:
            secrets = [[secret['namespace'], secret['name'],
                        secret['mount_path'], int(secret.get('mount_in_copy', False))]
                       for secret in secrets]

        service_account = spec.get('service_account')
        if service_account:
            service_account = [service_account['namespace'], service_account['name']]

        return [
            secrets,
            service_account,
            int(len(spec.get('input_files', [])) > 0),
            int(len(spec.get('output_files', [])) > 0)
        ]

    def get_spec_secrets(self, spec):
        if self.format_version == 1:
            return spec.get('secrets')
        secrets = spec[0]
        if secrets:
            return [{'namespace': secret[0],
                     'name': secret[1],
                     'mount_path': secret[2],
                     'mount_in_copy': bool(secret[3])} for secret in secrets]
        return None

    def get_spec_service_account(self, spec):
        if self.format_version == 1:
            return spec.get('service_account')
        service_account = spec[1]
        if service_account:
            return {'namespace': service_account[0],
                    'name': service_account[1]}
        return None

    def get_spec_has_input_files(self, spec):
        if self.format_version == 1:
            return len(spec.get('input_files', [])) > 0
        return bool(spec[2])

    def get_spec_has_output_files(self, spec):
        if self.format_version == 1:
            return len(spec.get('output_files', [])) > 0
        return bool(spec[3])

    def db_status(self, status):
        if self.format_version == 1:
            return status

        job_status = {'status': status}
        ec = Job.exit_code(job_status)

        status_version = status.get('version', 1)
        if status_version == 1:
            duration = Job.total_duration_msecs(job_status)
        else:
            start_time = status.get('start_time')
            end_time = status.get('end_time')
            if start_time and end_time:
                duration = end_time - start_time
            else:
                duration = None

        return [ec, duration]

    def get_status_exit_code_duration(self, status):
        if self.format_version == 1:
            job_status = {'status': status}
            return (Job.exit_code(job_status), Job.total_duration_msecs(job_status))
        assert len(status) == 2
        return status

    def cost(self, msec_mcpu, resources):
        if self.format_version < 3:
            assert msec_mcpu is not None
            return cost_from_msec_mcpu(msec_mcpu)
        return cost_from_resources(resources)
