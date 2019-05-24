import abc
import os
import subprocess as sp
import uuid
from shlex import quote as shq
import batch.client

from .resource import InputResourceFile, TaskResourceFile


class Backend:
    @abc.abstractmethod
    def _run(self, pipeline, dry_run, verbose, delete_scratch_on_exit):
        return


class LocalBackend(Backend):
    """
    Backend that executes pipelines on a local computer.

    Examples
    --------

    >>> local_backend = LocalBackend(tmp_dir='/tmp/user/')
    >>> p = Pipeline(backend=local_backend)

    Parameters
    ----------
    tmp_dir: :obj:`str`, optional
        Temporary directory to use.
    """

    def __init__(self, tmp_dir='/tmp/'):
        self._tmp_dir = tmp_dir

    def _run(self, pipeline, dry_run, verbose, delete_scratch_on_exit):  # pylint: disable=R0915
        tmpdir = self._get_scratch_dir()

        script = ['#!/bin/bash',
                  'set -e' + 'x' if verbose else '',
                  '\n',
                  '# change cd to tmp directory',
                  f"cd {tmpdir}",
                  '\n']

        copied_input_resource_files = set()
        os.makedirs(tmpdir + 'inputs/', exist_ok=True)

        def copy_input(task, r):
            if isinstance(r, InputResourceFile):
                if r not in copied_input_resource_files:
                    copied_input_resource_files.add(r)

                    if r._input_path.startswith('gs://'):
                        return [f'gsutil cp {r._input_path} {r._get_path(tmpdir)}']
                    else:
                        absolute_input_path = shq(os.path.realpath(r._input_path))
                        if task._image is not None:  # pylint: disable-msg=W0640
                            return [f'cp {absolute_input_path} {r._get_path(tmpdir)}']
                        else:
                            return [f'ln -sf {absolute_input_path} {r._get_path(tmpdir)}']
                else:
                    return []
            else:
                assert isinstance(r, TaskResourceFile)
                return []

        def copy_external_output(r):
            def cp(dest):
                if not dest.startswith('gs://'):
                    dest = os.path.abspath(dest)
                    directory = os.path.dirname(dest)
                    os.makedirs(directory, exist_ok=True)
                    return 'cp'
                else:
                    return 'gsutil cp'

            if isinstance(r, InputResourceFile):
                return [f'{cp(dest)} {shq(r._input_path)} {shq(dest)}'
                        for dest in r._output_paths]
            else:
                assert isinstance(r, TaskResourceFile)
                return [f'{cp(dest)} {r._get_path(tmpdir)} {shq(dest)}'
                        for dest in r._output_paths]

        write_inputs = [x for r in pipeline._input_resources for x in copy_external_output(r)]
        if write_inputs:
            script += ["# Write input resources to output destinations"]
            script += write_inputs
            script += ['\n']

        for task in pipeline._tasks:
            os.makedirs(tmpdir + task._uid + '/', exist_ok=True)

            script.append(f"# {task._uid} {task._label if task._label else ''}")

            script += [x for r in task._inputs for x in copy_input(task, r)]

            resource_defs = [r._declare(tmpdir) for r in task._mentioned]

            if task._image:
                defs = '; '.join(resource_defs) + '; ' if resource_defs else ''
                cmd = " && ".join(task._command)
                memory = f'-m {task._memory}' if task._memory else ''
                cpu = f'--cpus={task._cpu}' if task._cpu else ''

                script += [f"docker run "
                           f"-v {tmpdir}:{tmpdir} "
                           f"-w {tmpdir} "
                           f"{memory} "
                           f"{cpu} "
                           f"{task._image} /bin/bash "
                           f"-c {shq(defs + cmd)}",
                           '\n']
            else:
                script += resource_defs
                script += task._command

            script += [x for r in task._external_outputs for x in copy_external_output(r)]
            script += ['\n']

        script = "\n".join(script)
        if dry_run:
            print(script)
        else:
            try:
                sp.check_output(script, shell=True)
            except sp.CalledProcessError as e:
                print(e.output)
                raise e
            finally:
                if delete_scratch_on_exit:
                    sp.run(f'rm -rf {tmpdir}', shell=True)

    def _get_scratch_dir(self):
        def _get_random_name():
            directory = self._tmp_dir + '/pipeline-{}/'.format(uuid.uuid4().hex[:12])

            if os.path.isdir(directory):
                return _get_random_name()
            else:
                os.makedirs(directory, exist_ok=True)
                return directory

        return _get_random_name()


class BatchBackend(Backend):
    """
    Backend that executes pipelines on a Kubernetes cluster using `batch`.

    Examples
    --------

    >>> batch_backend = BatchBackend(tmp_dir='http://localhost:5000')
    >>> p = Pipeline(backend=batch_backend)

    Parameters
    ----------
    url: :obj:`str`
        URL to batch server.
    """

    def __init__(self, url):
        self._batch_client = batch.client.BatchClient(url)

    def _run(self, pipeline, dry_run, verbose, delete_scratch_on_exit):  # pylint: disable-msg=R0915
        if dry_run:
            raise NotImplementedError

        bucket = self._batch_client.bucket
        subdir_name = 'pipeline-{}'.format(uuid.uuid4().hex[:12])

        remote_tmpdir = f'gs://{bucket}/pipeline/{subdir_name}'
        local_tmpdir = f'/io/pipeline/{subdir_name}'

        default_image = 'ubuntu'

        batch = self._batch_client.create_batch()
        n_jobs_submitted = 0
        used_remote_tmpdir = False

        task_to_job_mapping = {}
        job_id_to_command = {}

        activate_service_account = 'set -ex; gcloud -q auth activate-service-account ' \
                                   '--key-file=/gsa-key/privateKeyData'

        def copy_input(r):
            if isinstance(r, InputResourceFile):
                return [(r._input_path, r._get_path(local_tmpdir))]
            else:
                assert isinstance(r, TaskResourceFile)
                return [(r._get_path(remote_tmpdir), r._get_path(local_tmpdir))]

        def copy_internal_output(r):
            assert isinstance(r, TaskResourceFile)
            return [(r._get_path(local_tmpdir), r._get_path(remote_tmpdir))]

        def copy_external_output(r):
            if isinstance(r, InputResourceFile):
                return [(r._input_path, dest) for dest in r._output_paths]
            else:
                assert isinstance(r, TaskResourceFile)
                return [(r._get_path(local_tmpdir), dest) for dest in r._output_paths]

        write_external_inputs = [x for r in pipeline._input_resources for x in copy_external_output(r)]
        if write_external_inputs:
            def _cp(src, dst):
                return f'gsutil -m cp -R {src} {dst}'

            write_cmd = activate_service_account + ' && ' + \
                        ' && '.join([_cp(*files) for files in write_external_inputs])

            j = batch.create_job(image='google/cloud-sdk:237.0.0-alpine',
                                 command=['/bin/bash', '-c', write_cmd],
                                 attributes={'label': 'write_external_inputs'})
            job_id_to_command[j.id] = write_cmd
            n_jobs_submitted += 1
            if verbose:
                print(f"Submitted Job {j.id} with command: {write_cmd}")

        for task in pipeline._tasks:
            inputs = [x for r in task._inputs for x in copy_input(r)]

            outputs = [x for r in task._internal_outputs for x in copy_internal_output(r)]
            if outputs:
                used_remote_tmpdir = True
            outputs += [x for r in task._external_outputs for x in copy_external_output(r)]

            resource_defs = [r._declare(directory=local_tmpdir) for r in task._mentioned]

            if task._image is None:
                if verbose:
                    print(f"Using image '{default_image}' since no image was specified.")

            make_local_tmpdir = f'mkdir -p {local_tmpdir}/{task._uid}/; '
            defs = '; '.join(resource_defs) + '; ' if resource_defs else ''
            task_command = [cmd.strip() for cmd in task._command]

            cmd = " && ".join(task_command)
            parent_ids = [task_to_job_mapping[t].id for t in task._dependencies]

            attributes = {'task_uid': task._uid}
            if task._label:
                attributes['label'] = task._label

            resources = {'requests': {}}
            if task._cpu:
                resources['requests']['cpu'] = task._cpu
            if task._memory:
                resources['requests']['memory'] = task._memory

            j = batch.create_job(image=task._image if task._image else default_image,
                                 command=['/bin/bash', '-c', make_local_tmpdir + defs + cmd],
                                 parent_ids=parent_ids,
                                 attributes=attributes,
                                 resources=resources,
                                 input_files=inputs if len(inputs) > 0 else None,
                                 output_files=outputs if len(outputs) > 0 else None)
            n_jobs_submitted += 1

            task_to_job_mapping[task] = j
            job_id_to_command[j.id] = defs + cmd
            if verbose:
                print(f"Submitted Job {j.id} with command: {defs + cmd}")

        if delete_scratch_on_exit and used_remote_tmpdir:
            parent_ids = list(job_id_to_command.keys())
            rm_cmd = f'gsutil rm -r {remote_tmpdir}'
            cmd = f'{activate_service_account} && {rm_cmd}'
            j = batch.create_job(
                image='google/cloud-sdk:237.0.0-alpine',
                command=['/bin/bash', '-c', cmd],
                parent_ids=parent_ids,
                attributes={'label': 'remove_tmpdir'},
                always_run=True)
            job_id_to_command[j.id] = cmd
            n_jobs_submitted += 1

        batch.close()
        status = batch.wait()

        failed_jobs = [(j['job_id'], j['exit_code']) for j in status['jobs'] if 'exit_code' in j and any([ec != 0 for _, ec in j['exit_code'].items()])]

        fail_msg = ''
        for jid, ec in failed_jobs:
            job = self._batch_client.get_job(batch.id, jid)
            log = job.log()
            label = job.status()['attributes'].get('label', None)
            fail_msg += (
                f"Job {jid} failed with exit code {ec}:\n"
                f"  Task label:\t{label}\n"
                f"  Command:\t{job_id_to_command[jid]}\n"
                f"  Log:\t{log}\n")

        n_complete = sum([j['state'] == 'Complete' for j in status['jobs']])
        if failed_jobs or n_complete != n_jobs_submitted:
            raise Exception(fail_msg)

        print("Pipeline completed successfully!")
