import os
import re
import warnings
from typing import Optional, Dict, Union, List, Any, Set

from hailtop.utils import secret_alnum_string

from . import backend as _backend, job, resource as _resource  # pylint: disable=cyclic-import
from .exceptions import BatchException


class Batch:
    """
    Object representing the distributed acyclic graph (DAG) of jobs to run.

    Examples
    --------
    Create a batch object:

    >>> p = Batch()

    Create a new job that prints "hello":

    >>> t = p.new_job()
    >>> t.command(f'echo "hello" ')

    Execute the DAG:

    >>> p.run()

    Notes
    -----

    The methods :meth:`.Batch.read_input` and :meth:`.Batch.read_input_group`
    are for adding input files to a batch. An input file is a file that already
    exists before executing a batch and is not present in the docker container
    the job is being run in.

    Files generated by executing a job are temporary files and must be written
    to a permanent location using the method :meth:`.Batch.write_output`.

    Parameters
    ----------
    name:
        Name of the batch.
    backend:
        Backend used to execute the jobs. Default is :class:`.LocalBackend`.
    attributes:
        Key-value pairs of additional attributes. 'name' is not a valid keyword.
        Use the name argument instead.
    requester_pays_project:
        The name of the Google project to be billed when accessing requester pays buckets.
    default_image:
        Docker image to use by default if not specified by a job.
    default_memory:
        Memory setting to use by default if not specified by a job. Only
        applicable if a docker image is specified for the :class:`.LocalBackend`
        or the :class:`.ServiceBackend`.
    default_cpu:
        CPU setting to use by default if not specified by a job. Only
        applicable if a docker image is specified for the :class:`.LocalBackend`
        or the :class:`.ServiceBackend`.
    default_storage:
        Storage setting to use by default if not specified by a job. Only
        applicable for the :class:`.ServiceBackend`.
    default_timeout:
        Maximum time in seconds for a job to run before being killed. Only
        applicable for the :class:`.ServiceBackend`. If `None`, there is no
        timeout.
    """

    _counter = 0
    _uid_prefix = "__BATCH__"
    _regex_pattern = r"(?P<BATCH>{}\d+)".format(_uid_prefix)

    @classmethod
    def _get_uid(cls):
        uid = "{}{}".format(cls._uid_prefix, cls._counter)
        cls._counter += 1
        return uid

    def __init__(self,
                 name: Optional[str] = None,
                 backend: Optional[_backend.Backend] = None,
                 attributes: Optional[Dict[str, str]] = None,
                 requester_pays_project: Optional[str] = None,
                 default_image: Optional[str] = None,
                 default_memory: Optional[str] = None,
                 default_cpu: Optional[str] = None,
                 default_storage: Optional[str] = None,
                 default_timeout: Optional[Union[float, int]] = None,
                 default_shell: Optional[str] = None):
        self._jobs: List[job.Job] = []
        self._resource_map: Dict[str, _resource.Resource] = {}
        self._allocated_files: Set[str] = set()
        self._input_resources: Set[_resource.InputResourceFile] = set()
        self._uid = Batch._get_uid()

        self.name = name

        if attributes is None:
            attributes = {}
        if 'name' in attributes:
            raise BatchException("'name' is not a valid attribute. Use the name argument instead.")
        self.attributes = attributes

        self.requester_pays_project = requester_pays_project

        self._default_image = default_image
        self._default_memory = default_memory
        self._default_cpu = default_cpu
        self._default_storage = default_storage
        self._default_timeout = default_timeout
        self._default_shell = default_shell

        self._backend = backend if backend else _backend.LocalBackend()

    def new_job(self,
                name: Optional[str] = None,
                attributes: Optional[Dict[str, str]] = None,
                shell: Optional[str] = None) -> job.Job:
        """
        Initialize a new job object with default memory, docker image,
        and CPU settings (defined in :class:`.Batch`) upon batch creation.

        Examples
        --------
        Create and execute a batch `b` with one job `j` that prints "hello world":

        >>> b = Batch()
        >>> j = b.new_job(name='hello', attributes={'language': 'english'})
        >>> j.command('echo "hello world"')
        >>> b.run()

        Parameters
        ----------
        name:
            Name of the job.
        attributes:
            Key-value pairs of additional attributes. 'name' is not a valid keyword.
            Use the name argument instead.
        """

        if attributes is None:
            attributes = {}

        if shell is None:
            shell = self._default_shell

        j = job.Job(batch=self, name=name, attributes=attributes, shell=shell)

        if self._default_image is not None:
            j.image(self._default_image)
        if self._default_memory is not None:
            j.memory(self._default_memory)
        if self._default_cpu is not None:
            j.cpu(self._default_cpu)
        if self._default_storage is not None:
            j.storage(self._default_storage)
        if self._default_timeout is not None:
            j.timeout(self._default_timeout)

        self._jobs.append(j)
        return j

    def _new_job_resource_file(self, source, value=None):
        if value is None:
            value = secret_alnum_string(5)
        jrf = _resource.JobResourceFile(value, source)
        self._resource_map[jrf._uid] = jrf  # pylint: disable=no-member
        return jrf

    def _new_input_resource_file(self, input_path, value=None):
        if value is None:
            value = f'{secret_alnum_string(5)}/{os.path.basename(input_path.rstrip("/"))}'
        irf = _resource.InputResourceFile(value)
        irf._add_input_path(input_path)
        self._resource_map[irf._uid] = irf  # pylint: disable=no-member
        self._input_resources.add(irf)
        return irf

    def _new_resource_group(self, source, mappings, root=None):
        assert isinstance(mappings, dict)
        if root is None:
            root = secret_alnum_string(5)
        d = {}
        new_resource_map = {}
        for name, code in mappings.items():
            if not isinstance(code, str):
                raise BatchException(f"value for name '{name}' is not a string. Found '{type(code)}' instead.")
            r = self._new_job_resource_file(source=source, value=eval(f'f"""{code}"""'))  # pylint: disable=W0123
            d[name] = r
            new_resource_map[r._uid] = r  # pylint: disable=no-member

        self._resource_map.update(new_resource_map)
        rg = _resource.ResourceGroup(source, root, **d)
        self._resource_map.update({rg._uid: rg})
        return rg

    def read_input(self, path: str) -> _resource.InputResourceFile:
        """
        Create a new input resource file object representing a single file.

        .. warning::

            To avoid expensive egress charges, input files should be located in buckets
            that are multi-regional in the United States because Batch runs jobs in any
            US region.

        Examples
        --------

        Read the file `hello.txt`:

        >>> b = Batch()
        >>> input = b.read_input('data/hello.txt')
        >>> j = b.new_job()
        >>> j.command(f'cat {input}')
        >>> b.run()

        Parameters
        ----------
        path: :obj:`str`
            File path to read.
        extension: :obj:`str`, optional
            File extension to use.
        """

        irf = self._new_input_resource_file(path)
        return irf

    def read_input_group(self, **kwargs: str) -> _resource.ResourceGroup:
        """Create a new resource group representing a mapping of identifier to
        input resource files.

        .. warning::

            To avoid expensive egress charges, input files should be located in buckets
            that are multi-regional in the United States because Batch runs jobs in any
            US region.

        Examples
        --------

        Read a binary PLINK file:

        >>> b = Batch()
        >>> bfile = b.read_input_group(bed="data/example.bed",
        ...                            bim="data/example.bim",
        ...                            fam="data/example.fam")
        >>> j = b.new_job()
        >>> j.command(f"plink --bfile {bfile} --geno --make-bed --out {j.geno}")
        >>> j.command(f"wc -l {bfile.fam}")
        >>> j.command(f"wc -l {bfile.bim}")
        >>> b.run() # doctest: +SKIP

        Read a FASTA file and it's index (file extensions matter!):

        >>> fasta = b.read_input_group(**{'fasta': 'data/example.fasta',
        ...                               'fasta.idx': 'data/example.fasta.idx'})

        Create a resource group where the identifiers don't match the file extensions:

        >>> rg = b.read_input_group(foo='data/foo.txt',
        ...                         bar='data/bar.txt')

        `rg.foo` and `rg.bar` will not have the `.txt` file extension and
        instead will be `{root}.foo` and `{root}.bar` where `{root}` is a random
        identifier.

        Notes
        -----
        The identifier is used to refer to a specific resource file. For example,
        given the resource group `rg`, you can use the attribute notation
        `rg.identifier` or the get item notation `rg[identifier]`.

        The file extensions for each file are derived from the identifier.  This
        is equivalent to `"{root}.identifier"` from
        :meth:`.Job.declare_resource_group`. We are planning on adding
        flexibility to incorporate more complicated extensions in the future
        such as `.vcf.bgz`.  For now, use :meth:`.JobResourceFile.add_extension`
        to add an extension to a resource file.

        Parameters
        ----------
        kwargs:
            Key word arguments where the name/key is the identifier and the value
            is the file path.
        """

        root = secret_alnum_string(5)
        new_resources = {name: self._new_input_resource_file(file, value=f'{root}/{os.path.basename(file.rstrip("/"))}')
                         for name, file in kwargs.items()}
        rg = _resource.ResourceGroup(None, root, **new_resources)
        self._resource_map.update({rg._uid: rg})
        return rg

    def write_output(self, resource: _resource.Resource, dest: str):  # pylint: disable=R0201
        """
        Write resource file or resource file group to an output destination.

        Examples
        --------

        Write a single job intermediate to a permanent location:

        >>> b = Batch()
        >>> j = b.new_job()
        >>> j.command(f'echo "hello" > {j.ofile}')
        >>> b.write_output(j.ofile, 'output/hello.txt')
        >>> b.run()

        .. warning::

            To avoid expensive egress charges, output files should be located in buckets
            that are multi-regional in the United States because Batch runs jobs in any
            US region.

        Notes
        -----
        All :class:`.JobResourceFile` are temporary files and must be written
        to a permanent location using :meth:`.write_output` if the output needs
        to be saved.

        Parameters
        ----------
        resource:
            Resource to be written to a file.
        dest:
            Destination file path. For a single :class:`.ResourceFile`, this will
            simply be `dest`. For a :class:`.ResourceGroup`, `dest` is the file
            root and each resource file will be written to `{root}.identifier`
            where `identifier` is the identifier of the file in the
            :class:`.ResourceGroup` map.
        """

        if not isinstance(resource, _resource.Resource):
            raise BatchException(f"'write_output' only accepts Resource inputs. Found '{type(resource)}'.")
        if isinstance(resource, _resource.JobResourceFile) and resource not in resource._source._mentioned:
            name = resource._source._resources_inverse
            raise BatchException(f"undefined resource '{name}'\n"
                                 f"Hint: resources must be defined within the "
                                 f"job methods 'command' or 'declare_resource_group'")

        if isinstance(self._backend, _backend.LocalBackend):
            dest = os.path.abspath(dest)

        resource._add_output_path(dest)

    def select_jobs(self, pattern: str) -> List[job.Job]:
        """
        Select all jobs in the batch whose name matches `pattern`.

        Examples
        --------

        Select jobs in batch matching `qc`:

        >>> b = Batch()
        >>> j = b.new_job(name='qc')
        >>> qc_jobs = b.select_jobs('qc')
        >>> assert qc_jobs == [j]

        Parameters
        ----------
        pattern:
            Regex pattern matching job names.
        """

        return [job for job in self._jobs if job.name is not None and re.match(pattern, job.name) is not None]

    def run(self,
            dry_run: bool = False,
            verbose: bool = False,
            delete_scratch_on_exit: bool = True,
            **backend_kwargs: Any):
        """
        Execute a batch.

        Examples
        --------

        Create a simple batch with one job and execute it:

        >>> b = Batch()
        >>> j = b.new_job()
        >>> j.command('echo "hello world"')
        >>> b.run()


        Parameters
        ----------
        dry_run:
            If `True`, don't execute code.
        verbose:
            If `True`, print debugging output.
        delete_scratch_on_exit:
            If `True`, delete temporary directories with intermediate files.
        backend_kwargs:
            See :meth:`.Backend._run` for backend-specific arguments.
        """

        seen = set()
        ordered_jobs = []

        def schedule_job(j):
            if j in seen:
                return
            seen.add(j)
            for p in j._dependencies:
                schedule_job(p)
            ordered_jobs.append(j)

        for j in self._jobs:
            schedule_job(j)

        assert len(seen) == len(self._jobs)

        job_index = {j: i for i, j in enumerate(ordered_jobs, start=1)}
        for j in ordered_jobs:
            i = job_index[j]
            j._job_id = i
            for d in j._dependencies:
                if job_index[d] >= i:
                    raise BatchException("cycle detected in dependency graph")

        self._jobs = ordered_jobs
        return self._backend._run(self, dry_run, verbose, delete_scratch_on_exit, **backend_kwargs)

    def __str__(self):
        return self._uid
