import abc

from shlex import quote as shq

from .utils import BatchException


class Resource:
    """
    Abstract class for resources.
    """

    _uid: str

    @abc.abstractmethod
    def _get_path(self, directory) -> str:
        pass

    @abc.abstractmethod
    def _add_output_path(self, path):
        pass

    def _declare(self, directory):
        return f"{self._uid}={shq(self._get_path(directory))}"  # pylint: disable=no-member


class ResourceFile(Resource, str):
    """
    Class representing a single file resource. There exist two subclasses:
    :class:`.InputResourceFile` and :class:`.JobResourceFile`.
    """
    _counter = 0
    _uid_prefix = "__RESOURCE_FILE__"
    _regex_pattern = r"(?P<RESOURCE_FILE>{}\d+)".format(_uid_prefix)

    @classmethod
    def _new_uid(cls):
        uid = "{}{}".format(cls._uid_prefix, cls._counter)
        cls._counter += 1
        return uid

    def __new__(cls, value):  # pylint: disable=W0613
        uid = ResourceFile._new_uid()
        r = str.__new__(cls, uid)
        r._uid = uid
        return r

    def __init__(self, value):
        super(ResourceFile, self).__init__()
        assert value is None or isinstance(value, str)
        self._value = value
        self._source = None
        self._output_paths = set()
        self._resource_group = None

    def _get_path(self, directory):
        raise NotImplementedError

    def _add_source(self, source):
        from .job import Job  # pylint: disable=cyclic-import
        assert isinstance(source, Job)
        self._source = source
        return self

    def _add_output_path(self, path):
        self._output_paths.add(path)
        if self._source is not None:
            self._source._external_outputs.add(self)

    def _add_resource_group(self, rg):
        self._resource_group = rg

    def _has_resource_group(self):
        return self._resource_group is not None

    def _get_resource_group(self):
        return self._resource_group

    def __str__(self):
        return self._uid  # pylint: disable=no-member

    def __repr__(self):
        return self._uid  # pylint: disable=no-member


class InputResourceFile(ResourceFile):
    """
    Class representing a resource from an input file.

    Examples
    --------
    `input` is an :class:`.InputResourceFile` of the batch `b`
    and is used in job `j`:

    >>> b = Batch()
    >>> input = b.read_input('data/hello.txt')
    >>> j = b.new_job(name='hello')
    >>> j.command(f'cat {input}')
    >>> b.run()
    """

    def __init__(self, value):
        self._input_path = None
        super().__init__(value)

    def _add_input_path(self, path):
        self._input_path = path
        return self

    def _get_path(self, directory):
        assert self._value is not None
        return shq(directory + '/inputs/' + self._value)


class JobResourceFile(ResourceFile):
    """
    Class representing an intermediate file from a job.

    Examples
    --------
    `j.ofile` is a :class:`.JobResourceFile` on the job`j`:

    >>> b = Batch()
    >>> j = b.new_job(name='hello-tmp')
    >>> j.command(f'echo "hello world" > {j.ofile}')
    >>> b.run()

    Notes
    -----
    All :class:`.JobResourceFile` are temporary files and must be written
    to a permanent location using :meth:`.Batch.write_output` if the output needs
    to be saved.
    """

    def __init__(self, value):
        super().__init__(value)
        self._has_extension = False

    def _get_path(self, directory):
        assert self._source is not None
        assert self._value is not None
        return shq(f'{directory}/{self._source._job_id}/{self._value}')

    def add_extension(self, extension):
        """
        Specify the file extension to use.

        Examples
        --------

        >>> b = Batch()
        >>> j = b.new_job()
        >>> j.command(f'echo "hello" > {j.ofile}')
        >>> j.ofile.add_extension('.txt')
        >>> b.run()

        Notes
        -----
        The default file name for a :class:`.ResourceFile` is a unique
        identifier with no file extensions.

        Parameters
        ----------
        extension: :obj:`str`
            File extension to use.

        Returns
        -------
        :class:`.ResourceFile`
            Same resource file with the extension specified
        """
        if self._has_extension:
            raise BatchException("Resource already has a file extension added.")
        self._value += extension
        self._has_extension = True
        return self


class ResourceGroup(Resource):
    """
    Class representing a mapping of identifiers to a resource file.

    Examples
    --------

    Initialize a batch and create a new job:

    >>> b = Batch()
    >>> j = b.new_job()

    Read a set of input files as a resource group:

    >>> bfile = b.read_input_group(bed='data/example.bed',
    ...                            bim='data/example.bim',
    ...                            fam='data/example.fam')

    Create a resource group from a job intermediate:

    >>> j.declare_resource_group(ofile={'bed': '{root}.bed',
    ...                                 'bim': '{root}.bim',
    ...                                 'fam': '{root}.fam'})
    >>> j.command(f'plink --bfile {bfile} --make-bed --out {j.ofile}')

    Reference the entire file group:

    >>> j.command(f'plink --bfile {bfile} --geno 0.2 --make-bed --out {j.ofile}')

    Reference a single file:

    >>> j.command(f'wc -l {bfile.fam}')

    Execute the batch:

    >>> b.run() # doctest: +SKIP

    Notes
    -----
    All files in the resource group are copied between jobs even if only one
    file in the resource group is mentioned. This is to account for files that
    are implicitly assumed to always be together such as a FASTA file and its
    index.
    """

    _counter = 0
    _uid_prefix = "__RESOURCE_GROUP__"
    _regex_pattern = r"(?P<RESOURCE_GROUP>{}\d+)".format(_uid_prefix)

    @classmethod
    def _new_uid(cls):
        uid = "{}{}".format(cls._uid_prefix, cls._counter)
        cls._counter += 1
        return uid

    def __init__(self, source, root, **values):
        self._source = source
        self._resources = {}  # dict of name to resource uid
        self._root = root
        self._uid = ResourceGroup._new_uid()

        for name, resource_file in values.items():
            assert isinstance(resource_file, ResourceFile)
            self._resources[name] = resource_file
            resource_file._add_resource_group(self)

    def _get_path(self, directory):
        subdir = str(self._source._job_id) if self._source else 'inputs'
        return directory + '/' + subdir + '/' + self._root

    def _add_output_path(self, path):
        for name, rf in self._resources.items():
            rf._add_output_path(path + '.' + name)

    def _get_resource(self, item):
        if item not in self._resources:
            raise BatchException(f"'{item}' not found in the resource group.\n"
                                 f"Hint: you must declare each attribute when constructing the resource group.")
        return self._resources[item]

    def __getitem__(self, item):
        return self._get_resource(item)

    def __getattr__(self, item):
        return self._get_resource(item)

    def __add__(self, other):
        assert isinstance(other, str)
        return str(self._uid) + other

    def __radd__(self, other):
        assert isinstance(other, str)
        return other + str(self._uid)

    def __str__(self):
        return self._uid
