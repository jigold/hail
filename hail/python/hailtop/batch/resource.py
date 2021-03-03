import abc
import os
# import dill
import cloudpickle
from shlex import quote as shq
from typing import Optional, Set

from . import job  # pylint: disable=cyclic-import
from .exceptions import BatchException


class Resource:
    """
    Abstract class for resources.
    """

    _uid: str

    @abc.abstractmethod
    def _get_path(self, directory: str) -> str:
        pass

    @abc.abstractmethod
    def _add_output_path(self, path: str) -> None:
        pass

    @abc.abstractmethod
    def _source(self):
        pass

    def _declare(self, directory: str) -> str:
        return f"export {self._uid}={shq(self._get_path(directory))}"  # pylint: disable=no-member


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

    def __new__(cls, *args, **kwargs):  # pylint: disable=W0613
        uid = ResourceFile._new_uid()
        r = str.__new__(cls, uid)
        r._uid = uid
        return r

    def __init__(self, value: Optional[str]):
        super().__init__()
        assert value is None or isinstance(value, str)
        self._value = value
        self._source: Optional[job.Job] = None
        self._output_paths: Set[str] = set()
        self._resource_group: Optional[ResourceGroup] = None

    def _get_path(self, directory: str):
        raise NotImplementedError

    def _add_output_path(self, path: str) -> None:
        self._output_paths.add(path)
        if self._source is not None:
            self._source._external_outputs.add(self)

    def _add_resource_group(self, rg: 'ResourceGroup') -> None:
        self._resource_group = rg

    def _has_resource_group(self) -> bool:
        return self._resource_group is not None

    def _get_resource_group(self) -> Optional['ResourceGroup']:
        return self._resource_group

    def __str__(self):
        return f'"{self._uid}"'  # pylint: disable=no-member

    def __repr__(self):
        return self._uid  # pylint: disable=no-member

    def __getstate__(self):
        return {'_uid': self._uid}

    def __setstate__(self, state):
        self._uid = state['_uid']

    def _load_value(self):
        return os.environ[self._uid]

    # def __reduce__(self):
    #     def reduce():
    #         return os.environ[self._uid]
    #     return (reduce, ())


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

    def _add_input_path(self, path: str) -> 'InputResourceFile':
        self._input_path = path
        return self

    def _get_path(self, directory: str) -> str:
        assert self._value is not None
        return directory + '/inputs/' + self._value


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

    def __init__(self, value, source: job.Job):
        super().__init__(value)
        self._has_extension = False
        self._source: job.Job = source

    def _get_path(self, directory: str) -> str:
        assert self._source is not None
        assert self._value is not None
        return f'{directory}/{self._source._job_id}/{self._value}'

    def add_extension(self, extension: str) -> 'JobResourceFile':
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
        The default file name for a :class:`.JobResourceFile` is the name
        of the identifier.

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
        assert self._value is not None
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

    def __init__(self, source: Optional[job.Job], root: str, **values: ResourceFile):
        self._source = source
        self._resources = {}  # dict of name to resource uid
        self._root = root
        self._uid = ResourceGroup._new_uid()

        for name, resource_file in values.items():
            assert isinstance(resource_file, ResourceFile)
            self._resources[name] = resource_file
            resource_file._add_resource_group(self)

    def _get_path(self, directory: str) -> str:
        subdir = str(self._source._job_id) if self._source else 'inputs'
        return directory + '/' + subdir + '/' + self._root

    def _add_output_path(self, path: str) -> None:
        for name, rf in self._resources.items():
            rf._add_output_path(path + '.' + name)

    def _get_resource(self, item: str) -> ResourceFile:
        if item not in self._resources:
            raise BatchException(f"'{item}' not found in the resource group.\n"
                                 f"Hint: you must declare each attribute when constructing the resource group.")
        return self._resources[item]

    def __getitem__(self, item: str) -> ResourceFile:
        return self._get_resource(item)

    def __getattr__(self, item: str) -> ResourceFile:
        return self._get_resource(item)

    def __add__(self, other: str):
        assert isinstance(other, str)
        return str(self._uid) + other

    def __radd__(self, other: str):
        assert isinstance(other, str)
        return other + str(self._uid)

    def __str__(self):
        return f'"{self._uid}"'

    def __getstate__(self):
        return {'resources': self._resources}
    #
    # def __reduce__(self):
    #     return (dict, (self._resources,))


def deserialize_object(uid):
    with open(os.environ[uid], 'rb') as f:
        x = cloudpickle.load(f)()
        # x = dill.load(f)()
    return x


class PythonResult(Resource, str):
    """
    Class representing a Python result.
    """
    _counter = 0
    _uid_prefix = "__PYTHON_RESULT__"
    _regex_pattern = r"(?P<PYTHON_RESULT>{}\d+)".format(_uid_prefix)

    @classmethod
    def _new_uid(cls):
        uid = "{}{}".format(cls._uid_prefix, cls._counter)
        cls._counter += 1
        return uid

    def __new__(cls, *args, **kwargs):  # pylint: disable=W0613
        uid = PythonResult._new_uid()
        r = str.__new__(cls, uid)
        r._uid = uid
        return r

    def __init__(self, value: Optional[str], source: job.PythonJob):
        super().__init__()
        assert value is None or isinstance(value, str)
        self._value = value
        self._source = source
        self._output_paths: Set[str] = set()
        self._json = None
        self._str = None
        self._repr = None

    def _get_path(self, directory: str) -> str:
        assert self._source is not None
        assert self._value is not None
        return f'{directory}/{self._source._job_id}/{self._value}'

    def _add_converted_resource(self, value):
        jrf = self._source._batch._new_job_resource_file(self._source, value)
        self._source._resources[value] = jrf
        self._source._resources_inverse[jrf] = value
        self._source._valid.add(jrf)
        self._source._mentioned.add(jrf)
        return jrf

    def _add_output_path(self, path: str) -> None:
        self._output_paths.add(path)
        if self._source is not None:
            self._source._external_outputs.add(self)

    def as_json(self):
        if self._json is None:
            jrf = self._add_converted_resource(self._value + '-json')
            jrf.add_extension('.json')
            self._json = jrf
        return self._json

    def as_str(self):
        if self._str is None:
            jrf = self._add_converted_resource(self._value + '-str')
            jrf.add_extension('.txt')
            self._str = jrf
        return self._str

    def as_repr(self):
        if self._repr is None:
            jrf = self._add_converted_resource(self._value + '-repr')
            jrf.add_extension('.txt')
            self._repr = jrf
        return self._repr

    def __str__(self):
        return f'"{self._uid}"'  # pylint: disable=no-member

    def __repr__(self):
        return self._uid  # pylint: disable=no-member

    def __getstate__(self):
        return {'_uid': self._uid}

    def _load_value(self):
        with open(os.environ[self._uid], 'rb') as f:
            x = cloudpickle.load(f)()
            # x = dill.load(f)()
        return x

    # def __getnewargs__(self):
    #     def reduce():
    #         with open(os.environ[self._uid], 'rb') as f:
    #             x = dill.load(f)()
    #         return x
    #     return (reduce, ())

    # def __reduce__(self):
    #     def reduce():
    #         with open(os.environ[self._uid], 'rb') as f:
    #             x = dill.load(f)()
    #         return x
    #     return (reduce, ())

# def reduce(v):
#     return 5
#
#
# class Foo:
#     def __init__(self, x):
#         self.x = x
#     def __getstate__(self):
#         return {'x': dill.dumps(self.x)}
#     def __reduce__(self):
#         def reduce():
#             x = dill.dumps(5)
#             return dill.loads(x)
#         return (reduce, ())
#
# class Foo:
#     def __init__(self, x):
#         self.x = x
#     def __getstate__(self):
#         return {'x': dill.dumps(self.x)}
#     def __reduce__(self):
#         def reduce():
#             x = dill.dumps(Foo(4))
#             return dill.loads(x)
#         return (reduce, ())
#
# f = Foo('foo')
# fs = dill.dumps(f)
# dill.loads(fs)