import re

from .backend import LocalBackend
from .task import Task
from .resource import ResourceGroup, InputResourceFile, TaskResourceFile
from .utils import get_sha


class Pipeline:
    _counter = 0
    _uid_prefix = "__PIPELINE__"
    _regex_pattern = r"(?P<PIPELINE>{}\d+)".format(_uid_prefix)

    @classmethod
    def _get_uid(cls):
        uid = "{}{}".format(cls._uid_prefix, cls._counter)
        cls._counter += 1
        return uid

    def __init__(self, backend=None, default_image=None):
        self._tasks = []
        self._resource_map = {}
        self._allocated_files = set()
        self._backend = backend if backend else LocalBackend()
        self._uid = Pipeline._get_uid()
        self._default_image = default_image

    def new_task(self):
        t = Task(pipeline=self)
        self._tasks.append(t)
        if self._default_image is not None:
            t.image(self._default_image)
        return t

    def _tmp_file(self, prefix=None, suffix=None):
        def _get_random_file():
            file = '{}{}{}'.format(prefix if prefix else '',
                                   get_sha(8),
                                   suffix if suffix else '')
            if file not in self._allocated_files:
                self._allocated_files.add(file)
                return file
            else:
                return _get_random_file()

        return _get_random_file()

    def _new_task_resource_file(self, source, value=None):
        trf = TaskResourceFile(value if value else self._tmp_file())
        trf.add_source(source)
        self._resource_map[trf._uid] = trf
        return trf

    def _new_input_resource_file(self, input_path, value=None):
        irf = InputResourceFile(value if value else self._tmp_file())
        irf.add_input_path(input_path)
        self._resource_map[irf._uid] = irf
        return irf

    def _new_resource_group(self, source, mappings):
        assert isinstance(mappings, dict)
        root = self._tmp_file()
        d = {}
        new_resource_map = {}
        for name, code in mappings.items():
            if not isinstance(code, str):
                raise ValueError(f"value for name '{name}' is not a string. Found '{type(code)}' instead.")
            r = self._new_task_resource_file(source=source, value=eval(f'f"""{code}"""'))  # pylint: disable=W0123
            d[name] = r
            new_resource_map[r._uid] = r

        self._resource_map.update(new_resource_map)
        rg = ResourceGroup(source, root, **d)
        self._resource_map.update({rg._uid: rg})
        return rg

    def read_input(self, path):
        return self._new_input_resource_file(path)

    def read_input_group(self, **kwargs):
        root = self._tmp_file()
        new_resources = {name: self._new_input_resource_file(file, root + '.' + name) for name, file in kwargs.items()}
        rg = ResourceGroup(None, root, **new_resources)
        self._resource_map.update({rg._uid: rg})
        return rg

    def write_output(self, resource, dest):  # pylint: disable=R0201
        resource.add_output_path(dest)

    def select_tasks(self, pattern):
        return [task for task in self._tasks if task._label is not None and re.match(pattern, task._label) is not None]

    def run(self, dry_run=False, verbose=False, delete_on_exit=True):
        dependencies = {task: task._dependencies for task in self._tasks}
        ordered_tasks = []
        niter = 0
        while dependencies:
            for task, deps in dependencies.items():
                if not deps:
                    ordered_tasks.append(task)
                    niter = 0
            for task, _ in dependencies.items():
                dependencies[task] = dependencies[task].difference(set(ordered_tasks))
            for task in ordered_tasks:
                if task in dependencies:
                    del dependencies[task]
            niter += 1

            if niter == 100:
                raise ValueError("cycle detected in dependency graph")

        self._tasks = ordered_tasks
        self._backend.run(self, dry_run, verbose, False, delete_on_exit)  # FIXME: expose bg option when implemented!

    def __str__(self):
        return self._uid
