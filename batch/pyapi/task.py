import re
from .resource import Resource, ResourceGroup, ResourceGroupBuilder

class TaskSettings(object):
    def __init__(self, cpu=None, memory=None, docker=None, env=None):
        self.cpu = cpu
        self.memory = memory
        self.docker = docker
        self.env = env


default_task_settings = TaskSettings(cpu=1, memory=1, docker=None, env=None)


class Task(object):
    _counter = 0
    _uid_prefix = "__TASK__"
    _regex_pattern = r"(?P<TASK>{}\d+)".format(_uid_prefix)

    @classmethod
    def _new_uid(cls):
        uid = "{}{}".format(cls._uid_prefix, cls._counter)
        cls._counter += 1
        return uid

    def __init__(self, pipeline, label=None, settings=None):
        self._settings = settings if settings else default_task_settings
        assert isinstance(self._settings, TaskSettings)

        self._pipeline = pipeline
        self._label = label
        self._command = []
        self._namespace = {}
        self._resources = {}
        self._dependencies = set()
        self._inputs = set()
        self._uid = Task._new_uid()

    def _get_resource(self, item):
        if item not in self._namespace:
            self._namespace[item] = self._pipeline._new_resource(self)
        r = self._namespace[item]
        if isinstance(r, Resource):
            return str(r)
        else:
            assert isinstance(r, ResourceGroup)
            return r

    def __getitem__(self, item):
        return self._get_resource(item)

    def __getattr__(self, item):
        return self._get_resource(item)

    def declare_resource_group(self, **kwargs):
        for name, rgb in kwargs.items():
            assert name not in self._namespace
            if not isinstance(rgb, ResourceGroupBuilder):
                raise ValueError(f"value for name '{name}' is not a ResourceGroupBuilder. Found '{type(rgb)}' instead.")
            self._namespace[name] = self._pipeline._new_resource_group(self, rgb)
        return self

    def command(self, command):
        from .pipeline import Pipeline

        def add_dependencies(r):
            if isinstance(r, ResourceGroup):
                [add_dependencies(resource) for _, resource in r._namespace.items()]
            else:
                assert isinstance(r, Resource)
                if r._source is not None and r._source != self:
                    self._dependencies.add(r._source)

        def handler(match_obj):
            groups = match_obj.groupdict()
            if groups['TASK']:
                raise ValueError(f"found a reference to a Task object in command '{command}'.")
            elif groups['PIPELINE']:
                raise ValueError(f"found a reference to a Pipeline object in command '{command}'.")
            else:
                assert groups['RESOURCE'] or groups['RESOURCE_GROUP']
                r_uid = match_obj.group()
                r = self._pipeline._resource_map.get(r_uid)
                if r is None:
                    raise KeyError(f"undefined resource '{r_uid}' in command '{command}'.")
                add_dependencies(r)
                self._resources[r._uid] = r
                return f"${{{r_uid}}}"

        subst_command = re.sub(f"({Resource._regex_pattern})|({ResourceGroup._regex_pattern})" \
                                f"|({Task._regex_pattern})|({Pipeline._regex_pattern})",
                                handler,
                                command)
        self._command.append(subst_command)
        return self

    def label(self, label):
        self._label = label
        return self

    def memory(self, memory):
        self._settings.memory = memory
        return self

    def cpu(self, cpu):
        self._settings.cpu = cpu
        return self

    def docker(self, docker):
        self._settings.docker = docker
        return self

    def env(self, env):
        self._settings.env = env
        return self

    def __str__(self):
        return self._uid
