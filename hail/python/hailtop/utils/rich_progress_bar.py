from collections import namedtuple
from enum import Enum
from typing import Dict, Optional, Callable, Tuple, List

from rich import filesize
from rich.color import Color
from rich.live import Live
from rich.panel import Panel
from rich.progress import (
    MofNCompleteColumn,
    BarColumn,
    TextColumn,
    TimeRemainingColumn,
    TimeElapsedColumn,
    Progress,
    ProgressColumn,
    SpinnerColumn,
    TaskProgressColumn,
    Task,
    TaskID,
)
from rich.style import Style
from rich.table import Table
from rich.text import Text

from ..batch_client.aioclient import BatchClient
from .rich_multistate_progress_bar_v2 import (
    MultiStateProgressColumn,
    MultiStateProgress
)
from .utils import async_to_blocking


class SimpleCopyToolProgressBarTask:
    def __init__(self, progress: Progress, tid):
        self._progress = progress
        self.tid = tid

    def total(self) -> int:
        assert len(self._progress.tasks) == 1
        return int(self._progress.tasks[0].total or 0)

    def update(self, delta_n: int, *, total: Optional[int] = None):
        self._progress.update(self.tid, advance=delta_n, total=total)

    def make_listener(self) -> Callable[[int], None]:
        return make_listener(self._progress, self.tid)


class SimpleCopyToolProgressBar:
    def __init__(self, *args, description: Optional[str] = None, total: int, visible: bool = True, **kwargs):
        self.description = description
        self.total = total
        self.visible = visible
        if len(args) == 0:
            args = CopyToolProgressBar.get_default_columns()
        self._progress = Progress(*args, **kwargs)

    def __enter__(self) -> SimpleCopyToolProgressBarTask:
        self._progress.start()
        tid = self._progress.add_task(self.description or '', total=self.total, visible=self.visible)
        return SimpleCopyToolProgressBarTask(self._progress, tid)

    def __exit__(self, exc_type, exc_value, traceback):
        del exc_type
        del exc_value
        del traceback
        try:
            self._progress.refresh()
        finally:
            self._progress.stop()


def make_listener(progress: Progress, tid) -> Callable[[int], None]:
    total = 0

    def listen(delta: int):
        nonlocal total
        if delta > 0:
            total += delta
            progress.update(tid, total=total)
        else:
            progress.update(tid, advance=-delta)
    return listen


def units(task: Task) -> Tuple[List[str], int]:
    if task.description == 'files':
        return ["files", "K files", "M files", "G files", "T files", "P files", "E files", "Z files", "Y files"], 1000
    if task.description == 'bytes':
        return ["bytes", "KiB", "MiB", "GiB", "TiB", "PiB", "EiB", "ZiB", "YiB"], 1024
    return ["", "K", "M", "G", "T", "P", "E", "Z", "Y"], 1000


class BytesOrCountOrN(ProgressColumn):
    def render(self, task: "Task") -> Text:
        completed = int(task.completed)
        n = int(task.total) if task.total is not None else completed
        unit, suffix = filesize.pick_unit_and_suffix(n, *units(task))
        precision = 0 if unit == 1 else 1

        completed_ratio = completed / unit
        completed_str = f"{completed_ratio:,.{precision}f}"

        if task.total is not None:
            total = int(task.total)
            total_ratio = total / unit
            total_str = f"{total_ratio:,.{precision}f}"
        else:
            total_str = "?"

        download_status = f"{completed_str}/{total_str} {suffix}"
        download_text = Text(download_status, style="progress.download")
        return download_text


class RateColumn(ProgressColumn):
    def render(self, task: "Task") -> Text:
        speed = task.finished_speed or task.speed
        if speed is None:
            return Text("?", style="progress.data.speed")

        speed = int(speed)
        unit, suffix = filesize.pick_unit_and_suffix(speed, *units(task))
        precision = 0 if unit == 1 else 1
        return Text(f"{speed / unit:,.{precision}f} {suffix}/s", style="progress.data.speed")


class CopyToolProgressBar:
    def __init__(self, *args, **kwargs):
        if len(args) == 0:
            args = CopyToolProgressBar.get_default_columns()
        self._progress = Progress(*args, **kwargs)

    @staticmethod
    def get_default_columns() -> Tuple[ProgressColumn, ...]:
        return (
            TextColumn("[progress.description]{task.description}"),
            BarColumn(complete_style="bar.finished"),
            TaskProgressColumn(),
            BytesOrCountOrN(),
            RateColumn(),
            TimeRemainingColumn(),
            TimeElapsedColumn()
        )

    def __enter__(self) -> Progress:
        self._progress.start()
        return self._progress

    def __exit__(self, exc_type, exc_value, traceback):
        del exc_type
        del exc_value
        del traceback
        try:
            self._progress.refresh()
        finally:
            self._progress.stop()


class BatchProgressBar:
    def __init__(self, *args, **kwargs):
        if len(args) == 0:
            args = BatchProgressBar.get_default_columns()
        self._progress = Progress(*args, **kwargs)

    @staticmethod
    def get_default_columns() -> Tuple[ProgressColumn, ...]:
        return (
            TextColumn("[progress.description]{task.description}"),
            BarColumn(complete_style="bar.finished"),
            TaskProgressColumn(),
            MofNCompleteColumn(),
            TimeRemainingColumn(),
            TimeElapsedColumn()
        )

    def __enter__(self) -> 'BatchProgressBar':
        self._progress.start()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        del exc_type
        del exc_value
        del traceback
        try:
            self._progress.refresh()
        finally:
            self._progress.stop()

    def with_task(self, description: str, *, total: int = 0, disable: bool = False, transient: bool = False) -> 'BatchProgressBarTask':
        tid = self._progress.add_task(description, total=total, visible=not disable)
        return BatchProgressBarTask(self._progress, tid, transient)


class BatchProgressBarTask:
    def __init__(self, progress: Progress, tid, transient: bool):
        self._progress = progress
        self.tid = tid
        self.transient = transient

    def total(self) -> int:
        assert len(self._progress.tasks) == 1
        return int(self._progress.tasks[0].total or 0)

    def __enter__(self) -> 'BatchProgressBarTask':
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        del exc_type
        del exc_value
        del traceback
        if self.transient:
            self._progress.remove_task(self.tid)

    def update(self, advance: Optional[int] = None, **kwargs):
        self._progress.update(self.tid, advance=advance, **kwargs)


class MarkJobCompleteColumn(ProgressColumn):
    def render(self, task: "Task") -> Text:
        """Show data transfer speed."""
        speed = task.finished_speed or task.speed
        if speed is None:
            return Text("?", style="progress.data.speed")
        return Text(f"{speed:>1.0f} jobs/s", style="progress.data.speed")


class PoolStats:
    @staticmethod
    def from_dict(pool: dict) -> 'PoolStats':
        name = pool['name']
        cores_mcpu_by_state = pool['all_versions_cores_mcpu_by_state']
        total_cores = pool['total_capacity_cores_mcpu'] // 1000
        pending_cores = cores_mcpu_by_state['pending'] // 1000
        active_cores = cores_mcpu_by_state['active'] // 1000

        me_cores = pool['user_running_cores_mcpu'] // 1000
        provisioning_cores = pending_cores
        available_cores = pool['current_worker_version_active_schedulable_free_cores_mcpu'] // 1000
        other_users_cores = active_cores - me_cores - available_cores
        assert 0 <= other_users_cores + me_cores + available_cores + provisioning_cores <= total_cores
        return PoolStats(name, total_cores, me_cores, other_users_cores, available_cores, provisioning_cores)

    def __init__(self, name: str, total_cores, me_cores, other_users_cores, available_cores, provisioning_cores):
        self.name = name
        self.total_cores = total_cores
        self.me_cores = me_cores
        self.other_users_cores = other_users_cores
        self.available_cores = available_cores
        self.provisioning_cores = provisioning_cores

    def get_value_from_cluster_state(self, state: 'ClusterState'):
        if state == ClusterState.ME:
            return self.me_cores
        elif state == ClusterState.OTHER_USERS:
            return self.other_users_cores
        elif state == ClusterState.AVAILABLE:
            return self.available_cores
        assert state == ClusterState.PROVISIONING
        return self.provisioning_cores


ClusterStyle = namedtuple('ClusterStyle', ['label', 'style'])


class ClusterState(Enum):
    ME = ClusterStyle('Me', Style(color='magenta'))
    OTHER_USERS = ClusterStyle('Other Users', Style(color='cyan'))
    AVAILABLE = ClusterStyle('Available', Style(color='green'))
    PROVISIONING = ClusterStyle('Provisioning', Style(color='yellow'))


class ClusterStateData:
    def __init__(self, task_id: TaskID, state_id: int, value: int):
        self.task_id = task_id
        self.state_id = state_id
        self.value = value


class ClusterCapacityProgress:
    def __init__(self, batch_client: BatchClient):
        self.batch_client = batch_client
        self._progress = MultiStateProgress(
            "{task.description}",
            MultiStateProgressColumn(),
            TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
            TextColumn("[progress.total]{task.total} cores"),
        )
        self._pool_states: Dict[str, Dict[ClusterState, ClusterStateData]] = {}
        self.initialize()

    def cluster_stats(self):
        return async_to_blocking(self.batch_client.cluster_stats())

    def initialize(self):
        cluster_stats = self.cluster_stats()
        for pool in cluster_stats['pools']:
            self._initialize_pool(pool)

    def _initialize_pool(self, pool: dict):
        pool_stats = PoolStats.from_dict(pool)
        t = self._progress.add_task(pool_stats.name, total=pool_stats.total_cores)
        for state in ClusterState:
            value = pool_stats.get_value_from_cluster_state(state)
            state_info = state.value
            state_id = self._progress.add_state(t, state_info.label, value, state_info.style)
            self._pool_states[pool_stats.name][state] = ClusterStateData(t, state_id, value)

    def update(self):
        cluster_stats = self.cluster_stats()
        for pool in cluster_stats['pools']:
            pool_stats = PoolStats.from_dict(pool)
            if pool_stats.name not in self._pool_states:
                self._initialize_pool(pool)
            for state, state_data in self._pool_states[pool_stats.name]:
                new_value = pool_stats.get_value_from_cluster_state(state)
                state_data.value = new_value
                self._progress.update_state(state_data.task_id, state_data.state_id, completed=new_value)


class BatchStatusTable:
    def __init__(self, batch_client: BatchClient):
        self.batch_client = batch_client
        self.progress_table = Table.grid()

        self.cluster_capacity_progress = ClusterCapacityProgress(batch_client)
        self.progress_table.add_row(
            Panel.fit(
                self.cluster_capacity_progress._progress,
                title="[b]Cluster Capacity",
                border_style="black",
                padding=(1, 2)
            ),
        )

        # self.job_progress = MultiStateProgress(
        #     "{task.description}",
        #     MultiStateProgressColumn(),
        #     TextColumn("[progress.percentage]{task.not_running_or_pending_percentage:>3.0f}%"),
        #     TextColumn("[progress.completed]{task.not_running_or_pending}/{task.total} jobs"),
        #     MarkJobCompleteColumn(),
        #     TimeElapsedColumn(),
        #     SpinnerColumn(style=Style(color=Color.from_rgb(50, 175, 255))),
        # )
        # self.progress_table.add_row(
        #     Panel.fit(self.job_progress, title="[b]Progress Bar", border_style="black", padding=(1, 2)),
        # )

    def __enter__(self):
        with Live(self.progress_table, refresh_per_second=30):
            self.cluster_capacity_progress.update()
