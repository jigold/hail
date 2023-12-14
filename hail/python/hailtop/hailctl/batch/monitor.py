import collections

import asyncio

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

from hailtop.utils.rich_multistate_progress_bar_v2 import (
    MultiStateProgressColumn,
    MultiStateProgress
)


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
        total_cores = pool['total_capacity_cores']
        pending_cores = cores_mcpu_by_state.get('pending', 0) / 1000
        active_cores = cores_mcpu_by_state.get('active', 0) / 1000

        me_cores = pool['user_running_cores_mcpu'] / 1000
        provisioning_cores = pending_cores
        available_cores = pool['current_worker_version_active_schedulable_free_cores_mcpu'] / 1000
        other_users_cores = active_cores - me_cores - available_cores
        assert other_users_cores >= 0
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
    def __init__(self, batch_client):
        self.batch_client = batch_client
        self._progress = MultiStateProgress(
            "{task.description}",
            MultiStateProgressColumn(),
            TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
            TextColumn("[progress.total]{task.total} cores"),
            max_visible_tasks=None
        )
        self._pool_states: Dict[str, Dict[ClusterState, ClusterStateData]] = collections.defaultdict(dict)
        self._pool_tasks: Dict[str, TaskID] = {}

    async def cluster_stats(self):
        return await self.batch_client.cluster_stats()

    async def initialize(self):
        cluster_stats = await self.cluster_stats()
        for pool in cluster_stats.values():
            self._initialize_pool(pool)

    def _initialize_pool(self, pool: dict):
        pool_stats = PoolStats.from_dict(pool)

        t = self._progress.add_task(pool_stats.name, total=pool_stats.total_cores)
        self._pool_tasks[pool_stats.name] = t

        for state in ClusterState:
            value = pool_stats.get_value_from_cluster_state(state)
            state_info = state.value
            state_id = self._progress.add_state(t, state_info.label, value, state_info.style)
            self._pool_states[pool_stats.name][state] = ClusterStateData(t, state_id, value)

    async def update(self):
        cluster_stats = await self.cluster_stats()
        for pool in cluster_stats.values():
            pool_stats = PoolStats.from_dict(pool)
            if pool_stats.name not in self._pool_states:
                self._initialize_pool(pool)
            t = self._pool_tasks[pool_stats.name]
            self._progress.update(t, total=pool_stats.total_cores)
            pool_states = self._pool_states[pool_stats.name]
            for state, state_data in pool_states.items():
                new_value = pool_stats.get_value_from_cluster_state(state)
                state_data.value = new_value
                self._progress.update_state(state_data.task_id, state_data.state_id, completed=new_value)


class BatchStatusTable:
    def __init__(self, batch_client, refresh_rate: int = 10):
        self.batch_client = batch_client
        self.progress_table = Table.grid()
        self.refresh_rate = refresh_rate

        self.cluster_capacity_progress = ClusterCapacityProgress(batch_client)
        self.progress_table.add_row(
            Panel.fit(
                self.cluster_capacity_progress._progress,
                title="[b]Cluster Capacity",
                border_style="black",
                padding=(1, 2)
            ),
        )

        self._live = Live(self.progress_table, refresh_per_second=10)
        self._live_update_task = None

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

    async def update(self):
        while True:
            await self.cluster_capacity_progress.update()
            self._live.update(self.progress_table, refresh=True)
            await asyncio.sleep(self.refresh_rate)

    async def __aenter__(self):
        await self.cluster_capacity_progress.initialize()
        self._live.__enter__()
        self._live_task = asyncio.ensure_future(self.update())

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self._live_update_task is not None:
            self._live_update_task.cancel()
            self._live_update_task = None
        self._live.__exit__(exc_type, exc_val, exc_tb)


async def async_monitor(include_cluster_stats: bool = True,
                        include_batch_progress: bool = True,
                        batch_id: Optional[int] = None):
    from hailtop.batch_client.aioclient import BatchClient  # pylint: disable=import-outside-toplevel

    async with await BatchClient.create('') as client:
        async with BatchStatusTable(client, refresh_rate=10):
            while True:
                await asyncio.sleep(300)
