import math
from typing import Any, Dict, Iterable, List, Optional, Tuple

from rich.console import Console, ConsoleOptions, RenderResult
from rich.progress import (
    BarColumn,
    Column,
    Progress,
    ProgressBar,
    ProgressColumn,
    ProgressSample,
    RenderableType,
    SpinnerColumn,
    Task,
    TaskID,
    TextColumn,
    TimeElapsedColumn,
)
from rich.segment import Segment as _Segment, Segments
from rich.style import Color, Style, StyleType
from rich.table import Table
from rich.text import Text


class State:
    def __init__(self, name: str, style: StyleType):
        self.name = name
        self.style = style
        self.value = 0.0

    def update(self, delta_n: Optional[float] = None, value: Optional[float] = None):
        if delta_n is not None:
            self.value += delta_n
        elif value is not None:
            self.value = value


class StateUpdate:
    def __init__(self, name: str, advance: Optional[float] = None, completed: Optional[float] = None):
        self.name = name
        self.advance = advance
        self.completed = completed


class MultiStateProgressTask(Task):
    def __init__(self, progress: Progress, states: List[State], *args, **kwargs):
        self.progress = progress
        self.states = {s.name: s for s in states}
        super().__init__(*args, **kwargs)

    @property
    def completed(self) -> int:
        if self.states:
            return sum(state.value for state in self.states.values())
        return 0

    # This is a hack to overide an attribute with a property
    @completed.setter
    def completed(self, value):
        pass

    def update_state(self, update: StateUpdate):
        state = self.states[update.name]
        state.update(update.advance, update.completed)

    def get_state(self, name: str) -> Optional[State]:
        return self.states.get(name)


class JobStateProgressTask(MultiStateProgressTask):
    def __init__(self, progress: Progress, *args, **kwargs):
        states = [
            State('succeeded', Style(color="green")),
            State('failed', Style(color="red")),
            State('cancelled', Style(color="yellow")),
            State('running', Style(color="blue"))
        ]
        super().__init__(progress, states, *args, **kwargs)

    @property
    def completed(self):
        if self.states:
            return sum(int(s.value) for s in self.states.values() if s.name != 'running')
        return None

    # This is a hack to overide an attribute with a property
    @completed.setter
    def completed(self, value):
        pass

    @property
    def percentage(self):
        if self.completed is not None and self.total:
            return 100 * (self.completed / self.total)
        return None

    @property
    def finished(self) -> bool:
        return self.completed >= self.total


class BatchJobStateProgressBar(ProgressBar):
    def __init__(self, task: MultiStateProgressTask, *args, **kwargs):
        self.task = task
        super().__init__(*args, **kwargs)

    def __rich_console__(
        self, console: Console, options: ConsoleOptions
    ) -> RenderResult:
        width = min(self.width or options.max_width, options.max_width)
        ascii = options.legacy_windows or options.ascii_only

        segments = []

        total_bar_count = 0
        total_half_bar_count = 0

        bar = "-" if ascii else "██"
        half_bar = " " if ascii else "█"

        for state in self.task.states.values():
            completed = (
                min(self.task.total, max(0, state.value)) if self.task.total is not None else None
            )

            complete_halves = math.ceil(
                (width * completed / self.task.total)
                if self.task.total and completed is not None
                else width
            )

            bar_count = int(complete_halves // 2)
            total_bar_count += bar_count
            half_bar_count = complete_halves % 2
            total_half_bar_count += half_bar_count

            if bar_count:
                segments.append(_Segment(bar * bar_count, state.style))
            if half_bar_count:
                segments.append(_Segment(half_bar * half_bar_count, state.style))

        style = console.get_style(self.style)

        if not console.no_color:
            remaining_half_bars = width - 2 * total_bar_count - total_half_bar_count
            if remaining_half_bars and console.color_system is not None:
                if remaining_half_bars:
                    segments.append(_Segment(half_bar * remaining_half_bars, style))

        assert segments
        yield Segments(segments)


class BatchJobStateBarColumn(BarColumn):
    def render(self, task: MultiStateProgressTask) -> BatchJobStateProgressBar:
        progress_bar = ProgressBar(
            total=max(0, task.total) if task.total is not None else None,
            width=None if self.bar_width is None else max(1, self.bar_width),
        )
        return BatchJobStateProgressBar(task, progress_bar)


class JobStateChangeRateColumn(ProgressColumn):
    def render(self, task: MultiStateProgressTask) -> Text:
        speed = task.finished_speed or task.speed
        if speed is None:
            return Text("?", style="progress.data.speed")
        return Text(f"{speed:>1.0f} jobs/s", style="progress.data.speed")


class BatchJobStateProgress(Progress):
    _tasks: Dict[TaskID, JobStateProgressTask]

    @classmethod
    def get_default_columns(cls) -> Tuple[ProgressColumn, ...]:
        return (
            TextColumn("[progress.description]{task.description}"),
            BatchJobStateBarColumn(),
            TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
            TextColumn("[progress.completed]{task.completed}/{task.total} jobs"),
            JobStateChangeRateColumn(),
            TimeElapsedColumn(),
            SpinnerColumn(style=Style(color=Color.from_rgb(50, 175, 255))),
        )

    def __enter__(self) -> 'BatchJobStateProgress':
        self.start()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        del exc_type
        del exc_value
        del traceback
        try:
            self.refresh()
        finally:
            self.stop()

    def update(
        self,
        task_id: TaskID,
        *,
        state_updates: Optional[List[StateUpdate]] = None,
        total: Optional[float] = None,
        description: Optional[str] = None,
        visible: Optional[bool] = None,
        refresh: bool = False,
        **fields: Any,
    ) -> None:
        with self._lock:
            task = self._tasks[task_id]
            completed_start = task.completed

            if total is not None and total != task.total:
                task.total = total
                task._reset()
            for update in state_updates:
                task.update_state(update)
            if description is not None:
                task.description = description
            if visible is not None:
                task.visible = visible
            task.fields.update(fields)
            update_completed = task.completed - completed_start

            current_time = self.get_time()
            old_sample_time = current_time - self.speed_estimate_period
            _progress = task._progress

            popleft = _progress.popleft
            while _progress and _progress[0].timestamp < old_sample_time:
                popleft()
            if update_completed > 0:
                _progress.append(ProgressSample(current_time, update_completed))
            if (
                task.total is not None
                and task.completed >= task.total
                and task.finished_time is None
            ):
                task.finished_time = task.elapsed

        if refresh:
            self.refresh()

    def with_task(self, *args, **kwargs) -> JobStateProgressTask:
        tid = self.add_task(*args, **kwargs)
        return self._tasks[tid]

    def add_task(
        self,
        description: str,
        start: bool = True,
        total: Optional[float] = 100.0,
        completed: int = 0,
        visible: bool = True,
        **fields: Any,
    ) -> TaskID:
        with self._lock:
            task = JobStateProgressTask(
                self,
                self._task_index,
                description,
                total,
                completed,
                visible=visible,
                fields=fields,
                _get_time=self.get_time,
                _lock=self._lock,
            )
            self._tasks[self._task_index] = task
            if start:
                self.start_task(self._task_index)
            new_task_index = self._task_index
            self._task_index = TaskID(int(self._task_index) + 1)
        self.refresh()
        return new_task_index

    def get_task(self, tid: TaskID) -> JobStateProgressTask:
        return self._tasks.get(tid)

    def make_legend(self) -> Optional[Table]:
        if not self._tasks:
            return None
        tasks = list(self._tasks.values())
        current_task = tasks[-1]
        columns = [TextColumn('legend: ')]

        seen = set()
        for name, state in current_task.states.items():
            if name not in seen:
                columns.append(TextColumn(f"━ {name}", state.style))
                seen.add(name)

        table_columns = (
            (
                Column(no_wrap=True)
                if isinstance(_column, str)
                else _column.get_table_column().copy()
            )
            for _column in columns
        )
        table = Table.grid(*table_columns, padding=(0, 2), expand=self.expand)

        table.add_row(
            *(
                (
                    column.format(task=current_task)
                    if isinstance(column, str)
                    else column(current_task)
                )
                for column in columns
            )
        )

        return table

    def get_renderables(self) -> Iterable[RenderableType]:
        tasks_table = self.make_tasks_table(self.tasks)
        yield tasks_table
        empty_table = Table.grid(padding=(0, 2), expand=self.expand)
        yield empty_table
        legend = self.make_legend()
        if legend:
            yield legend
