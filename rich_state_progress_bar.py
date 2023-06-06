import collections
import random
from time import sleep
from typing import List, Optional, Any, Union, Dict, Deque
from threading import Event, RLock, Thread
from functools import lru_cache
import math

from rich.live import Live
from rich.panel import Panel
from rich.progress import TimeElapsedColumn, RenderableType, Group, GetTimeCallable, Progress, ProgressSample, TransferSpeedColumn, SpinnerColumn, BarColumn, TextColumn, ProgressColumn, StyleType, Column, Task, ProgressBar, JupyterMixin, TaskID
from rich.segment import Segment, SegmentLines, Segments
from rich.table import Table
from rich.text import Text


import math
from functools import lru_cache
from time import monotonic
from typing import Iterable, List, Optional

from rich.color import Color, blend_rgb
from rich.color_triplet import ColorTriplet
from rich.console import Console, ConsoleOptions, RenderResult
from rich.jupyter import JupyterMixin
from rich.measure import Measurement
from rich.segment import Segment
from rich.style import Style, StyleType

# Number of characters before 'pulse' animation repeats
PULSE_SIZE = 20


class CustomState:
    def __init__(self, description: str, val: float, style: StyleType):
        self.description = description
        self.val = val
        self.style = style


class CustomProgressBar(JupyterMixin):
    def __init__(
        self,
        total: Optional[float] = 100.0,
        states: Optional[List[CustomState]] = None,
        width: Optional[int] = None,
        pulse: bool = False,
        style: StyleType = "bar.back",
        finished_style: StyleType = "bar.finished",
        pulse_style: StyleType = "bar.pulse",
        animation_time: Optional[float] = None,
    ):
        self.total = total
        self.states = states or []
        self.width = width
        self.pulse = pulse
        self.style = style
        self.finished_style = finished_style
        self.pulse_style = pulse_style
        self.animation_time = animation_time

        self._pulse_segments: Optional[List[Segment]] = None

    def __repr__(self) -> str:
        return f"<Bar {self.completed!r} of {self.total!r}>"

    @property
    def completed(self):
        if self.states:
            return sum(s.val for s in self.states)
        return 0

    @property
    def percentage_completed(self) -> Optional[float]:
        """Calculate percentage complete."""
        if self.total is None:
            return None
        completed = (self.completed / self.total) * 100.0
        completed = min(100, max(0.0, completed))
        return completed

    @lru_cache(maxsize=16)
    def _get_pulse_segments(
        self,
        fore_style: Style,
        back_style: Style,
        color_system: str,
        no_color: bool,
        ascii: bool = False,
    ) -> List[Segment]:
        """Get a list of segments to render a pulse animation.

        Returns:
            List[Segment]: A list of segments, one segment per character.
        """
        bar = "-" if ascii else "━"
        segments: List[Segment] = []
        if color_system not in ("standard", "eight_bit", "truecolor") or no_color:
            segments += [Segment(bar, fore_style)] * (PULSE_SIZE // 2)
            segments += [Segment(" " if no_color else bar, back_style)] * (
                PULSE_SIZE - (PULSE_SIZE // 2)
            )
            return segments

        append = segments.append
        fore_color = (
            fore_style.color.get_truecolor()
            if fore_style.color
            else ColorTriplet(255, 0, 255)
        )
        back_color = (
            back_style.color.get_truecolor()
            if back_style.color
            else ColorTriplet(0, 0, 0)
        )
        cos = math.cos
        pi = math.pi
        _Segment = Segment
        _Style = Style
        from_triplet = Color.from_triplet

        for index in range(PULSE_SIZE):
            position = index / PULSE_SIZE
            fade = 0.5 + cos((position * pi * 2)) / 2.0
            color = blend_rgb(fore_color, back_color, cross_fade=fade)
            append(_Segment(bar, _Style(color=from_triplet(color))))
        return segments

    def update(self, total: Optional[float] = None) -> None:
        """Update progress with new values.

        Args:
            completed (float): Number of steps completed.
            total (float, optional): Total number of steps, or ``None`` to not change. Defaults to None.
        """
        self.total = total if total is not None else self.total

    def _render_pulse(
        self, console: Console, width: int, ascii: bool = False
    ) -> Iterable[Segment]:
        """Renders the pulse animation.

        Args:
            console (Console): Console instance.
            width (int): Width in characters of pulse animation.

        Returns:
            RenderResult: [description]

        Yields:
            Iterator[Segment]: Segments to render pulse
        """
        fore_style = console.get_style(self.pulse_style, default="white")
        back_style = console.get_style(self.style, default="black")

        pulse_segments = self._get_pulse_segments(
            fore_style, back_style, console.color_system, console.no_color, ascii=ascii
        )
        segment_count = len(pulse_segments)
        current_time = (
            monotonic() if self.animation_time is None else self.animation_time
        )
        segments = pulse_segments * (int(width / segment_count) + 2)
        offset = int(-current_time * 15) % segment_count
        segments = segments[offset : offset + width]
        yield from segments

    def __rich_console__(
        self, console: Console, options: ConsoleOptions
    ) -> RenderResult:

        width = min(self.width or options.max_width, options.max_width)
        ascii = options.legacy_windows or options.ascii_only
        should_pulse = self.pulse or self.total is None
        if should_pulse:
            yield from self._render_pulse(console, width, ascii=ascii)
            return

        # completed = 0
        segments = []
        total_bar_count = 0
        total_half_bar_count = 0

        bar = "-" if ascii else "█"  # "━"
        half_bar_right = " " if ascii else "█"  # "╸"
        half_bar_left = " " if ascii else "█" # "╺"

        _Segment = Segment

        for state in self.states:
            completed = (
                min(self.total, max(0, state.val)) if self.total is not None else None
            )

            complete_halves = (
                int(width * 2 * completed / self.total)
                if self.total and completed is not None
                else width * 2
            )
            bar_count = complete_halves // 2
            total_bar_count += bar_count
            half_bar_count = complete_halves % 2
            total_half_bar_count += half_bar_count

            # is_finished = self.total is None or self.completed >= self.total
            # complete_style = console.get_style(
            #     self.finished_style if is_finished else completed_state.style
            # )
            if bar_count:
                segments.append(_Segment(bar * bar_count, state.style))
            if half_bar_count:
                segments.append(_Segment(half_bar_right * half_bar_count, state.style))

        style = console.get_style(self.style)

        if not console.no_color:
            remaining_bars = width - total_bar_count - total_half_bar_count
            if remaining_bars and console.color_system is not None:
                if not total_half_bar_count and total_bar_count:
                    segments.append(_Segment(half_bar_left, style))
                    remaining_bars -= 1
                if remaining_bars:
                    segments.append(_Segment(bar * remaining_bars, style))

        assert segments

        yield Segments(segments)

    def __rich_measure__(
        self, console: Console, options: ConsoleOptions
    ) -> Measurement:
        return (
            Measurement(self.width, self.width)
            if self.width is not None
            else Measurement(4, options.max_width)
        )


class CustomStateTask(Task):
    def __init__(self, id: TaskID, description: str, total: Optional[float], _get_time: GetTimeCallable, finished_time: Optional[float] = None,
                 visible: bool = True, fields: Optional[Dict[str, Any]] = None, start_time: Optional[float] = None,
                 stop_time: Optional[float] = None, finished_speed: Optional[float] = None, _lock: Optional[RLock] = None):
        self.id = id
        self.description = description
        self.total = total
        self._get_time = _get_time
        self.finished_time = finished_time
        self.visible = visible
        self.fields = fields or {}
        self.start_time = start_time
        self.stop_time = stop_time
        self.finished_speed = finished_speed
        self.states: List[CustomState] = []
        self._progress = collections.deque(maxlen=1000)
        self._lock = _lock or RLock()

    @property
    def completed(self):
        return self.get_completed()

    @property
    def not_running_or_pending(self):
        if self.states:
            return sum(s.val for s in self.states if s.description != 'running')
        return None

    @property
    def not_running_or_pending_percentage(self):
        if self.not_running_or_pending is not None and self.total:
            return 100 * (self.not_running_or_pending / self.total)
        return None

    def get_completed(self):
        if self.states:
            return sum(s.val for s in self.states)
        return None

    def add_state(self, state: CustomState) -> int:
        self.states.append(state)
        return len(self.states) - 1

    @property
    def speed(self) -> Optional[float]:
        """Optional[float]: Get the estimated speed in steps per second."""
        if self.start_time is None:
            return None
        with self._lock:
            progress = self._progress
            if not progress:
                return None
            total_time = progress[-1].timestamp - progress[0].timestamp
            if total_time == 0:
                return None
            iter_progress = iter(progress)
            next(iter_progress)
            total_completed = sum(sample.completed for sample in iter_progress)
            speed = total_completed / total_time
            return speed

    @property
    def time_remaining(self) -> Optional[float]:
        """Optional[float]: Get estimated time to completion, or ``None`` if no data."""
        if self.finished:
            return 0.0
        speed = self.speed
        if not speed:
            return None
        remaining = self.remaining
        if remaining is None:
            return None
        estimate = math.ceil(remaining / speed)
        return estimate

    def _reset(self) -> None:
        """Reset progress."""
        self._progress.clear()
        self.finished_time = None
        self.finished_speed = None


class CustomMarkCompleteColumn(ProgressColumn):
    def render(self, task: "Task") -> Text:
        """Show data transfer speed."""
        speed = task.finished_speed or task.speed
        if speed is None:
            return Text("?", style="progress.data.speed")
        return Text(f"{speed:>1.0f} jobs/s", style="progress.data.speed")


class CustomBarColumn(ProgressColumn):
    """Renders a visual progress bar.

    Args:
        bar_width (Optional[int], optional): Width of bar or None for full width. Defaults to 40.
        style (StyleType, optional): Style for the bar background. Defaults to "bar.back".
        complete_style (StyleType, optional): Style for the completed bar. Defaults to "bar.complete".
        finished_style (StyleType, optional): Style for a finished bar. Defaults to "bar.finished".
        pulse_style (StyleType, optional): Style for pulsing bars. Defaults to "bar.pulse".
    """

    def __init__(
        self,
        bar_width: Optional[int] = 40,
        style: StyleType = "bar.back",
        complete_style: StyleType = "bar.complete",
        pulse_style: StyleType = "bar.pulse",
        table_column: Optional[Column] = None,
    ) -> None:
        self.bar_width = bar_width
        self.style = style
        self.complete_style = complete_style
        self.pulse_style = pulse_style
        super().__init__(table_column=table_column)

    def render(self, task: "CustomStateTask") -> CustomProgressBar:
        """Gets a progress bar widget for a task."""
        return CustomProgressBar(
            total=max(0, task.total) if task.total is not None else None,
            states=task.states,
            width=None if self.bar_width is None else max(1, self.bar_width),
            pulse=False,
            animation_time=task.get_time(),
            style=self.style,
            pulse_style=self.pulse_style,
        )


class CustomProgress(Progress):
    _tasks: Dict[TaskID, CustomStateTask] = {}

    def update_state(self,
                     task_id: TaskID,
                     state_id: int,
                     *,
                     completed: Optional[float] = None,
                     advance: Optional[float] = None,
                     refresh: bool = False,
                     ):
        with self._lock:
            task = self._tasks[task_id]
            state = task.states[state_id]
            completed_start = task.get_completed()

            if advance is not None:
                state.val += advance
            if completed is not None:
                state.val = completed

            update_completed = task.get_completed() - completed_start

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
                and task.not_running_or_pending >= task.total
                and task.finished_time is None
            ):
                task.finished_time = task.elapsed

        if refresh:
            self.refresh()

    def update(
        self,
        task_id: TaskID,
        *,
        total: Optional[float] = None,
        description: Optional[str] = None,
        visible: Optional[bool] = None,
        refresh: bool = False,
        **fields: Any,
    ) -> None:
        """Update information associated with a task.

        Args:
            task_id (TaskID): Task id (returned by add_task).
            total (float, optional): Updates task.total if not None.
            description (str, optional): Change task description if not None.
            visible (bool, optional): Set visible flag if not None.
            refresh (bool): Force a refresh of progress information. Default is False.
            **fields (Any): Additional data fields required for rendering.
        """
        with self._lock:
            task = self._tasks[task_id]

            if total is not None and total != task.total:
                task.total = total
                task._reset()
            if description is not None:
                task.description = description
            if visible is not None:
                task.visible = visible
            task.fields.update(fields)

            completed_start = task.completed

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

    def add_state(self,
                  task_id: TaskID,
                  description: str,
                  value: float,
                  style: StyleType,
                  ) -> int:
        state = CustomState(description, value, style)
        task = self._tasks[task_id]
        state_id = task.add_state(state)
        self.update(task_id, refresh=False)
        return state_id

    def add_task(
        self,
        description: str,
        start: bool = True,
        total: Optional[float] = 100.0,
        visible: bool = True,
        refresh: bool = False,
        **fields: Any,
    ) -> TaskID:
        """Add a new 'task' to the Progress display.

        Args:
            description (str): A description of the task.
            start (bool, optional): Start the task immediately (to calculate elapsed time). If set to False,
                you will need to call `start` manually. Defaults to True.
            total (float, optional): Number of total steps in the progress if known.
                Set to None to render a pulsing animation. Defaults to 100.
            completed (int, optional): Number of steps completed so far. Defaults to 0.
            visible (bool, optional): Enable display of the task. Defaults to True.
            **fields (str): Additional data fields required for rendering.

        Returns:
            TaskID: An ID you can use when calling `update`.
        """
        with self._lock:
            task = CustomStateTask(
                self._task_index,
                description,
                total,
                visible=visible,
                fields=fields,
                _get_time=self.get_time,
                _lock=self._lock,
            )
            self._tasks[self._task_index] = task
            if start:
                self.start_task(self._task_index)

            tasks = list(self._tasks.values())
            last_tasks = tasks[-5:]
            for task in last_tasks:
                task.visible = True
            other_tasks = tasks[:-5]
            for task in other_tasks:
                task.visible = False
            new_task_index = self._task_index
            self._task_index = TaskID(int(self._task_index) + 1)

        if refresh:
            self.refresh()

        return new_task_index

    def make_legend(self) -> Optional[Table]:
        if not self._tasks:
            return None
        tasks = list(self._tasks.values())
        current_task = tasks[-1]
        columns = [TextColumn('legend: ')]
        all_states = sorted([state for task in tasks for state in task.states], key=lambda x: x.description)

        seen = set()
        for state in all_states:
            description = state.description
            if description not in seen:
                columns.append(TextColumn(f"━ {description}", state.style))
                seen.add(description)

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

    def make_previous_tasks_table(self) -> Optional[Table]:
        if not self._tasks:
            return None
        tasks = list(self._tasks.values())
        current_task = tasks[0]  # dummy
        previous_n_tasks = len(tasks) - 5
        columns = [TextColumn(f'{previous_n_tasks} task(s) previously completed')]

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

    def get_renderable(self) -> RenderableType:
        """Get a renderable for the progress display."""
        renderable = Group(*self.get_renderables())
        return renderable

    def get_renderables(self) -> Iterable[RenderableType]:
        """Get a number of renderables for the progress display."""
        if len(self.tasks) > 5:
            yield self.make_previous_tasks_table()
            empty_table = Table.grid(padding=(0, 2), expand=self.expand)
            yield empty_table
        table1 = self.make_tasks_table(self.tasks)
        yield table1
        empty_table = Table.grid(padding=(0, 2), expand=self.expand)
        yield empty_table
        table2 = self.make_legend()
        if table2:
            yield table2


job_progress1 = CustomProgress(
    "{task.description}",
    CustomBarColumn(),
    TextColumn("[progress.percentage]{task.not_running_or_pending_percentage:>3.0f}%"),
    TextColumn("[progress.completed]{task.not_running_or_pending}/{task.total} jobs"),
    CustomMarkCompleteColumn(),
    TimeElapsedColumn(),
    SpinnerColumn(style=Style(color=Color.from_rgb(50, 175, 255))),
)

task_names = ['matrix_type(...)', 'exec(...)', 'exec(...)', 'foo(...)', 'table_type(...)', 'reference_genome(...)',
              'vep(...)', 'collect(...)']

cluster_capacity_progress = CustomProgress(
    "{task.description}",
    CustomBarColumn(),
    TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
    TextColumn("[progress.total]{task.total} cores"),
)

progress_table = Table.grid()
progress_table.add_row(
    Panel.fit(
        cluster_capacity_progress, title="[b]Cluster Capacity", border_style="black", padding=(1, 2)
    ),
)
progress_table.add_row(
    Panel.fit(job_progress1, title="[b]Progress Bar", border_style="black", padding=(1, 2)),
)

with Live(progress_table, refresh_per_second=10):
    t1 = cluster_capacity_progress.add_task("standard", total=1500)
    cluster_capacity_progress.add_state(t1, 'Me', 300, Style(color="magenta"))
    cluster_capacity_progress.add_state(t1, 'Other Users', 600, Style(color="cyan"))
    cluster_capacity_progress.add_state(t1, 'Available', 100, Style(color="green"))
    cluster_capacity_progress.add_state(t1, 'Provisioning', 100, Style(color="yellow"))

    t2 = cluster_capacity_progress.add_task("standard-np", total=100)
    cluster_capacity_progress.add_state(t2, 'Me', 10, Style(color="magenta"))
    cluster_capacity_progress.add_state(t2, 'Other Users', 40, Style(color="cyan"))
    cluster_capacity_progress.add_state(t2, 'Available', 10, Style(color="green"))
    cluster_capacity_progress.add_state(t2, 'Provisioning', 10, Style(color="yellow"))

    for task_name in task_names[:-1]:
        j = job_progress1.add_task(f"{task_name}", total=1500)
        state1 = job_progress1.add_state(j, 'succeeded', 0, Style(color="green"))
        state2 = job_progress1.add_state(j, 'running', 150, Style(color="blue"))
        while not job_progress1.finished:
            sleep(2)
            job_progress1.update_state(j, state1, advance=150)
        job_progress1.stop_task(j)

    job2 = job_progress1.add_task(task_names[-1], total=300)
    state21 = job_progress1.add_state(job2, 'succeeded', 20, Style(color="green"))
    state22 = job_progress1.add_state(job2, 'failed', 5, Style(color="red"))
    state23 = job_progress1.add_state(job2, 'cancelled', 15, Style(color="yellow"))
    state24 = job_progress1.add_state(job2, 'running', 30, Style(color="blue"))

    while not job_progress1.finished:
        rand_num = random.random()
        if rand_num < 0.75:
            job_progress1.update_state(job2, state21, advance=20)
        elif 0.75 <= rand_num <= 0.85:
            job_progress1.update_state(job2, state22, advance=10)
        else:
            job_progress1.update_state(job2, state22, advance=10)
        sleep(10)
    job_progress1.stop_task(job2)

