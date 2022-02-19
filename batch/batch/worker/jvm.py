from typing import Iterator, Optional, Tuple, List

import asyncio
import logging
import uuid
import os
import concurrent.futures
from contextlib import ExitStack, contextmanager

from hailtop.utils import find_spark_home, blocking_to_async

from .jvm_entryway_protocol import read_bool, read_int, read_str, write_int, write_str, EndOfStream
from .exceptions import JVMUserError


log = logging.getLogger('jvm')


@contextmanager
def scoped_ensure_future(coro_or_future, *, loop=None) -> Iterator[asyncio.Future]:
    fut = asyncio.ensure_future(coro_or_future, loop=loop)
    try:
        yield fut
    finally:
        fut.cancel()


class BufferedOutputProcess:
    @classmethod
    async def create(cls, *args, **kwargs):
        assert 'stdout' not in kwargs
        assert 'stderr' not in kwargs

        process = await asyncio.create_subprocess_exec(
            *args, **kwargs, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
        )
        stop_event = asyncio.Event()
        return cls(process, stop_event)

    def __init__(self, process, stop_event: asyncio.Event):
        self.process = process
        self.stop_event = stop_event
        self.buf = bytearray()
        assert process.stdout is not None
        self.stdout_pump = asyncio.ensure_future(self.pump_to_buffer(process.stdout))
        assert process.stderr is not None
        self.stderr_pump = asyncio.ensure_future(self.pump_to_buffer(process.stderr))

    async def pump_to_buffer(self, strm: asyncio.StreamReader):
        with scoped_ensure_future(self.stop_event.wait()) as stop_fut:
            while not strm.at_eof() and not self.stop_event.is_set():
                with scoped_ensure_future(strm.readline()) as read_fut:
                    await asyncio.wait([read_fut, stop_fut], return_when=asyncio.FIRST_COMPLETED)
                    if read_fut.done():
                        result = read_fut.result()
                        self.buf.extend(result)

    def output(self) -> str:
        return self.buf.decode()

    def retrieve_and_clear_output(self) -> str:
        buf = self.buf.decode()
        self.buf = bytearray()
        return buf

    def kill(self):
        return self.process.kill()

    @property
    def returncode(self) -> Optional[int]:
        return self.process.returncode

    def close(self):
        try:
            self.kill()
        finally:
            try:
                self.stdout_pump.cancel()
            finally:
                self.stderr_pump.cancel()


class JVM:
    SPARK_HOME = find_spark_home()

    FINISH_USER_EXCEPTION = 0
    FINISH_ENTRYWAY_EXCEPTION = 1
    FINISH_NORMAL = 2
    FINISH_CANCELLED = 3
    FINISH_JVM_EOS = 4

    @classmethod
    async def create_process(cls, socket_file: str) -> BufferedOutputProcess:
        # JVM and Hail both treat MB as 1024 * 1024 bytes.
        # JVMs only start in standard workers which have 3.75 GiB == 3840 MiB per core.
        # We only allocate 3700 MiB so that we stay well below the machine's max memory.
        # We allocate 60% of memory per core to off heap memory: 1480 + 2220 = 3700.
        return await BufferedOutputProcess.create(
            'java',
            '-Xmx1480M',
            '-cp',
            f'/jvm-entryway:/jvm-entryway/junixsocket-selftest-2.3.3-jar-with-dependencies.jar:{JVM.SPARK_HOME}/jars/*',
            'is.hail.JVMEntryway',
            socket_file,
            env={'HAIL_WORKER_OFF_HEAP_MEMORY_PER_CORE_MB': '2220'},
        )

    @classmethod
    async def create_process_and_connect(cls, index: int, socket_file: str) -> Tuple[BufferedOutputProcess, str]:
        process = await cls.create_process(socket_file)
        try:
            attempts = 0
            delay = 0.25
            while True:
                try:
                    log.info(f'JVM-{index}: trying to open socket')
                    reader, writer = await asyncio.open_unix_connection(socket_file)
                    try:
                        log.info(f'JVM-{index}: establishing connection')
                        b = await read_bool(reader)
                        assert b, f'expected true, got {b}'
                        writer.write(b'\0x01')
                        break
                    finally:
                        writer.close()
                except ConnectionRefusedError:
                    output = process.retrieve_and_clear_output()
                    log.warning(f'JVM-{index}: connection refused. {output}')
                    raise
                except FileNotFoundError as err:
                    attempts += 1
                    if attempts == 240:
                        raise ValueError(
                            f'JVM-{index}: failed to establish connection after {240 * delay} seconds'
                        ) from err
                    await asyncio.sleep(delay)
            startup_output = process.retrieve_and_clear_output()
            return process, startup_output
        except:
            process.close()
            raise

    @classmethod
    async def create(cls, index: int, pool: concurrent.futures.ThreadPoolExecutor):
        while True:
            try:
                token = uuid.uuid4().hex
                socket_file = '/socket-' + token
                root_dir = '/root-' + token
                output_file = root_dir + '/output'
                should_interrupt = asyncio.Event()
                await blocking_to_async(pool, os.mkdir, root_dir)
                process, startup_output = await cls.create_process_and_connect(index, socket_file)
                log.info(f'JVM-{index}: startup output: {startup_output}')
                return cls(index, socket_file, root_dir, output_file, should_interrupt, process)
            except ConnectionRefusedError:
                pass

    async def new_connection(self):
        while True:
            try:
                interim_output = self.process.retrieve_and_clear_output()
                if len(interim_output) > 0:
                    log.warning(f'{self}: unexpected output between jobs')

                return await asyncio.open_unix_connection(self.socket_file)
            except ConnectionRefusedError:
                log.warning(f'{self}: unexpected exit between jobs', extra=dict(output=self.process.output()))
                os.remove(self.socket_file)
                process, startup_output = await self.create_process_and_connect(self.index, self.socket_file)
                self.process = process
                log.info(f'JVM-{self.index}: startup output: {startup_output}')

    def __init__(
        self,
        index: int,
        socket_file: str,
        root_dir: str,
        output_file: str,
        should_interrupt: asyncio.Event,
        process: BufferedOutputProcess
    ):
        self.index = index
        self.socket_file = socket_file
        self.root_dir = root_dir
        self.output_file = output_file
        self.should_interrupt = should_interrupt
        self.process = process

    def __str__(self):
        return f'JVM-{self.index}'

    def __repr__(self):
        return f'JVM-{self.index}'

    def interrupt(self):
        self.should_interrupt.set()

    def reset(self):
        self.should_interrupt.clear()

    def kill(self):
        if self.process is not None:
            self.process.kill()

    def output(self) -> str:
        return self.process.output()

    def retrieve_and_clear_output(self) -> str:
        return self.process.retrieve_and_clear_output()

    async def execute(self, classpath: str, scratch_dir: str, command_string: List[str]):
        # assert self.worker is not None  # FIXME: why is this assertion needed

        log.info(f'{self}: execute')

        with ExitStack() as stack:
            reader: asyncio.StreamReader
            writer: asyncio.StreamWriter
            reader, writer = await self.new_connection()
            stack.callback(writer.close)
            log.info(f'{self}: connection acquired')

            command_string = [classpath, 'is.hail.backend.service.Main', scratch_dir, *command_string]

            write_int(writer, len(command_string))
            for arg in command_string:
                assert isinstance(arg, str)
                write_str(writer, arg)
            await writer.drain()

            wait_for_message_from_process: asyncio.Future = asyncio.ensure_future(read_int(reader))
            stack.callback(wait_for_message_from_process.cancel)
            wait_for_interrupt: asyncio.Future = asyncio.ensure_future(self.should_interrupt.wait())
            stack.callback(wait_for_interrupt.cancel)

            await asyncio.wait([wait_for_message_from_process, wait_for_interrupt], return_when=asyncio.FIRST_COMPLETED)

            if wait_for_interrupt.done():
                await wait_for_interrupt  # retrieve exceptions
                if not wait_for_message_from_process.done():
                    write_int(writer, 0)  # tell process to cancel
                    await writer.drain()

            eos_exception = None
            try:
                message = await wait_for_message_from_process
            except EndOfStream as exc:
                try:
                    self.kill()
                except ProcessLookupError:
                    log.warning(f'{self}: JVM died after we received EOS')
                message = JVM.FINISH_JVM_EOS
                eos_exception = exc

            if message == JVM.FINISH_NORMAL:
                log.info(f'{self}: finished normally (interrupted: {wait_for_interrupt.done()})')
            elif message == JVM.FINISH_CANCELLED:
                assert wait_for_interrupt.done()
                log.info(f'{self}: was cancelled')
            elif message == JVM.FINISH_USER_EXCEPTION:
                log.info(f'{self}: user exception encountered (interrupted: {wait_for_interrupt.done()})')
                exception = await read_str(reader)
                raise JVMUserError(exception)
            elif message == JVM.FINISH_ENTRYWAY_EXCEPTION:
                log.info(f'{self}: entryway exception encountered (interrupted: {wait_for_interrupt.done()})')
                exception = await read_str(reader)
                raise ValueError(exception)
            elif message == JVM.FINISH_JVM_EOS:
                assert eos_exception is not None
                log.warning(f'{self}: unexpected end of stream in jvm (interrupted: {wait_for_interrupt.done()})')
                raise ValueError('unexpected end of stream in jvm') from eos_exception
