from typing import Optional
import os
import logging
import asyncio
import uuid

from hailtop.utils import check_shell

from .config import CORES, HAIL_SERVICES, NAMESPACE, INTERNAL_GATEWAY_IP, INTERNET_INTERFACE, CLOUD_WORKER_API, CLOUD

log = logging.getLogger('network')

IPTABLES_WAIT_TIMEOUT_SECS = 60
N_SLOTS = 4 * CORES  # Jobs are allowed at minimum a quarter core

port_allocator: Optional['PortAllocator'] = None
network_allocator: Optional['NetworkAllocator'] = None


class PortAllocator:
    def __init__(self):
        self.ports = asyncio.Queue()
        port_base = 46572
        for port in range(port_base, port_base + 10):
            self.ports.put_nowait(port)

    async def allocate(self):
        return await self.ports.get()

    def free(self, port):
        self.ports.put_nowait(port)


class NetworkNamespace:
    def __init__(self, subnet_index: int, private: bool, internet_interface: str):
        assert subnet_index <= 255
        self.subnet_index = subnet_index
        self.private = private
        self.internet_interface = internet_interface
        self.network_ns_name = uuid.uuid4().hex[:5]
        self.hostname = 'hostname-' + uuid.uuid4().hex[:10]
        self.veth_host = self.network_ns_name + '-host'
        self.veth_job = self.network_ns_name + '-job'

        if private:
            self.host_ip = f'172.20.{subnet_index}.10'
            self.job_ip = f'172.20.{subnet_index}.11'
        else:
            self.host_ip = f'172.21.{subnet_index}.10'
            self.job_ip = f'172.21.{subnet_index}.11'

        self.port = None
        self.host_port = None

    async def init(self):
        await self.create_netns()
        await self.enable_iptables_forwarding()

        os.makedirs(f'/etc/netns/{self.network_ns_name}')
        with open(f'/etc/netns/{self.network_ns_name}/hosts', 'w') as hosts:
            hosts.write('127.0.0.1 localhost\n')
            hosts.write(f'{self.job_ip} {self.hostname}\n')
            if NAMESPACE == 'default':
                for service in HAIL_SERVICES:
                    hosts.write(f'{INTERNAL_GATEWAY_IP} {service}.hail\n')
            hosts.write(f'{INTERNAL_GATEWAY_IP} internal.hail\n')

        # Jobs on the private network should have access to the metadata server
        # and our vdc. The public network should not so we use google's public
        # resolver.
        with open(f'/etc/netns/{self.network_ns_name}/resolv.conf', 'w') as resolv:
            if self.private:
                resolv.write(f'nameserver {CLOUD_WORKER_API.nameserver_ip}\n')
                if CLOUD == 'gcp':
                    resolv.write('search c.hail-vdc.internal google.internal\n')
            else:
                resolv.write('nameserver 8.8.8.8\n')

    async def create_netns(self):
        await check_shell(
            f'''
ip netns add {self.network_ns_name} && \
ip link add name {self.veth_host} type veth peer name {self.veth_job} && \
ip link set dev {self.veth_host} up && \
ip link set {self.veth_job} netns {self.network_ns_name} && \
ip address add {self.host_ip}/24 dev {self.veth_host}
ip -n {self.network_ns_name} link set dev {self.veth_job} up && \
ip -n {self.network_ns_name} link set dev lo up && \
ip -n {self.network_ns_name} address add {self.job_ip}/24 dev {self.veth_job} && \
ip -n {self.network_ns_name} route add default via {self.host_ip}'''
        )

    async def enable_iptables_forwarding(self):
        await check_shell(
            f'''
iptables -w {IPTABLES_WAIT_TIMEOUT_SECS} --append FORWARD --out-interface {self.veth_host} --in-interface {self.internet_interface} --jump ACCEPT && \
iptables -w {IPTABLES_WAIT_TIMEOUT_SECS} --append FORWARD --out-interface {self.veth_host} --in-interface {self.veth_host} --jump ACCEPT'''
        )

    async def expose_port(self, port, host_port):
        self.port = port
        self.host_port = host_port
        await self.expose_port_rule(action='append')

    async def expose_port_rule(self, action: str):
        # Appending to PREROUTING means this is only exposed to external traffic.
        # To expose for locally created packets, we would append instead to the OUTPUT chain.
        await check_shell(
            f'iptables -w {IPTABLES_WAIT_TIMEOUT_SECS} --table nat --{action} PREROUTING '
            f'--match addrtype --dst-type LOCAL '
            f'--protocol tcp '
            f'--match tcp --dport {self.host_port} '
            f'--jump DNAT --to-destination {self.job_ip}:{self.port}'
        )

    async def cleanup(self):
        if self.host_port:
            assert self.port
            await self.expose_port_rule(action='delete')
        self.host_port = None
        self.port = None
        await check_shell(
            f'''
ip link delete {self.veth_host} && \
ip netns delete {self.network_ns_name}'''
        )
        await self.create_netns()


class NetworkAllocator:
    def __init__(self):
        self.private_networks = asyncio.Queue()
        self.public_networks = asyncio.Queue()
        self.internet_interface = INTERNET_INTERFACE

    async def reserve(self):
        for subnet_index in range(N_SLOTS):
            public = NetworkNamespace(subnet_index, private=False, internet_interface=self.internet_interface)
            await public.init()
            self.public_networks.put_nowait(public)

            private = NetworkNamespace(subnet_index, private=True, internet_interface=self.internet_interface)

            await private.init()
            self.private_networks.put_nowait(private)

    async def allocate_private(self) -> NetworkNamespace:
        return await self.private_networks.get()

    async def allocate_public(self) -> NetworkNamespace:
        return await self.public_networks.get()

    def free(self, netns: NetworkNamespace):
        asyncio.ensure_future(self._free(netns))

    async def _free(self, netns: NetworkNamespace):
        await netns.cleanup()
        if netns.private:
            self.private_networks.put_nowait(netns)
        else:
            self.public_networks.put_nowait(netns)


def get_port_allocator():
    global port_allocator
    if port_allocator is None:
        port_allocator = PortAllocator()
    return port_allocator


def get_network_allocator():
    global network_allocator
    if network_allocator is None:
        network_allocator = NetworkAllocator()
    return network_allocator
