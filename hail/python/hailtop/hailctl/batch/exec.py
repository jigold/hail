import os
import argparse
from subprocess import Popen

from .batch_cli_utils import get_job_if_exists


def init_parser(parser):
    parser.add_argument('--public-ssh-key', type=str, help="File path to SSH public key", default='~/.ssh/id_rsa.pub')
    parser.add_argument('--private-ssh-key', type=str, help="File path to SSH private key", default='~/.ssh/id_rsa')
    parser.add_argument('batch_id', type=int, help="ID number of the desired batch")
    parser.add_argument('job_id', type=int, help="ID number of the desired job")
    parser.add_argument('cmd', nargs=argparse.REMAINDER)


def main(args, pass_through_args, client):  # pylint: disable=unused-argument
    maybe_job = get_job_if_exists(client, args.batch_id, args.job_id)
    if maybe_job is None:
        print(f"Job with ID {args.job_id} on batch {args.batch_id} not found")
        return

    public_key_path = os.path.abspath(os.path.expanduser(args.public_ssh_key))
    with open(public_key_path, 'r') as f:
        public_key = f.read()

    login_info = maybe_job.login(public_key)

    user = login_info['user']
    host = login_info['host']
    port = login_info['port']

    cmd = ' '.join(args.cmd)

    ssh_command = f'ssh -t -p {port} -i {args.private_ssh_key} {user}@{host}'
    if cmd:
        ssh_command += f' -- {cmd}'
    else:
        ssh_command += ' /bin/bash'

    print(ssh_command)
