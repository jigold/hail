import asyncio
import os
from shlex import quote as shq
import sys
import tempfile
from typing import List

from hailtop import pip_version
from hailtop.utils import CalledProcessError, async_to_blocking, filter_none, secret_alnum_string


def init_parser(parser):
    parser.add_argument('project', type=str, help="Project to create the Mini-Batch cluster in")
    parser.add_argument('--cluster-state-bucket', type=str, help="Location to store cluster state files")


def main(args, pass_through_args):  # pylint: disable=unused-argument
    if args.cluster_state_bucket is None:
        cluster_state_bucket = f'{args.project}-mini-batch-cluster-state'
    else:
        cluster_state_bucket = args.cluster_state_bucket

    print(f'retrieving cluster state from gs://{cluster_state_bucket}')

    def main_tf(prefix: str):
        return f'''
terraform {{
  required_providers {{
    google = {{
      source = "hashicorp/google"
      version = "3.48.0"
    }}
  }}

  backend "gcs" {{
#    bucket  = # Set with -backend-config "bucket=BUCKET"
    prefix  = "{prefix}"
  }}
}}
'''

    for prefix in ('mini-batch/terraform/infra/state', 'mini-batch/terraform/driver/state'):
        with tempfile.TemporaryDirectory() as tmp:
            print(f'initializing terraform files in {tmp}')
            tf = main_tf(prefix)
            with open(f'{tmp}/main.tf', 'w') as f:
                f.write(tf + '/n')
            os.system(f'''
set -ex
cd {tmp}
terraform init -backend-config "bucket={cluster_state_bucket}"
terraform destroy -target google_compute_instance.driver
''')

        #os.system(f'gsutil rm gs://{cluster_state_bucket}/{prefix}/default.tfstate')
