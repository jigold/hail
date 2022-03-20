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
    parser.add_argument('zone', type=str, help="Zone to create the Mini-Batch cluster in")
    parser.add_argument('organization_domain', type=str,
                        help="Name of the organization domain for checking user credentials.")
    parser.add_argument('oauth2_credentials_file', type=str,
                        help="Local location of the OAuth2 credentials for the project.")
    parser.add_argument('username', type=str, help="username of Hail Batch account")
    parser.add_argument('email', type=str, help="login email to use with OAuth2 authentication")

    parser.add_argument('--machine-family', type=str, default='n1', help="Machine family of the driver such as n1, n2")
    parser.add_argument('--machine-type', type=str, default='standard', choices=['highcpu', 'standard', 'highmem'],
                        help="Machine type of the driver")
    parser.add_argument('--cores', type=int, default=4, choices=[4, 8, 16, 32],
                        help="Number of cores of the driver instance")
    parser.add_argument('--boot-disk-size', type=int, default=75,
                        help="Size of the boot disk for the driver instance. Make sure this is at least 75 GB.")

    parser.add_argument('--cluster-state-bucket', type=str, help="Location to store cluster state files")
    parser.add_argument('--bucket-storage-class', type=str, default="REGIONAL",
                        help="Storage class to use when creating new buckets")

    parser.add_argument('--repo', type=str, default='hail-is/hail',
                        help="Name of the Hail repository to create the driver from")

    commit = parser.add_mutually_exclusive_group(required=False)
    commit.add_argument('--branch', type=str, help="Name of the branch to create the driver from", nargs='?')
    commit.add_argument('--pip-version', type=str, help="Name of the pip version to create the driver from.", nargs='?')
    commit.add_argument('--sha', type=str, help="Name of the SHA to create the driver from.", nargs='?')


def main(args, pass_through_args):  # pylint: disable=unused-argument
    commit = (filter_none([args.branch, args.pip_version, args.sha]) + [pip_version()])[0]

    region = args.zone.rsplit('-', maxsplit=1)[0]

    if args.cluster_state_bucket is None:
        cluster_state_bucket = f'{args.project}-mini-batch-cluster-state'
        try:
            os.system(f'gsutil ls gs://{cluster_state_bucket}')
        except:
            print(f'bucket gs://{cluster_state_bucket} is not accessible. trying to create it.')
            try:
                os.system(f'gsutil mb -p {args.project} -c {args.bucket_storage_class} -l {region} gs://{cluster_state_bucket}')
            except:
                print(f'error creating bucket gs://{cluster_state_bucket}')
                raise
    else:
        cluster_state_bucket = args.cluster_state_bucket

    print(f'storing cluster state in gs://{cluster_state_bucket}')

    print(f'enabling services')
    os.system(f'''
gcloud --project {args.project} services enable \
    container.googleapis.com \
    compute.googleapis.com \
    cloudresourcemanager.googleapis.com \
    servicenetworking.googleapis.com \
    sqladmin.googleapis.com \
    serviceusage.googleapis.com \
    logging.googleapis.com \
    iam.googleapis.com \
    artifactregistry.googleapis.com
''')

    # FIXME: read oauth2 credentials in terraform?
    remote_oauth2_credentials_file = f'gs://{cluster_state_bucket}/mini-batch/oauth2/oauth2_credentials_file'
    os.system(f'gsutil cp {shq(os.path.expanduser(args.oauth2_credentials_file))} {shq(remote_oauth2_credentials_file)}')

    with tempfile.TemporaryDirectory() as tmp:
        print(f'initializing terraform files in {tmp}')

        tf_dir = os.path.dirname(os.path.realpath(__file__)) + '/infra/gcp'
        print(tf_dir)

        os.system(f'cp {tf_dir}/main.tf {tmp}/main.tf')
        os.system(f'cp {tf_dir}/variables.tf {tmp}/variables.tf')
        os.system(f'cp {tf_dir}/outputs.tf {tmp}/outputs.tf')

        with open(f'{tmp}/inputs.tfvars', 'w') as f:
            f.write(f'''
cluster_name = "{args.project}"
gcp_project = "{args.project}"
gcp_region = "{region}"
gcp_zone = "{args.zone}"
gcp_location = "{region}"  # FIXME
organization_domain = "{args.organization_domain}"
username = "{args.username}"
email = "{args.email}"
n_cores = {args.cores}
boot_disk_size = {args.boot_disk_size}
machine_family = "{args.machine_family}"
machine_type = "{args.machine_type}"
# FIXME: db variables
bucket_storage_class = "{args.bucket_storage_class}"
bucket_location = "{region}"
minikube_memory_mib = {3500}
oauth2_credentials_file = "{remote_oauth2_credentials_file}"
repo = "{args.repo}"
commit = "{commit}"
tf_state_bucket = "{cluster_state_bucket}"
''')

        os.system(f'''
set -ex
cd {tmp}
terraform init -backend-config "bucket={cluster_state_bucket}"
terraform apply -auto-approve -var-file inputs.tfvars
''')

    # SSH connect to runner to see progress???