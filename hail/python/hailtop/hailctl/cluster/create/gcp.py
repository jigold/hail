import asyncio
import os
from shlex import quote as shq
import sys
import tempfile
from typing import List

from hailtop import pip_version
from hailtop.utils import CalledProcessError, async_to_blocking, filter_none, secret_alnum_string, check_shell_output, check_shell
from hailtop.aiocloud import aiogoogle


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


async def run(args, pass_through_args):  # pylint: disable=unused-argument

    runner_vm_name = f'mini-batch-{args.project}-runner'
    runner_sa_name = f'mini-batch-runner@{args.project}.iam.gserviceaccount.com'
    commit = (filter_none([args.branch, args.pip_version, args.sha]) + [pip_version()])[0]

    region = args.zone.rsplit('-', maxsplit=1)[0]

    if args.cluster_state_bucket is None:
        cluster_state_bucket = f'{args.project}-mini-batch-cluster-state'
        try:
            await check_shell_output(f'gsutil ls gs://{cluster_state_bucket}')
        except CalledProcessError:
            print(f'bucket gs://{cluster_state_bucket} is not accessible. trying to create it.')
            try:
                await check_shell_output(f'gsutil mb -p {args.project} -c {args.bucket_storage_class} -l {region} gs://{cluster_state_bucket}')
            except CalledProcessError:
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

    remote_oauth2_credentials_file = f'gs://{cluster_state_bucket}/mini-batch/oauth2/oauth2_credentials_file'
    await check_shell_output(f'''
gsutil cp {shq(os.path.expanduser(args.oauth2_credentials_file))} {shq(remote_oauth2_credentials_file)}
''')

    with tempfile.TemporaryDirectory() as tmp:
        runner_script = f'{tmp}/runner.sh'
        with open(runner_script, 'w') as f:
            f.write(f'''
sudo tee run.sh <<EOF
#! /bin/bash
set -x

touch started
gsutil cp started gs://${{TF_STATE_BUCKET}}/mini-batch/setup-driver/started

NAME=$(curl -s -H 'Metadata-Flavor: Google' http://metadata.google.internal/computeMetadata/v1/instance/name)
ZONE=$(curl -s -H 'Metadata-Flavor: Google' http://metadata.google.internal/computeMetadata/v1/instance/zone)
PROJECT=$(curl -s -H "Metadata-Flavor: Google" "http://metadata.google.internal/computeMetadata/v1/project/project-id")
REPO=$(curl -s -H "Metadata-Flavor: Google" "http://metadata.google.internal/computeMetadata/v1/instance/attributes/repo")
COMMIT=$(curl -s -H "Metadata-Flavor: Google" "http://metadata.google.internal/computeMetadata/v1/instance/attributes/commit")
TF_STATE_BUCKET=$(curl -s -H "Metadata-Flavor: Google" "http://metadata.google.internal/computeMetadata/v1/instance/attributes/tf_state_bucket")

sudo apt-get update
sudo apt-get install -y git

git clone https://github.com/$REPO.git
cd hail/
git checkout "$COMMIT"
cd /

sudo apt-get update

sudo apt-get install -y \
    apt-transport-https \
    ca-certificates \
    conntrack \
    curl \
    emacs-nox \
    gnupg \
    jq \
    lsb-release \
    net-tools \
    software-properties-common

# Install Terraform
curl -fsSL https://apt.releases.hashicorp.com/gpg | sudo apt-key add -
sudo apt-add-repository "deb [arch=amd64] https://apt.releases.hashicorp.com $(lsb_release -cs) main"
sudo apt-get update
sudo apt-get install terraform

# Setup driver
sh /hail/infra/mini-batch/gcp/setup.sh

gsutil cp run.log gs://${{TF_STATE_BUCKET}}/mini-batch/setup-driver/log

touch done
gsutil cp done gs://${{TF_STATE_BUCKET}}/mini-batch/setup-driver/done

gcloud --project $PROJECT -q compute instances delete $NAME --zone=$ZONE
EOF

sudo nohup /bin/bash run.sh >run.log 2>&1 & 
''')

        try:
            os.system(f'''
gcloud --project {args.project} iam service-accounts create {runner_sa_name} --display-name "{runner_sa_name}" && \
    gcloud --project {args.project} projects add-iam-policy-binding {args.project} --member='serviceAccount:{runner_sa_name}' --role='owner'
''')

            await check_shell_output(f'''
gcloud compute instances create {runner_vm_name} \
    --project {args.project} \
    --image-project ubuntu-os-cloud \
    --image-family ubuntu-minimal-2004-lts \
    --image ubuntu-minimal-2004-focal-v20220308 \
    --boot-disk-size 20 \
    --boot-disk-type pd-ssd \
    --labels mini-batch-runner \
    --machine-type n1-standard-1 \
    --metadata region="{region}" \
    --metadata organization_domain="{args.organization_domain}" \
    --metadata username="{args.username}" \
    --metadata email="{args.email}" \
    --metadata bucket_storage_class="{args.bucket_storage_class}" \
    --metadata bucket_location="{region}" \
    --metadata oauth2_credentials_file="{remote_oauth2_credentials_file}" \
    --metadata repo="{args.repo}" \
    --metadata commit="{commit}" \
    --metadata tf_state_bucket="{cluster_state_bucket}" \
    --metadata driver_machine_type="{args.machine_type}" \
    --metadata-from-file startup-script={runner_script} \
    --no-restart-on-failure \
    --tags mini-batch-runner \
    --zone {args.zone} \
    --scopes cloud-platform \
    --service-account {runner_sa_name}
''')

            async def wait_for_file_exists(file):
                while True:
                    try:
                        await check_shell(f'gsutil -q stat {file}')
                    except CalledProcessError:
                        pass
                    else:
                        return

            await asyncio.wait_for(wait_for_file_exists(f'gs://${cluster_state_bucket}/mini-batch/setup-driver/started'), timeout=120)

            complete_task = asyncio.wait_for(wait_for_file_exists(f'gs://${cluster_state_bucket}/mini-batch/setup-driver/started'), timeout=20 * 60)
            stream_logs_task = check_shell_output(f'''
gcloud --project {shq(args.project)} compute ssh {shq(runner_vm_name)} --command "sudo tail -f /run.log"
''')

            try:
                await asyncio.wait(complete_task, stream_logs_task, return_when=asyncio.FIRST_COMPLETED)
                if complete_task.done():
                    await complete_task.result()
            finally:
                for task in (complete_task, stream_logs_task):
                    task.cancel()
        finally:
            try:
                await check_shell_output(f'''
gcloud --project {shq(args.project)} -q compute instances delete {runner_vm_name} --zone={shq(args.zone)}
''')
            finally:
                await check_shell_output(f'''
gcloud --project {shq(args.project)} iam service-accounts delete {shq(runner_sa_name)}
''')

def main(args, pass_through_args):
    async_to_blocking(run(args, pass_through_args))
