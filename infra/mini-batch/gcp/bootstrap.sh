#! /bin/bash

set -ex

IP_ADDRESS=$(curl -s -H "Metadata-Flavor: Google" "http://metadata.google.internal/computeMetadata/v1/instance/network-interfaces/0/ip")
EXTERNAL_IP=$(curl -s -H "Metadata-Flavor: Google" "http://metadata.google.internal/computeMetadata/v1/instance/network-interfaces/0/access-configs/0/external-ip")
HOSTNAME=$(hostname)

REGION=$(curl -s -H "Metadata-Flavor: Google" "http://metadata.google.internal/computeMetadata/v1/instance/attributes/region")
OAUTH2_CREDENTIALS_FILE=$(curl -s -H "Metadata-Flavor: Google" "http://metadata.google.internal/computeMetadata/v1/instance/attributes/oauth2_credentials_file")
BUCKET_STORAGE_CLASS=$(curl -s -H "Metadata-Flavor: Google" "http://metadata.google.internal/computeMetadata/v1/instance/attributes/bucket_storage_class")
BUCKET_LOCATION=$(curl -s -H "Metadata-Flavor: Google" "http://metadata.google.internal/computeMetadata/v1/instance/attributes/bucket_location")
TF_STATE_BUCKET=$(curl -s -H "Metadata-Flavor: Google" "http://metadata.google.internal/computeMetadata/v1/instance/attributes/tf_state_bucket")
ORGANIZATION_DOMAIN=$(curl -s -H "Metadata-Flavor: Google" "http://metadata.google.internal/computeMetadata/v1/instance/attributes/organization_domain")
USERNAME=$(curl -s -H "Metadata-Flavor: Google" "http://metadata.google.internal/computeMetadata/v1/instance/attributes/username")
EMAIL=$(curl -s -H "Metadata-Flavor: Google" "http://metadata.google.internal/computeMetadata/v1/instance/attributes/email")
DB_CORES=$(curl -s -H "Metadata-Flavor: Google" "http://metadata.google.internal/computeMetadata/v1/instance/attributes/db_cores")
DB_MEMORY=$(curl -s -H "Metadata-Flavor: Google" "http://metadata.google.internal/computeMetadata/v1/instance/attributes/db_memory")

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

# Install Docker
curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo gpg --dearmor -o /usr/share/keyrings/docker-archive-keyring.gpg
echo \
    "deb [arch=$(dpkg --print-architecture) signed-by=/usr/share/keyrings/docker-archive-keyring.gpg] https://download.docker.com/linux/ubuntu \
    $(lsb_release -cs) stable" | sudo tee /etc/apt/sources.list.d/docker.list > /dev/null

sudo apt-get update
sudo apt-get install -y docker-ce docker-ce-cli containerd.io

# Install Minikube
curl -LO https://storage.googleapis.com/minikube/releases/latest/minikube-linux-amd64
sudo install minikube-linux-amd64 /usr/local/bin/minikube

#sudo useradd -m minibatch
#sudo usermod -aG docker minibatch
#sudo usermod -aG sudo minibatch
#sudo newgrp docker

# use bare metal (driver=none) so host network is directly exposed
sudo CHANGE_MINIKUBE_NONE_USER=true minikube start --driver=none

sudo ln -s $(which minikube) /usr/local/bin/kubectl

# add label to node to get around node selectors specified in yaml
sudo kubectl label node $HOSTNAME preemptible=true

gsutil cp ${OAUTH2_CREDENTIALS_FILE} /oauth2_credentials_file

cd /hail/infra/mini-batch/gcp/infra/

tee inputs.tfvars <<EOF
bucket_location = "${BUCKET_LOCATION}"
bucket_storage_class = "${BUCKET_STORAGE_CLASS}"
db_cores = "${DB_CORES}"
db_memory = "${DB_MEMORY}"
tf_state_bucket = "${TF_STATE_BUCKET}"
EOF

# sudo terraform init -backend-config "bucket=${TF_STATE_BUCKET}"
# sudo terraform apply -var-file="inputs.tfvars"

cd ../k8s/

tee inputs.tfvars <<EOF
organization_domain = "${ORGANIZATION_DOMAIN}"
internal_ip = "${IP_ADDRESS}"
external_ip = "${EXTERNAL_IP}"
batch_gcp_regions = ["${REGION}"]
tf_state_bucket = "${TF_STATE_BUCKET}"
EOF

# sudo terraform init
# sudo terraform apply --var-file="inputs.tfvars"

#cd $HAIL/infra
#./install_bootstrap_dependencies.sh
#cd gcp/
#./bootstrap.sh configure_gcloud {gcp_zone}
#MINIBATCH=1 ./bootstrap.sh deploy_unmanaged

#mkdir /global-config
#kubectl -n default get secret global-config -o json | jq -r '.data | map_values(@base64d) | to_entries|map("echo -n \(.value) > /global-config/\(.key)") | .[]' | bash

# create worker boot disk image
#batch/gcp-create-worker-image.sh

# bootstrap deploying batch
#./bootstrap.sh bootstrap {repo}:{branch} deploy_batch  # FIXME support current repo

# modify this file to only have batch, batch-driver???
#$HAIL/letsencrypt/subdomains.txt  # FIXME: will letsencrypt work???
#make -C $HAIL/gateway deploy

#./bootstrap.sh bootstrap {repo}:{branch} create_initial_user ${USERNAME} ${EMAIL}  # FIXME support current repo
