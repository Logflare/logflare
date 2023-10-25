GCLOUD_LOCATION ?= us-central1
GCLOUD_KEYRING ?= logflare-keyring-us-central1
GCLOUD_KEY ?= logflare-secrets-key
GCLOUD_PROJECT ?= logflare-staging
ERL_COOKIE ?= monster

ENV ?= dev
SHA_IMAGE_TAG ?= dev-$(shell git rev-parse --short=7 HEAD)
VERSION ?= $(shell cat ./VERSION)
NORMALIZED_VERSION ?= $(shell cat ./VERSION | tr '.' '-')

help:
	@cat DEVELOPMENT.md

test:
	-epmd -daemon
	mix test.coverage

.PHONY: test

setup: setup.node
	mix setup

setup.node:
	npm --prefix ./assets install

.PHONY: setup setup.node

start: start.orange

start.orange: ERL_NAME = orange
start.orange: PORT = 4000
start.orange: LOGFLARE_GRPC_PORT = 50051
start.orange: __start__

start.pink: ERL_NAME = pink
start.pink: PORT = 4001
start.pink: LOGFLARE_GRPC_PORT = 50052
start.pink: __start__

__start__: decrypt.${ENV}
	@env $$(cat .${ENV}.env | xargs) PORT=${PORT} LOGFLARE_GRPC_PORT=${LOGFLARE_GRPC_PORT} iex --sname ${ERL_NAME} --cookie ${ERL_COOKIE} -S mix phx.server

.PHONY: __start__

# Encryption and decryption of secrets
# Usage:
#
#     make decrypt.{dev,staging,prod} # Decrypt secrets for given environment
#     make encrypt.{dev,staging,prod} # Encrypt secrets for given environment

%: cloudbuild/%.enc
	@gcloud kms decrypt --ciphertext-file=$< --plaintext-file=$@ \
		--location=${GCLOUD_LOCATION} \
		--keyring=${GCLOUD_KEYRING} \
		--key=${GCLOUD_KEY} \
		--project=${GCLOUD_PROJECT}
	@echo "$@ has been decrypted"

%.enc:
	@gcloud kms encrypt --ciphertext-file=cloudbuild/$@ --plaintext-file=$(@:.enc=) \
		--location=${GCLOUD_LOCATION} \
		--keyring=${GCLOUD_KEYRING} \
		--key=${GCLOUD_KEY} \
		--project=${GCLOUD_PROJECT}
	@echo "$@ has been encrypted"

.PRECIOUS: %.enc

decrypt.dev: .dev.env
encrypt.dev: .dev.env.enc

.PHONY: decrypt.dev encrypt.dev

envs = staging prod

%crypt.prod: GCLOUD_KEYRING = logflare-prod-keyring-us-central1
%crypt.prod: GCLOUD_KEY = logflare-prod-secrets-key
%crypt.prod: GCLOUD_PROJECT = logflare-232118

.SECONDEXPANSION:
$(addprefix decrypt.,${envs}): decrypt.%: \
	.$$*.gcloud.json \
 	.$$*.env \
 	.$$*.cacert.key \
 	.$$*.cacert.pem \
 	.$$*.cert.key \
 	.$$*.cert.pem \
 	.$$*.db-client-cert.pem \
 	.$$*.db-client-key.pem \
 	.$$*.db-server-ca.pem

$(addprefix encrypt.,${envs}): encrypt.%: \
	.$$*.gcloud.json.enc \
	.$$*.env.enc \
	.$$*.cacert.key.enc \
	.$$*.cacert.pem.enc \
	.$$*.cert.key.enc \
	.$$*.cert.pem.enc \
 	.$$*.db-client-cert.pem.enc \
 	.$$*.db-client-key.pem.enc \
 	.$$*.db-server-ca.pem.enc

.PHONY: $(addprefix encrypt.,${envs})
.PHONY: $(addprefix decrypt.,${envs})

# OpenTelemetry Protobufs

grpc.protoc:
	dir=$$(mktemp -d); \
	trap 'rm -rf "$$dir"' EXIT; \
	git clone https://github.com/open-telemetry/opentelemetry-proto.git $$dir; \
	protoc -I=$$dir --elixir_out=plugins=grpc:$(PWD)/lib/logflare_grpc $$(find $$dir -iname '*.proto')


# manual deployment scripts

deploy.staging.main:
	@gcloud config set project logflare-staging
	gcloud builds submit \
		projects/logflare-staging/locations/us-central1/connections/github-logflare/repositories/Logflare-logflare \
		--revision=main  \
		--config=cloudbuild/staging/build-image.yaml \
		--substitutions=_IMAGE_TAG=$(SHA_IMAGE_TAG) \
		--region=us-central1 \
		--gcs-log-dir="gs://logflare-staging_cloudbuild-logs/logs"
	
	gcloud builds submit \
		--no-source \
		--config=./cloudbuild/staging/deploy.yaml \
		--substitutions=_IMAGE_TAG=$(SHA_IMAGE_TAG) \
		--region=us-central1 \
		--gcs-log-dir="gs://logflare-staging_cloudbuild-logs/logs"

deploy.staging.versioned:
	@gcloud config set project logflare-staging
	gcloud builds submit \
		projects/logflare-staging/locations/us-central1/connections/github-logflare/repositories/Logflare-logflare \
		--revision=main  \
		--config=cloudbuild/staging/build-image.yaml \
		--substitutions=_IMAGE_TAG=$(VERSION) \
		--region=us-central1 \
		--gcs-log-dir="gs://logflare-staging_cloudbuild-logs/logs"
	
	gcloud builds submit \
		--no-source \
		--config=./cloudbuild/staging/deploy.yaml \
		--substitutions=_IMAGE_TAG=$(VERSION),_NORMALIZED_IMAGE_TAG=$(NORMALIZED_VERSION),_INSTANCE_TYPE=c2d-standard-2,_CLUSTER=versioned \
		--region=us-central1 \
		--gcs-log-dir="gs://logflare-staging_cloudbuild-logs/logs"


deploy.prod.versioned:
	@gcloud config set project logflare-232118
	gcloud builds submit \
		projects/logflare-232118/locations/europe-west3/connections/github-logflare/repositories/Logflare-logflare \
		--revision=main  \
		--config=./cloudbuild/prod/build-image.yaml \
		--substitutions=_IMAGE_TAG=$(VERSION) \
		--region=europe-west3 \
		--gcs-log-dir="gs://logflare-prod_cloudbuild-logs/logs"
	
	gcloud builds submit \
		--no-source \
		--config=./cloudbuild/prod/pre-deploy.yaml \
		--substitutions=_IMAGE_TAG=$(VERSION),_NORMALIZED_IMAGE_TAG=$(NORMALIZED_VERSION) \
		--region=europe-west3 \
		--gcs-log-dir="gs://logflare-prod_cloudbuild-logs/logs"
	@echo "Instance template has been created successfully. Complete the deployment by navigating to https://console.cloud.google.com/compute/instanceGroups/list?hl=en&project=logflare-232118"
.PHONY: deploy.staging.main