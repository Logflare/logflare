GCLOUD_LOCATION ?= us-central1
GCLOUD_KEYRING ?= logflare-keyring-us-central1
GCLOUD_KEY ?= logflare-secrets-key
GCLOUD_PROJECT ?= logflare-staging
ERL_COOKIE ?= monster

ENV ?= dev
SHA_IMAGE_TAG ?= dev-$(shell git rev-parse --short=7 HEAD)
VERSION ?= $(shell cat ./VERSION)
NORMALIZED_VERSION ?= $(shell cat ./VERSION | tr '.' '-')

LOGFLARE_SUPABASE_MODE ?= false


help:
	@cat DEVELOPMENT.md

test:
	-epmd -daemon
	mix test

test.failed:
	-epmd -daemon
	mix test --failed

test.only:
	-epmd -daemon
	mix test.only

compile.check:
	ERL_COMPILER_OPTIONS=bin_opt_info mix test.compile --force

.PHONY: test test.only compile.check

setup: setup.node
	-epmd -daemon
	# install dependencies
	asdf install

	# add protobuf install
	mix escript.install hex protobuf

	asdf reshim
	# run elixir setup
	mix setup

setup.node:
	npm --prefix ./assets install

reset:
	docker compose down
	MIX_ENV=dev mix ecto.reset
	MIX_ENV=test mix ecto.reset
	rm -rf _build .elixir_ls deps assets/node_modules

.PHONY: setup setup.node reset

start: start.orange

start.orange: ERL_NAME = orange
start.orange: PORT = 4000
start.orange: ENV_FILE = .dev.env
start.orange: LOGFLARE_GRPC_PORT = 50051
start.orange: __start__

start.pink: ERL_NAME = pink
start.pink: PORT = 4001
start.pink: ENV_FILE = .dev.env
start.pink: LOGFLARE_GRPC_PORT = 50052
start.pink: __start__

# temp alias

start.sb.bq: LOGFLARE_SUPABASE_MODE = true
start.sb.bq: start.st.bq

start.st.bq: ERL_NAME = st_
start.st.bq: PORT ?= 4000
start.st.bq: ENV_FILE = .single_tenant_bq.env
start.st.bq: LOGFLARE_GRPC_PORT = 50051
start.st.bq: __start__

start.sb.pg: LOGFLARE_SUPABASE_MODE = true
start.sb.pg: start.st.pg

start.st.pg: ERL_NAME = st_pg
start.st.pg: PORT ?= 4000
start.st.pg: ENV_FILE = .single_tenant_pg.env
start.st.pg: LOGFLARE_GRPC_PORT = 50051
start.st.pg: __start__

observer: 
	erl -sname observer -hidden -setcookie ${ERL_COOKIE} -run observer

__start__:
	@env $$(cat ${ENV_FILE} | xargs) PORT=${PORT} LOGFLARE_GRPC_PORT=${LOGFLARE_GRPC_PORT} LOGFLARE_SUPABASE_MODE=${LOGFLARE_SUPABASE_MODE} iex --sname ${ERL_NAME} --cookie ${ERL_COOKIE} -S mix phx.server


migrate:
	@env $$(cat .dev.env | xargs) mix ecto.migrate


.PHONY: __start__ migrate start.sb.pg start.sb.bq start.st.pg start.st.bq start.orange start.pink

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
 	.$$*.cert.key \
 	.$$*.cert.pem \
 	.$$*.req.pem \
 	.$$*.db-client-cert.pem \
 	.$$*.db-client-key.pem \
 	.$$*.db-server-ca.pem

$(addprefix encrypt.,${envs}): encrypt.%: \
	.$$*.gcloud.json.enc \
	.$$*.env.enc \
	.$$*.cert.key.enc \
	.$$*.cert.pem.enc \
	.$$*.req.pem.enc \
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

# For google rpc protos (status, etc)
	dir=$$(mktemp -d); \
	trap 'rm -rf "$$dir"' EXIT; \
	git clone https://github.com/googleapis/googleapis.git $$dir; \
	protoc -I=$$dir --elixir_out=plugins=grpc:$(PWD)/lib/logflare_grpc $$(find $$dir -path "*/rpc/*" -iname '*.proto')

# Mock data for testing interceptors
	dir=./priv/test_protobuf; \
	protoc -I=$$dir --elixir_out=plugins=grpc:$(PWD)/test/support/test_protobuf/ $$(find $$dir -iname '*.proto')


# manual deployment scripts

deploy.staging.main:
	@gcloud config set project logflare-staging
	gcloud builds submit . \
		--config=cloudbuild/staging/build-image.yaml \
		--substitutions=_IMAGE_TAG=$(SHA_IMAGE_TAG) \
		--region=europe-west1 \
		--gcs-log-dir="gs://logflare-staging_cloudbuild-logs/logs"

	gcloud builds submit . \
		--config=./cloudbuild/staging/deploy.yaml \
		--substitutions=_IMAGE_TAG=$(SHA_IMAGE_TAG),_INSTANCE_TYPE=c2d-standard-16 \
		--region=us-central1 \
		--gcs-log-dir="gs://logflare-staging_cloudbuild-logs/logs"

	gcloud builds submit . \
		--config=./cloudbuild/staging/deploy.yaml \
		--substitutions=_IMAGE_TAG=$(SHA_IMAGE_TAG),_INSTANCE_GROUP=instance-group-staging-main-saturated,_INSTANCE_TYPE=c2d-highcpu-16 \
		--region=us-central1 \
		--gcs-log-dir="gs://logflare-staging_cloudbuild-logs/logs"

deploy.staging.versioned:
	@gcloud config set project logflare-staging
	gcloud builds submit . \
		--config=cloudbuild/staging/build-image.yaml \
		--substitutions=_IMAGE_TAG=$(VERSION) \
		--region=europe-west1 \
		--gcs-log-dir="gs://logflare-staging_cloudbuild-logs/logs"

	gcloud builds submit . \
		--config=./cloudbuild/staging/deploy.yaml \
		--substitutions=_IMAGE_TAG=$(VERSION),_NORMALIZED_IMAGE_TAG=$(NORMALIZED_VERSION),_CLUSTER=versioned \
		--region=us-west1 \
		--gcs-log-dir="gs://logflare-staging_cloudbuild-logs/logs"


deploy.prod.versioned:
	@gcloud config set project logflare-232118
	@echo "Creating staging instance template and deploying..."
	gcloud builds submit . \
		--config=./cloudbuild/prod/build-image.yaml \
		--substitutions=_IMAGE_TAG=$(VERSION) \
		--region=europe-west3 \
		--gcs-log-dir="gs://logflare-prod_cloudbuild-logs/logs"

	@echo "Creating canary instance template..."
	gcloud builds submit . \
		--config=./cloudbuild/prod/pre-deploy.yaml \
		--substitutions=_IMAGE_TAG=$(VERSION),_NORMALIZED_IMAGE_TAG=$(NORMALIZED_VERSION) \
		--region=europe-west3 \
		--gcs-log-dir="gs://logflare-prod_cloudbuild-logs/logs"

	@echo "Deploying to canary instances"
	gcloud builds submit . \
		--config=./cloudbuild/prod/deploy.yaml \
		--substitutions=_IMAGE_TAG=$(VERSION),_NORMALIZED_IMAGE_TAG=$(NORMALIZED_VERSION) \
		--region=europe-west3 \
		--gcs-log-dir="gs://logflare-prod_cloudbuild-logs/logs"


	@echo "Creating prod instance templates..."
	gcloud builds submit . \
		--config=./cloudbuild/prod/pre-deploy.yaml \
		--substitutions=_IMAGE_TAG=$(VERSION),_NORMALIZED_IMAGE_TAG=$(NORMALIZED_VERSION),_CLUSTER=prod-a \
		--region=europe-west3 \
		--gcs-log-dir="gs://logflare-prod_cloudbuild-logs/logs"
	gcloud builds submit . \
		--config=./cloudbuild/prod/pre-deploy.yaml \
		--substitutions=_IMAGE_TAG=$(VERSION),_NORMALIZED_IMAGE_TAG=$(NORMALIZED_VERSION),_CLUSTER=prod-b \
		--region=europe-west3 \
		--gcs-log-dir="gs://logflare-prod_cloudbuild-logs/logs"
	gcloud builds submit . \
		--config=./cloudbuild/prod/pre-deploy.yaml \
		--substitutions=_IMAGE_TAG=$(VERSION),_NORMALIZED_IMAGE_TAG=$(NORMALIZED_VERSION),_CLUSTER=prod-c \
		--region=europe-west3 \
		--gcs-log-dir="gs://logflare-prod_cloudbuild-logs/logs"
	gcloud builds submit . \
		--config=./cloudbuild/prod/pre-deploy.yaml \
		--substitutions=_IMAGE_TAG=$(VERSION),_NORMALIZED_IMAGE_TAG=$(NORMALIZED_VERSION),_CLUSTER=prod-d \
		--region=europe-west3 \
		--gcs-log-dir="gs://logflare-prod_cloudbuild-logs/logs"
	gcloud builds submit . \
		--config=./cloudbuild/prod/pre-deploy.yaml \
		--substitutions=_IMAGE_TAG=$(VERSION),_NORMALIZED_IMAGE_TAG=$(NORMALIZED_VERSION),_CLUSTER=prod-e \
		--region=europe-west3 \
		--gcs-log-dir="gs://logflare-prod_cloudbuild-logs/logs"
	gcloud builds submit . \
		--config=./cloudbuild/prod/pre-deploy.yaml \
		--substitutions=_IMAGE_TAG=$(VERSION),_NORMALIZED_IMAGE_TAG=$(NORMALIZED_VERSION),_CLUSTER=prod-f \
		--region=europe-west3 \
		--gcs-log-dir="gs://logflare-prod_cloudbuild-logs/logs"

	gcloud builds submit . \
		--config=./cloudbuild/prod/pre-deploy.yaml \
		--substitutions=_IMAGE_TAG=$(VERSION),_NORMALIZED_IMAGE_TAG=$(NORMALIZED_VERSION),_CLUSTER=prod-g \
		--region=europe-west3 \
		--gcs-log-dir="gs://logflare-prod_cloudbuild-logs/logs"


	@echo "Instance template has been created successfully. Complete the deployment by navigating to https://console.cloud.google.com/compute/instanceGroups/list?hl=en&project=logflare-232118"
.PHONY: deploy.staging.main

tag-versioned:

	@echo "Checking dockerhub registry for dev image supabase/logflare:$(SHA_IMAGE_TAG) ..."
	@echo "Dev image must be built on CI: https://github.com/Logflare/logflare/actions" \
		docker manifest inspect supabase/logflare:$(SHA_IMAGE_TAG) >/dev/null
	@echo "OK"

	@echo "Retagging dev image to supabase/logflare:$(VERSION) ..."
	docker buildx imagetools create -t supabase/logflare:$(VERSION) -t supabase/logflare:latest supabase/logflare:$(SHA_IMAGE_TAG)
	@echo "OK"

.PHONY: tag-versioned


ssl.prod: CERT_DOMAIN = logflare.app
ssl.staging: CERT_DOMAIN = logflarestaging.com

$(addprefix ssl.,${envs}): ssl.%:
	@echo "Generating self-signed certificate..."
	@openssl req -x509 -newkey rsa:2048 -keyout .$*.cert.key -out .$*.cert.pem -days 3650 \
		-nodes -subj "/C=US/ST=DE/O=Supabase/OU=Logflare/CN=$(CERT_DOMAIN)"


.PHONY: $(addprefix ssl.,${envs})
