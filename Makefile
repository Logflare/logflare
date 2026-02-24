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

# Detect which version manager is available (MISE preferred over ASDF)
MISE_AVAILABLE := $(shell command -v mise 2> /dev/null)
ASDF_AVAILABLE := $(shell command -v asdf 2> /dev/null)

ifdef MISE_AVAILABLE
	VERSION_MANAGER := mise
	VERSION_MANAGER_INSTALL := mise install
	VERSION_MANAGER_RESHIM := mise reshim
else ifdef ASDF_AVAILABLE
	VERSION_MANAGER := asdf
	VERSION_MANAGER_INSTALL := asdf install
	VERSION_MANAGER_RESHIM := asdf reshim
else
	VERSION_MANAGER :=
	VERSION_MANAGER_INSTALL :=
	VERSION_MANAGER_RESHIM :=
endif

# Tool validation
REQUIRED_TOOLS := docker git gcloud

define check_tool
	@if command -v $(1) >/dev/null 2>&1; then \
		echo "✓ $(1) found"; \
	else \
		echo "⚠ Warning: $(1) is not installed or not in PATH"; \
	fi
endef

# Color codes
GREEN := \033[0;32m
YELLOW := \033[1;33m
BLUE := \033[0;34m
RED := \033[0;31m
BOLD := \033[1m
NC := \033[0m

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

check-tools:
	@echo ""
	@echo -e "$(BOLD)$(BLUE)🔧 Checking required tools...$(NC)"
	@echo ""
	@for tool in $(REQUIRED_TOOLS); do \
		if command -v $$tool >/dev/null 2>&1; then \
			echo -e "  $(GREEN)✓$(NC) $$tool found"; \
		else \
			echo -e "  $(YELLOW)⚠$(NC)  Warning: $$tool is not installed or not in PATH"; \
		fi; \
	done
	@echo ""
	@echo -e "$(BOLD)$(BLUE)Tool check complete$(NC)"
	@echo ""

check-version-manager:
ifndef VERSION_MANAGER
	@echo ""
	@echo -e "$(RED)❌ Error: Neither MISE nor ASDF is installed.$(NC)"
	@echo -e "$(BOLD)Please install one of the following:$(NC)"
	@echo "  - MISE: https://mise.jdx.dev/getting-started.html"
	@echo "  - ASDF: https://asdf-vm.com/guide/getting-started.html"
	@echo ""
	@exit 1
else
	@echo -e "$(BOLD)$(BLUE)📦 Using $(VERSION_MANAGER) for version management$(NC)"
	@echo ""
endif

setup: check-tools check-version-manager setup.node
	@echo -e "$(BOLD)$(BLUE)🚀 Installing language dependencies...$(NC)"
	@echo ""
	$(VERSION_MANAGER_INSTALL)
	-epmd -daemon

	@echo ""
	@echo -e "$(BOLD)$(BLUE)🔧 Installing protobuf tooling...$(NC)"
	mix escript.install hex protobuf
	@echo ""

	$(VERSION_MANAGER_RESHIM)
	@echo -e "$(BOLD)$(BLUE)⚙️ Running Elixir setup...$(NC)"
	@echo ""
	mix setup
	@echo ""

	@echo -e "$(BOLD)$(GREEN)✅ Setup complete!$(NC)"
	@echo ""

setup.node:
	@echo -e "$(BOLD)$(BLUE)📦 Installing Node.js dependencies...$(NC)"
	@echo ""
	npm --prefix ./assets ci 
	@echo ""

reset:
	docker compose down
	MIX_ENV=dev mix ecto.reset
	MIX_ENV=test mix ecto.reset
	rm -rf _build .elixir_ls deps assets/node_modules

.PHONY: setup setup.node reset check-version-manager check-tools

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

start.green: ERL_NAME = green
start.green: PORT = 4002
start.green: ERL_COOKIE = greenmonster
start.green: ENV_FILE = .dev.env
start.green: LOGFLARE_GRPC_PORT = 50053
start.green: __start__

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
	@if [ ! -f ${ENV_FILE} ]; then \
		touch ${ENV_FILE}; \
	fi
	@env $$(cat ${ENV_FILE} .dev.env | xargs) PORT=${PORT} LOGFLARE_GRPC_PORT=${LOGFLARE_GRPC_PORT} LOGFLARE_SUPABASE_MODE=${LOGFLARE_SUPABASE_MODE} iex --sname ${ERL_NAME}-${ERL_COOKIE} --cookie ${ERL_COOKIE} -S mix phx.server


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
	git clone https://github.com/googleapis/googleapis.git --depth=1 $$dir; \
	protoc -I=$$dir --elixir_out=plugins=grpc:$(PWD)/lib/logflare_grpc $$(find $$dir -path "*/rpc/*" -iname '*.proto')

# Mock data for testing interceptors
	dir=./priv/test_protobuf; \
	protoc -I=$$dir --elixir_out=plugins=grpc:$(PWD)/test/support/test_protobuf/ $$(find $$dir -iname '*.proto')

# For Google BigQuery
# if you have encoding issues make sure to run the terminal as latin1
# eg: LC_CTYPE=en_US.iso88591 luit make grpc.protoc.bq
grpc.protoc.bq:
	dir=$$(mktemp -d); \
	trap 'rm -rf "$$dir"' EXIT; \
	git clone https://github.com/googleapis/googleapis.git --depth=1 $$dir; \
	protoc -I=$$dir --elixir_out=plugins=grpc:$(PWD)/lib/logflare_grpc $$(find $$dir -path "*/cloud/bigquery/storage/v1/*" -iname '*.proto')

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
		--substitutions=_IMAGE_TAG=$(VERSION),_NORMALIZED_IMAGE_TAG=$(NORMALIZED_VERSION),_CLUSTER=prod-a,_LOGFLARE_ALERTS_ENABLED=true \
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


docker.multi-step:
	docker-compose build base runner

.PHONY: $(addprefix ssl.,${envs}) docker.build.multistep
