FROM elixir:1.12.3-alpine as builder

ENV MIX_ENV prod

RUN apk update && \
    apk add -f curl git build-base nodejs npm rust cargo python3

COPY . /logflare


WORKDIR /logflare
ENV CARGO_NET_GIT_FETCH_WITH_CLI=true
RUN mix do local.rebar --force, local.hex --force, deps.get, release

WORKDIR /logflare/assets
RUN npm install
RUN npm run deploy

WORKDIR /logflare
RUN mix phx.digest

# alpine version must match the base erlang image version used
# https://github.com/erlef/docker-elixir/blob/master/1.12/alpine/Dockerfile
# https://github.com/erlang/docker-erlang-otp/blob/master/24/alpine/Dockerfile
FROM alpine:3.17.0 as app

# Required for the BeamVM to run
RUN apk update && apk add -f openssl libgcc libstdc++ ncurses-libs curl

# Copy required files from builder step
COPY --from=builder logflare/_build/prod /opt/app
COPY --from=builder logflare/VERSION /opt/app/VERSION
COPY --from=builder logflare/priv/static /opt/app/rel/logflare/bin/priv/static

# Move files to the correct folder taking into consideration the VERSION
RUN cp -r /opt/app/rel/logflare/bin/priv/static /opt/app/rel/logflare/lib/logflare-$(cat /opt/app/VERSION)/priv/static

# Cleanup static assets not in use
RUN rm -r /opt/app/rel/logflare/bin/priv

WORKDIR /opt/app/rel/logflare/bin
COPY run.sh ./run.sh
CMD ["sh", "run.sh"]
