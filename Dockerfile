FROM elixir:1.15-alpine as builder

ENV MIX_ENV prod
# Due to some Rust caveats with SSL on Alpine images, we need to use GIT to fecth cargo registry index
ENV CARGO_NET_GIT_FETCH_WITH_CLI=true

# cache intermediate layers for deps compilation
COPY ./VERSION mix.exs mix.lock /logflare/
COPY assets/package.json assets/package-lock.json /logflare/assets/
WORKDIR /logflare
RUN apk update && \
    # all the base dependencies
    apk add -f curl git build-base nodejs npm rust cargo python3 && \
    # all the app dependencies
    mix do local.rebar --force, local.hex --force, deps.get, deps.compile && \
    npm --prefix assets ci

COPY . /logflare
RUN mix release  && \
    npm run --prefix assets deploy && \
    mix phx.digest

# alpine version must match the base erlang image version used
# https://github.com/erlef/docker-elixir/blob/master/1.15/alpine/Dockerfile
# https://github.com/erlang/docker-erlang-otp/blob/master/26/alpine/Dockerfile
FROM alpine:3.18.0 as app

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
