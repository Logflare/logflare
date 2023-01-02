FROM elixir:1.12-alpine as builder

ENV MIX_ENV prod

RUN apk update && \
    apk add -f curl git build-base nodejs yarn rust cargo

COPY . /logflare

WORKDIR /logflare

RUN mix do local.rebar --force, local.hex --force, deps.get, phx.digest, release

FROM alpine:3.16.0 as app

# Required for the BeamVM to run
RUN apk update && apk add -f openssl libgcc libstdc++ ncurses-libs

COPY --from=builder logflare/_build/prod /opt/app
COPY --from=builder logflare/VERSION /opt/app/rel/logflare/bin/VERSION
COPY --from=builder logflare/priv /opt/app/rel/logflare/bin/priv

WORKDIR /opt/app/rel/logflare/bin
COPY run.sh ./run.sh
CMD ["sh", "run.sh"]