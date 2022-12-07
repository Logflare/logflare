FROM elixir:1.12-alpine as builder

ENV MIX_ENV prod
ENV MAGIC_COOKIE=$magic_cookie

RUN apk update && \
    apk add -f curl git build-base nodejs yarn rust cargo

COPY . /logflare

WORKDIR /logflare/native/sqlparser_ex
# Compile Rust
RUN cargo build

WORKDIR /logflare
RUN mix do local.rebar --force, local.hex --force
RUN mix do deps.get, deps.compile

RUN mix phx.digest
RUN mix release

FROM alpine:3.16.0 as app
WORKDIR /root/
# erlexec requires SHELL to be set
ENV SHELL /bin/sh
ENV MAGIC_COOKIE=$magic_cookie

RUN apk update && apk add -f openssl libgcc libstdc++ ncurses-libs
COPY --from=builder ./logflare/_build/prod /root/app

WORKDIR /root/app/rel/logflare/bin
COPY run.sh ./run.sh
CMD ["sh", "run.sh"]