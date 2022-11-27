FROM elixir:1.12-alpine as builder

ENV MIX_ENV prod
ENV JAVA_HOME /opt/java/jdk-16.0.1/
ENV MAGIC_COOKIE=$magic_cookie
ENV HEX_HTTP_CONCURRENCY=1d
ENV HEX_HTTP_TIMEOUT=120

RUN apk update && \
    apk add -f curl git build-base nodejs yarn

RUN curl https://download.java.net/java/GA/jdk16.0.1/7147401fd7354114ac51ef3e1328291f/9/GPL/openjdk-16.0.1_linux-x64_bin.tar.gz -o openjdk-16.0.1_linux-x64_bin.tar.gz && \
    mkdir -p /opt/java && \
    tar xzvfp openjdk-16.0.1_linux-x64_bin.tar.gz -C /opt/java && \
    rm -f openjdk-16.0.1_linux-x64_bin.tar.gz

COPY . /logflare

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