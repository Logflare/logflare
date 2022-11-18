FROM elixir:1.12-alpine as builder

# erlexec requires SHELL to be set
ENV SHELL /bin/bash
ENV MIX_ENV prod
ENV JAVA_HOME /opt/java/jdk-16.0.1/
ENV MAGIC_COOKIE $magic_cookie

RUN apk update && \
    apk add -f curl git build-base nodejs yarn

RUN curl https://download.java.net/java/GA/jdk16.0.1/7147401fd7354114ac51ef3e1328291f/9/GPL/openjdk-16.0.1_linux-x64_bin.tar.gz -o openjdk-16.0.1_linux-x64_bin.tar.gz && \
    mkdir -p /opt/java && \
    tar xzvfp openjdk-16.0.1_linux-x64_bin.tar.gz -C /opt/java && \
    rm -f openjdk-16.0.1_linux-x64_bin.tar.gz

COPY . /logflare

WORKDIR /logflare
RUN mix local.rebar --force
RUN mix local.hex --force
RUN mix do deps.get, deps.compile

RUN mix phx.digest
RUN mix release --force --overwrite

FROM elixir:1.12-alpine as app
WORKDIR /root/

ENV MIX_ENV prod
ENV MAGIC_COOKIE $magic_cookie

COPY --from=builder ./logflare/_build/prod ./app
COPY --from=builder ./logflare/priv/static ./app/priv/static

ADD .google.secret.json app/.google.secret.json

CMD ["./app/rel/logflare/bin/logflare", "start", "--name" , "logflare"]
