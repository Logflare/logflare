FROM rust:1.42 as rust
FROM elixir:1.10

ENV RUSTUP_HOME=/usr/local/rustup \
    CARGO_HOME=/usr/local/cargo \
    PATH=/usr/local/cargo/bin:$PATH
COPY --from=rust /usr/local/cargo /usr/local/cargo
COPY --from=rust /usr/local/rustup /usr/local/rustup

ENV MIX_ENV staging

COPY ./ /logflare
WORKDIR /logflare

RUN curl -sL https://deb.nodesource.com/setup_13.x | bash -

RUN curl -sS https://dl.yarnpkg.com/debian/pubkey.gpg | apt-key add -
RUN echo "deb https://dl.yarnpkg.com/debian/ stable main" | tee /etc/apt/sources.list.d/yarn.list

RUN apt-get update

RUN apt-get install -y nodejs yarn

WORKDIR /logflare

RUN mix local.rebar --force
RUN mix local.hex --force

RUN mix deps.get
RUN mix compile

RUN cd /logflare/assets && yarn 

WORKDIR /logflare