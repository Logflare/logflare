FROM elixir:1.10

ENV MIX_ENV staging

RUN curl -sL https://deb.nodesource.com/setup_14.x | bash -

RUN curl -sS https://dl.yarnpkg.com/debian/pubkey.gpg | apt-key add -
RUN echo "deb https://dl.yarnpkg.com/debian/ stable main" | tee /etc/apt/sources.list.d/yarn.list

RUN apt-get update
RUN apt-get install -y nodejs yarn

RUN mix local.rebar --force
RUN mix local.hex --force

ENV MIX_ENV staging

COPY config /logflare/config/
COPY mix.* /logflare/

WORKDIR /logflare
RUN mix deps.get && mix compile

COPY assets/package.json /logflare/assets/
COPY assets/yarn.lock /logflare/assets/

WORKDIR /logflare/assets
RUN yarn 

COPY assets /logflare/assets/

WORKDIR /logflare/assets
RUN ./node_modules/webpack/bin/webpack.js --mode production 

COPY . /logflare
WORKDIR /logflare

RUN mix phx.digest
RUN mix release --overwrite

ARG MAGIC_COOKIE_PREFIX
ARG SHORT_COMMIT_SHA

ENV MAGIC_COOKIE_PREFIX=$MAGIC_COOKIE_PREFIX
ENV SHORT_COMMIT_SHA=$SHORT_COMMIT_SHA

ENTRYPOINT [ "/logflare/run_staging.bash" ]
