FROM elixir:1.10

ENV MIX_ENV staging

RUN curl -sL https://deb.nodesource.com/setup_14.x | bash -

RUN curl -sS https://dl.yarnpkg.com/debian/pubkey.gpg | apt-key add -
RUN echo "deb https://dl.yarnpkg.com/debian/ stable main" | tee /etc/apt/sources.list.d/yarn.list

RUN apt-get update
RUN apt-get install -y nodejs yarn

RUN mix local.rebar --force
RUN mix local.hex --force

COPY assets/package.json /logflare/asssets/
COPY assets/yarn.lock /logflare/assets/
RUN cd /logflare/assets && yarn 
RUN ./node_modules/webpack/bin/webpack.js --mode production 

COPY mix.* /logflare/
RUN mix deps.get
RUN mix compile

ARG MAGIC_COOKIE_PREFIX
ARG SHORT_COMMIT_SHA

ENV MAGIC_COOKIE_PREFIX=$MAGIC_COOKIE_PREFIX
ENV SHORT_COMMIT_SHA=$SHORT_COMMIT_SHA

ENV MIX_ENV staging

COPY ./ /logflare
WORKDIR /logflare

RUN mix phx.digest
RUN mix release  --overwrite

ENTRYPOINT [ "/logflare/run_staging.bash" ]
