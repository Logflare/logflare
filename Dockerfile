FROM elixir:latest

RUN curl -sL https://deb.nodesource.com/setup_13.x | bash -

RUN curl -sS https://dl.yarnpkg.com/debian/pubkey.gpg | apt-key add -
RUN echo "deb https://dl.yarnpkg.com/debian/ stable main" | tee /etc/apt/sources.list.d/yarn.list
RUN apt-get update

RUN apt-get install -y nodejs yarn

RUN mix local.rebar --force
RUN mix local.hex --force

COPY ./ /logflare
WORKDIR /logflare

ENV MIX_ENV staging
ENV PORT 80

RUN mix deps.get
RUN mix compile

RUN cd /logflare/assets \
    && yarn \
    && ./node_modules/webpack/bin/webpack.js --mode production --silent

WORKDIR /logflare

RUN mix phx.digest
RUN mix release --force --overwrite

ENTRYPOINT ["/logflare/run.bash"]
