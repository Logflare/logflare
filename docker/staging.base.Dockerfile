FROM elixir:1.10

ENV MIX_ENV staging

RUN curl -sL https://deb.nodesource.com/setup_14.x | bash -

RUN curl -sS https://dl.yarnpkg.com/debian/pubkey.gpg | apt-key add - && \
    echo "deb https://dl.yarnpkg.com/debian/ stable main" | tee /etc/apt/sources.list.d/yarn.list

RUN apt-get update && \
    apt-get install -y nodejs yarn

RUN mix local.rebar --force && \ 
    mix local.hex --force

ENV MIX_ENV staging

COPY config /logflare/config/
COPY mix.* /logflare/

WORKDIR /logflare
RUN mix deps.get && \
    mix compile

COPY assets/package.json /logflare/assets/
COPY assets/yarn.lock /logflare/assets/

WORKDIR /logflare/assets
RUN yarn 
