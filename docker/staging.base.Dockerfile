FROM elixir:1.10

RUN curl -sL https://deb.nodesource.com/setup_14.x | bash - && \
    curl -sS https://dl.yarnpkg.com/debian/pubkey.gpg | apt-key add - && \
    echo "deb https://dl.yarnpkg.com/debian/ stable main" | tee /etc/apt/sources.list.d/yarn.list && \
    apt-get update && \
    apt-get install -y nodejs yarn && \
    mix local.rebar --force && \ 
    mix local.hex --force

ENV MIX_ENV staging

COPY config /logflare/config/
COPY mix.* /logflare/

RUN cd /logflare && \
    mix deps.get && \
    mix compile

COPY assets/package.json assets/yarn.lock /logflare/assets/

RUN cd /logflare/assets && yarn && yarn upgrade phoenix phoenix_html phoenix_live_view phoenix_live_react
