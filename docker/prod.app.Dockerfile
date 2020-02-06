FROM elixir:1.9.4

COPY ./ /logflare
WORKDIR /logflare

ENV MIX_ENV prod

RUN mix deps.get
RUN mix compile --force

RUN cd /logflare/assets \
    && yarn \
    && ./node_modules/webpack/bin/webpack.js --mode production --silent

WORKDIR /logflare

RUN mix phx.digest
RUN mix release --force --overwrite

WORKDIR /logflare

ENTRYPOINT [ "/logflare/run_prod.bash" ]
