FROM gcr.io/logflare-staging/logflare_base

COPY ./ /logflare
WORKDIR /logflare

ENV MIX_ENV staging

RUN mix deps.get
RUN mix compile --force

WORKDIR /logflare/assets
RUN yarn 
RUN yarn upgrade phoenix phoenix_html phoenix_live_view phoenix_live_react
RUN ./node_modules/webpack/bin/webpack.js --mode production --silent

WORKDIR /logflare

RUN mix phx.digest
RUN mix release --force --overwrite

WORKDIR /logflare

ENTRYPOINT [ "/logflare/run_staging.bash" ]
