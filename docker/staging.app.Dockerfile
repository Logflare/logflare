FROM gcr.io/logflare-staging/logflare_base

ARG MAGIC_COOKIE_PREFIX
ARG SHORT_COMMIT_SHA

ENV MAGIC_COOKIE_PREFIX=$MAGIC_COOKIE_PREFIX
ENV SHORT_COMMIT_SHA=$SHORT_COMMIT_SHA

ENV MIX_ENV staging

COPY ./ /logflare
WORKDIR /logflare

RUN mix deps.get
RUN mix compile --force

WORKDIR /logflare/assets
RUN yarn 
RUN yarn upgrade phoenix phoenix_html phoenix_live_view phoenix_live_react
RUN ./node_modules/webpack/bin/webpack.js --mode production 

WORKDIR /logflare

RUN mix phx.digest
RUN mix release --force --overwrite

WORKDIR /logflare

ENTRYPOINT [ "/logflare/run_staging.bash" ]
