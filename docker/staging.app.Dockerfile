FROM gcr.io/logflare-staging/logflare_base

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