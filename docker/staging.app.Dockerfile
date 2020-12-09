FROM gcr.io/logflare-staging/logflare_base


ARG MAGIC_COOKIE_PREFIX
ARG SHORT_COMMIT_SHA

ENV MAGIC_COOKIE_PREFIX=$MAGIC_COOKIE_PREFIX
ENV SHORT_COMMIT_SHA=$SHORT_COMMIT_SHA

COPY . /logflare

WORKDIR /logflare
RUN wget https://dl.google.com/cloudsql/cloud_sql_proxy.linux.amd64 -O cloud_sql_proxy
RUN chmod +x cloud_sql_proxy

WORKDIR /logflare/assets
RUN ./node_modules/webpack/bin/webpack.js --mode production 

WORKDIR /logflare
RUN mix phx.digest
RUN mix release --overwrite

ENTRYPOINT [ "/logflare/run_staging.bash" ]
