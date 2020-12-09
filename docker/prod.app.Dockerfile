FROM gcr.io/logflare-232118/logflare_base:latest

ARG MAGIC_COOKIE
ENV MAGIC_COOKIE=$MAGIC_COOKIE


COPY ./ /logflare
WORKDIR /logflare

RUN wget https://dl.google.com/cloudsql/cloud_sql_proxy.linux.amd64 -O cloud_sql_proxy
RUN chmod +x cloud_sql_proxy

ENV MIX_ENV prod

RUN mix deps.get
RUN mix compile --force

RUN cd /logflare/assets \ 
    && yarn \
    && yarn upgrade phoenix phoenix_html phoenix_live_view phoenix_live_react \
    && ./node_modules/webpack/bin/webpack.js --mode production --silent

WORKDIR /logflare

RUN mix phx.digest
RUN mix release --force --overwrite

WORKDIR /logflare

CMD [ "/logflare/run_prod.bash" ]
