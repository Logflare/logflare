FROM gcr.io/logflare-staging/logflare_base

COPY ./ /logflare
WORKDIR /logflare

ENV MIX_ENV staging

RUN mix deps.get
RUN mix compile

RUN cd /logflare/assets \
    && yarn \
    && ./node_modules/webpack/bin/webpack.js --mode production --silent

WORKDIR /logflare

RUN mix phx.digest
RUN mix release --force --overwrite

WORKDIR /logflare

ENTRYPOINT [ "/logflare/run.bash" ]


