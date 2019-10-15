FROM gcr.io/logflare-232118/logflare_app:latest

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