FROM rust:1.42 as rust
FROM gcr.io/logflare-staging/logflare_base

ENV RUSTUP_HOME=/usr/local/rustup \
    CARGO_HOME=/usr/local/cargo \
    PATH=/usr/local/cargo/bin:$PATH
COPY --from=rust /usr/local/cargo /usr/local/cargo
COPY --from=rust /usr/local/rustup /usr/local/rustup

COPY ./ /logflare
WORKDIR /logflare

ENV MIX_ENV staging

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
