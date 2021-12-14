FROM gcr.io/logflare-staging/logflare_base

ARG MAGIC_COOKIE_PREFIX
ARG SHORT_COMMIT_SHA

ENV MAGIC_COOKIE_PREFIX=$MAGIC_COOKIE_PREFIX
ENV SHORT_COMMIT_SHA=$SHORT_COMMIT_SHA

COPY . /logflare

WORKDIR /logflare/assets
RUN ./node_modules/webpack/bin/webpack.js --mode production 

WORKDIR /logflare
RUN mix phx.digest
RUN mix release --overwrite

# erlexec requires SHELL to be set
ENV SHELL /bin/bash

ENTRYPOINT ["tini", "--"]

CMD [ "/logflare/run_staging.bash" ]
