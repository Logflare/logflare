ARG ELIXIR_VERSION=1.19.5
ARG OTP_VERSION=27.3.4.6
ARG DEBIAN_VERSION=trixie-20260112-slim

ARG BUILDER_IMAGE="hexpm/elixir:${ELIXIR_VERSION}-erlang-${OTP_VERSION}-debian-${DEBIAN_VERSION}"
ARG RUNNER_IMAGE="debian:${DEBIAN_VERSION}"

FROM ${BUILDER_IMAGE} AS builder

ENV MIX_ENV prod

WORKDIR /app

# install build dependencies
RUN apt-get update -y && apt-get install -y curl bash build-essential git gcc make && \
    # install nodejs
    curl -fsSL https://deb.nodesource.com/setup_22.x | bash - && \
    apt-get install -y nodejs && \
    # install rust
    curl https://sh.rustup.rs -sSf | bash -s -- -y && \
    # cleanup
    apt-get clean && rm -f /var/lib/apt/lists/*_*
# app dependencies
COPY ./VERSION mix.exs mix.lock ./
RUN mix do local.rebar --force + local.hex --force + deps.get + deps.compile

COPY assets/package.json assets/package-lock.json assets/
RUN npm --prefix assets ci

COPY config/config.exs config/
COPY config/prod.exs config/
COPY config/runtime.exs config/
COPY . ./

# rust bin path
ENV PATH="/root/.cargo/bin:${PATH}"

# check installed correctly
RUN cargo version

# release
RUN mix release  && \
    npm run --prefix assets deploy && \
    mix phx.digest

FROM ${RUNNER_IMAGE}

# Required for the BeamVM to run
RUN apt-get update -y && apt-get install -y curl libstdc++6 openssl locales \
    && apt-get clean && rm -f /var/lib/apt/lists/*_*

# Set the locale
RUN sed -i '/en_US.UTF-8/s/^# //g' /etc/locale.gen && locale-gen

ENV LANG en_US.UTF-8
ENV LANGUAGE en_US:en
ENV LC_ALL en_US.UTF-8

# Copy required files from builder step
COPY --from=builder app/_build/prod /opt/app
COPY --from=builder app/VERSION /opt/app/VERSION
COPY --from=builder app/priv/static /opt/app/rel/logflare/bin/priv/static

# Move files to the correct folder taking into consideration the VERSION
RUN cp -r /opt/app/rel/logflare/bin/priv/static /opt/app/rel/logflare/lib/logflare-$(cat /opt/app/VERSION)/priv/static

# Cleanup static assets not in use
RUN rm -r /opt/app/rel/logflare/bin/priv

WORKDIR /opt/app/rel/logflare/bin
COPY run.sh ./run.sh
CMD ["sh", "run.sh"]
