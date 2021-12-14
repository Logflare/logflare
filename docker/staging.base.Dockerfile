FROM elixir:1.12

RUN wget -c https://download.java.net/java/GA/jdk16.0.1/7147401fd7354114ac51ef3e1328291f/9/GPL/openjdk-16.0.1_linux-x64_bin.tar.gz && \
    mkdir -p /opt/java && \
    tar xzvfp openjdk-16.0.1_linux-x64_bin.tar.gz -C /opt/java && \
    rm -f openjdk-16.0.1_linux-x64_bin.tar.gz

ENV JAVA_HOME /opt/java/jdk-16.0.1/

RUN curl -sL https://deb.nodesource.com/setup_14.x | bash - && \
    curl -sS https://dl.yarnpkg.com/debian/pubkey.gpg | apt-key add - && \
    echo "deb https://dl.yarnpkg.com/debian/ stable main" | tee /etc/apt/sources.list.d/yarn.list && \
    apt-get update && \
    apt-get install tini && \
    apt-get install -y nodejs yarn && \
    mix local.rebar --force && \ 
    mix local.hex --force

ENV MIX_ENV staging

COPY config /logflare/config/
COPY mix.* /logflare/

RUN cd /logflare && \
    mix deps.get && \
    mix compile

COPY assets/package.json assets/yarn.lock /logflare/assets/

RUN cd /logflare/assets && yarn && yarn upgrade phoenix phoenix_html phoenix_live_view phoenix_live_react
