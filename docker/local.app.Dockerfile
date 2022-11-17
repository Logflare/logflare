FROM elixir:1.12-slim

# erlexec requires SHELL to be set
ENV SHELL /bin/bash
ENV MIX_ENV local
ENV JAVA_HOME /opt/java/jdk-16.0.1/

RUN apt-get update && \
    apt-get install -y curl git build-essential

RUN curl -sL https://deb.nodesource.com/setup_14.x | bash - && \
    curl -sS https://dl.yarnpkg.com/debian/pubkey.gpg | apt-key add - && \
    echo "deb https://dl.yarnpkg.com/debian/ stable main" | tee /etc/apt/sources.list.d/yarn.list

RUN apt-get update && \
    apt-get install -y nodejs yarn

RUN curl https://download.java.net/java/GA/jdk16.0.1/7147401fd7354114ac51ef3e1328291f/9/GPL/openjdk-16.0.1_linux-x64_bin.tar.gz -o openjdk-16.0.1_linux-x64_bin.tar.gz && \
    mkdir -p /opt/java && \
    tar xzvfp openjdk-16.0.1_linux-x64_bin.tar.gz -C /opt/java && \
    rm -f openjdk-16.0.1_linux-x64_bin.tar.gz

COPY . /logflare

WORKDIR /logflare
RUN mix local.rebar --force
RUN mix local.hex --force
RUN mix deps.get && \
    mix compile && \
    mix phx.digest

RUN cd /logflare/assets && \
    yarn && \
    yarn upgrade phoenix phoenix_html phoenix_live_view phoenix_live_react

CMD [ "elixir", "--sname", "local", "--cookie", "cookie" ,"-S" ,"mix" , "phx.server" ]
