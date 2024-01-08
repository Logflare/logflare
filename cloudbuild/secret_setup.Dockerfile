ARG TAG_VERSION
FROM supabase/logflare:${TAG_VERSION}

RUN apt-get update && apt-get -y install tini
RUN echo $RANDOM | md5sum | head -c 20 > /tmp/.magic_cookie
COPY .secrets.env /tmp/.secrets.env
COPY gcloud.json gcloud.json
COPY cacert.pem cacert.pem
COPY cert.pem cert.pem
COPY cert.key cert.key
COPY db-client-cert.pem db-client-cert.pem
COPY db-client-key.pem db-client-key.pem
COPY db-server-ca.pem db-server-ca.pem

ENTRYPOINT ["tini", "--"]

CMD ["sh", "run.sh"]
