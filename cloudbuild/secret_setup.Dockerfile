ARG TAG_VERSION
FROM supabase/logflare:${TAG_VERSION}

RUN apk add tini

COPY .secrets.env /tmp/.secrets.env
COPY gcloud.json gcloud.json
COPY cacert.pem cacert.pem
COPY cert.pem cert.pem
COPY cert.key cert.key

ENTRYPOINT ["tini", "--"]

CMD ["sh", "run.sh"]
