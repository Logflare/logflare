ARG TAG_VERSION=latest
FROM supabase/logflare:${TAG_VERSION}

RUN apk add tini
COPY .secrets.env /tmp/.secrets.env

ENTRYPOINT ["tini", "--"]

CMD ["sh", "run.sh"]
