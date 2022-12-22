ARG TAG_VERSION=latest
FROM supabase/logflare:${TAG_VERSION}

RUN apk add tini
COPY .secrets.env .secrets.env
RUN export $(cat .secrets.env | xargs)

ENTRYPOINT ["tini", "--"]

CMD ["sh", "run.sh"]
