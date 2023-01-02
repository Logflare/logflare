ARG TAG_VERSION=latest
FROM supabase/logflare:${TAG_VERSION}

RUN apk add tini

COPY .secrets.env /tmp/.secrets.env
COPY gcloud.json /opt/app/gcloud.json

ENTRYPOINT ["tini", "--"]

CMD ["sh", "run.sh"]
