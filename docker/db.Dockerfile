from postgres:13.4

RUN echo "\connect postgres" >> /docker-entrypoint-initdb.d/init.sql
RUN echo "ALTER SYSTEM SET wal_level = 'logical';">> /docker-entrypoint-initdb.d/init.sql
# RUN echo "CREATE PUBLICATION logflare_pub FOR ALL TABLES;">> /docker-entrypoint-initdb.d/init.sql

EXPOSE 5432
ENTRYPOINT ["docker-entrypoint.sh"]
CMD ["postgres"]
