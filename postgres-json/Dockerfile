ARG eventuateCommonVersion

FROM eventuateio/eventuate-postgres:$eventuateCommonVersion

COPY 4.initialize-database.sql /docker-entrypoint-initdb.d
