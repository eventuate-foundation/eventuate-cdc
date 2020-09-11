ARG eventuateCommonVersion

FROM eventuateio/eventuate-mysql:$eventuateCommonVersion
COPY 3.initialize-database.sql /docker-entrypoint-initdb.d
