ARG eventuateCommonVersion

FROM eventuateio/eventuate-mariadb:$eventuateCommonVersion
COPY 3.initialize-database.sql /docker-entrypoint-initdb.d
