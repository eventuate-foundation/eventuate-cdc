FROM mysql:5.7.13
COPY replication.cnf /etc/mysql/conf.d
COPY 1.initialize-database.sql /docker-entrypoint-initdb.d
COPY 2.initialize-database.sql /docker-entrypoint-initdb.d
