FROM mariadb:10.3.8
COPY replication.cnf /etc/mysql/conf.d
COPY 1.initialize-database.sql /docker-entrypoint-initdb.d
COPY 2.initialize-database.sql /docker-entrypoint-initdb.d
