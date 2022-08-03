#! /bin/bash -e

if [ -z "$MYSQL_PORT" ]; then
    export MYSQL_PORT=3307
fi

docker run $* \
   --name mysqlterm --network=${PWD##*/}_default  --rm \
   mysql/mysql-server:8.0.27-1.2.6-server \
   sh -c 'exec mysql -hmysql -P3306 -uroot -prootpassword -o eventuate'
