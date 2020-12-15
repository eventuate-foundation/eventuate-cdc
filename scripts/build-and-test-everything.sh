#! /bin/bash -e

set -o pipefail

SCRIPTS="
./scripts/build-and-test-mysql.sh
./scripts/build-and-test-mysql8.sh
./scripts/build-and-test-mysql-migration.sh
./scripts/build-and-test-mariadb.sh
./scripts/build-and-test-mssql-polling.sh
./scripts/build-and-test-postgres-polling.sh
./scripts/build-and-test-postgres-wal.sh
./scripts/build-and-test-all-database-id-jsonschema.sh
./scripts/build-and-test-all-eventuate-local-cdc-mysql-binlog.sh
./scripts/build-and-test-all-eventuate-local-cdc-mysql8-binlog.sh
./scripts/build-and-test-all-eventuate-local-cdc-mariadb-binlog.sh
./scripts/build-and-test-all-eventuate-local-cdc-postgres-polling.sh
./scripts/build-and-test-all-eventuate-local-cdc-postgres-wal.sh
./scripts/build-and-test-all-tram-cdc-mysql-binlog.sh
./scripts/build-and-test-all-tram-cdc-mysql8-binlog.sh
./scripts/build-and-test-all-tram-cdc-mssql-polling.sh
./scripts/build-and-test-all-tram-cdc-postgres-polling.sh
./scripts/build-and-test-all-tram-cdc-postgres-wal.sh
./scripts/build-and-test-all-cdc-unified.sh
"

date > build-and-test-everything.log

for script in $SCRIPTS ; do
   echo '****************************************** Running' $script
   date >> build-and-test-everything.log
   echo '****************************************** Running' $script >> build-and-test-everything.log
   $script | tee -a build-and-test-everything.log
done

echo 'Finished successfully!!!'
