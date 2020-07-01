#! /bin/bash -e

BRANCH=$(git rev-parse --abbrev-ref HEAD)

if [  $BRANCH == "master" ] ; then
  VERSION=$(sed -e '/^version=/!d' -e 's/^version=\(.*\)-SNAPSHOT$/\1.BUILD-SNAPSHOT/' < gradle.properties)
  echo master: publishing $VERSION
  ./gradlew -P version=$VERSION -P deployUrl=${S3_REPO_DEPLOY_URL} uploadArchives
else

  if ! [[  $BRANCH =~ ^[0-9]+ ]] ; then
    echo Not release $BRANCH - no PUSH
    exit 0
  elif [[  $BRANCH =~ RELEASE$ ]] ; then
    BINTRAY_REPO_TYPE=release
  elif [[  $BRANCH =~ M[0-9]+$ ]] ; then
      BINTRAY_REPO_TYPE=milestone
  elif [[  $BRANCH =~ RC[0-9]+$ ]] ; then
      BINTRAY_REPO_TYPE=rc
  else
    echo cannot figure out bintray for this branch $BRANCH
    exit -1
  fi

  echo BINTRAY_REPO_TYPE=${BINTRAY_REPO_TYPE}

  VERSION=$BRANCH

  $PREFIX ./gradlew -P version=${VERSION} \
    -P bintrayRepoType=${BINTRAY_REPO_TYPE} \
    -P deployUrl=https://dl.bintray.com/eventuateio-oss/eventuate-maven-${BINTRAY_REPO_TYPE} \
    testClasses bintrayUpload

fi

DOCKER_REPO=eventuateio

# This is current directory name, right?
# But CircleCI is using older version

DOCKER_COMPOSE_PREFIX=eventuatecdc_

$PREFIX ./gradlew assemble
docker-compose -f docker-compose-mysql.yml -f docker-compose-cdc-mysql-binlog.yml build cdcservice


function tagAndPush() {
  LOCAL=$1
  REMOTE=$2
  $PREFIX docker tag ${DOCKER_COMPOSE_PREFIX?}$LOCAL $DOCKER_REPO/$REMOTE:$VERSION
  $PREFIX docker tag ${DOCKER_COMPOSE_PREFIX?}$LOCAL $DOCKER_REPO/$REMOTE:latest
  echo Pushing $DOCKER_REPO/$REMOTE:$VERSION
  $PREFIX docker push $DOCKER_REPO/$REMOTE:$VERSION
  $PREFIX docker push $DOCKER_REPO/$REMOTE:latest
}

$PREFIX docker login -u ${DOCKER_USER_ID?} -p ${DOCKER_PASSWORD?}

docker images

tagAndPush "cdcservice" "eventuate-cdc-service"
