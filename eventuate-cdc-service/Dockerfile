FROM java:openjdk-8u91-jdk

CMD java ${JAVA_OPTS} -jar eventuate-cdc-service-0.2.0.RELEASE.jar ${CDC_OPTS}
HEALTHCHECK --start-period=30s --interval=5s CMD curl -f http://localhost:8080/actuator/health || exit 1
COPY build/libs/eventuate-cdc-service-0.2.0.RELEASE.jar .
