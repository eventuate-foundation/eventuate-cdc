FROM amazoncorretto:17.0.1-al2

CMD java ${JAVA_OPTS} -jar eventuate-cdc-service-*.jar ${CDC_OPTS}
HEALTHCHECK --interval=5s --retries=20 CMD curl -f http://localhost:8080/actuator/health || exit 1
COPY build/libs/eventuate-cdc-service-*SNAPSHOT.jar .
