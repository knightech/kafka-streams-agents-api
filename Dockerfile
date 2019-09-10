FROM openjdk:8-jdk-alpine
ENV APP_FILE springbootkbe-0.0.5-SNAPSHOT.jar
ENV APP_HOME /usr/app

COPY build/libs/*.jar $APP_HOME/

RUN mkdir -p /prometheus-config
COPY prometheus-config/jmx_prometheus_javaagent.jar /tmp/jmx_prometheus_javaagent.jar
COPY prometheus-config/prometheus-config.yml /prometheus-config/prometheus-config.yml
COPY prometheus-config/prometheus-logging.properties /prometheus-config/prometheus-logging.properties

WORKDIR $APP_HOME
ENTRYPOINT ["sh", "-c"]
ENV KAFKA_JMX_OPTS -Djava.util.logging.config.file=/prometheus-config/prometheus-logging.properties -Dcom.sun.management.jmxremote -Dcom.sun.management.jmxremote.authenticate=false -Dcom.sun.management.jmxremote.ssl=false -javaagent:/tmp/jmx_prometheus_javaagent.jar=5182:/prometheus-config/prometheus-config.yml
CMD ["exec java $KAFKA_JMX_OPTS -jar $APP_FILE"]

EXPOSE 8000 5182