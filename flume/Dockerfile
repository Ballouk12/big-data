FROM eclipse-temurin:8-jre

ENV FLUME_VERSION=1.11.0
ENV BOOTSTRAP_SERVERS=kafka-bd:9092

RUN apt-get update && apt-get install -y bash curl tar ca-certificates && rm -rf /var/lib/apt/lists/*

RUN curl -f -L -o flume.tar.gz https://dlcdn.apache.org/flume/${FLUME_VERSION}/apache-flume-${FLUME_VERSION}-bin.tar.gz \
    && ls -lh flume.tar.gz \
    && tar -xzf flume.tar.gz \
    && rm flume.tar.gz \
    && mv apache-flume*-bin /opt/flume

ENV PATH=/opt/flume/bin:$PATH

WORKDIR /opt/flume

# Entrypoint to start Flume agent
CMD ["flume-ng", "agent", "-n", "a1", "-c", "conf", "-f", "conf/flume.conf", "-Dflume.root.logger=INFO,console"]
