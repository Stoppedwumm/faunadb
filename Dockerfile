# --- Stage 1: Build Stage ---
FROM sbtscala/scala-sbt:eclipse-temurin-alpine-17.0.10_7_1.9.9_2.13.13 AS builder

WORKDIR /app

# Copy all source files
COPY . .

# RUN THE ACTUAL COMPILATION (This usually takes 5-10 minutes)
RUN export FAUNADB_RELEASE=true && sbt service/assembly

# --- Stage 2: Runtime Stage ---
FROM eclipse-temurin:17-jre-alpine

WORKDIR /faunadb
RUN apk add --no-cache bash

# Create necessary directories (added /var/log/faunadb)
RUN mkdir -p /faunadb/bin /faunadb/lib /faunadb/data /var/log/faunadb

# Optional: Redirect logs to stdout so 'docker logs' works
# RUN ln -sf /dev/stdout /var/log/faunadb/gc.log

# Define JAVA_OPTS (Keeping your previous required opens)
ENV JAVA_OPTS="-Xmx2g -XX:+UseG1GC -ea \
    --add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED \
    --add-opens=java.base/java.util.concurrent=ALL-UNNAMED \
    --add-opens=java.base/java.util=ALL-UNNAMED \
    --add-opens=java.base/java.io=ALL-UNNAMED \
    --add-opens=java.base/sun.nio.ch=ALL-UNNAMED"

# Copy your config
COPY faunadb.yml /faunadb/faunadb.yml

# Copy the JAR and scripts from builder
COPY --from=builder /app/service/target/scala-2.13/faunadb.jar /faunadb/lib/faunadb.jar
COPY --from=builder /app/service/src/main/scripts/faunadb /faunadb/bin/
COPY --from=builder /app/service/src/main/scripts/faunadb-admin /faunadb/bin/
COPY --from=builder /app/service/src/main/scripts/faunadb-backup-s3-upload /faunadb/bin/

RUN chmod +x /faunadb/bin/*
ENV PATH="/faunadb/bin:${PATH}"

EXPOSE 8443 8084

ENTRYPOINT ["faunadb"]
