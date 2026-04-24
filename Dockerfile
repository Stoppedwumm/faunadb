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

# Install bash because the FaunaDB scripts require it
RUN apk add --no-cache bash

# Create directories
RUN mkdir -p /faunadb/bin /faunadb/lib /faunadb/data

# Copy the compiled JAR from the builder stage
COPY --from=builder /app/service/target/scala-2.13/faunadb.jar /faunadb/lib/faunadb.jar

# Copy the startup scripts
COPY --from=builder /app/service/src/main/scripts/faunadb /faunadb/bin/
COPY --from=builder /app/service/src/main/scripts/faunadb-admin /faunadb/bin/
COPY --from=builder /app/service/src/main/scripts/faunadb-backup-s3-upload /faunadb/bin/

# Make scripts executable
RUN chmod +x /faunadb/bin/*

# Set Environment and Ports
ENV PATH="/faunadb/bin:${PATH}"
EXPOSE 8443 8084

ENTRYPOINT ["faunadb"]
