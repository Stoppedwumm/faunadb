# --- Stage 1: Build Stage ---
FROM sbtscala/scala-sbt:eclipse-temurin-17-alpine AS builder

WORKDIR /app

# Copy the project files
COPY . .

# Set release flag and build the assembly JAR (fat JAR)
RUN export FAUNADB_RELEASE=true && sbt service/assembly

# --- Stage 2: Runtime Stage ---
FROM openjdk:17-slim

WORKDIR /faunadb

# Create the directory structure expected by the scripts
RUN mkdir -p /faunadb/bin /faunadb/lib /faunadb/data

# Copy the JAR from the builder stage
# (Wildcard used in case version numbers are appended)
COPY --from=builder /app/service/target/scala-2.13/faunadb.jar /faunadb/lib/faunadb.jar

# Copy the scripts from the source
COPY --from=builder /app/service/src/main/scripts/faunadb /faunadb/bin/
COPY --from=builder /app/service/src/main/scripts/faunadb-admin /faunadb/bin/
COPY --from=builder /app/service/src/main/scripts/faunadb-backup-s3-upload /faunadb/bin/

# Make scripts executable
RUN chmod +x /faunadb/bin/*

# Add bin to PATH
ENV PATH="/faunadb/bin:${PATH}"

# Expose default FaunaDB ports
# 8443: API / Database
# 8084: GraphQL
EXPOSE 8443 8084

# Default entry point is the faunadb script
ENTRYPOINT ["faunadb"]
