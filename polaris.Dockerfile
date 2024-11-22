#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#  http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#

# Base Image
# Use a non-docker-io registry, because pulling images from docker.io is
# subject to aggressive request rate limiting and bandwidth shaping.
FROM registry.access.redhat.com/ubi9/openjdk-21:1.20-2.1726695192 AS build

# Copy the REST catalog into the container
COPY --chown=default:root polaris/. /app

# Set the working directory in the container, nuke any existing builds
WORKDIR /app
RUN rm -rf build

RUN curl -L -o /tmp/yq https://github.com/mikefarah/yq/releases/download/v4.44.5/yq_linux_arm64 &&\
    chmod +x /tmp/yq

# PG default database = default realm because we need to make sure the database is created before the service starts
ENV POLARIS_DEFAULT_REALM=default
ENV PGSERVER="jdbc:postgresql://postgres:5432/default"
ENV POLARIS_PG_USER=pyiceberg
ENV POLARIS_PG_PASS=pyiceberg

# Massive thanks @loicalleyne who authored this comment https://github.com/apache/polaris/issues/230#issuecomment-2354161079
RUN /tmp/yq -i '.persistence.persistence-unit.properties.property[0].+@value = strenv(PGSERVER) | .persistence.persistence-unit.properties.property[1].+@value = strenv(POLARIS_PG_USER) | .persistence.persistence-unit.properties.property[2].+@value = strenv(POLARIS_PG_PASS) | .persistence.persistence-unit.properties.property += { "+@name":"jakarta.persistence.jdbc.driver","+@value":"org.postgresql.Driver"}' /app/extension/persistence/eclipselink/src/main/resources/META-INF/persistence.xml
RUN /tmp/yq -i '.metaStoreManager.type = "eclipse-link" | .metaStoreManager += {"persistence-unit": "polaris"} | .defaultRealms[0] = strenv(POLARIS_DEFAULT_REALM)' /app/polaris-server.yml
RUN ./gradlew --no-daemon --info -PeclipseLink=true -PeclipseLinkDeps=org.postgresql:postgresql:42.7.4 clean prepareDockerDist

FROM registry.access.redhat.com/ubi9/openjdk-21-runtime:1.20-2.1729089285
WORKDIR /app
COPY --from=build /app/polaris-service/build/docker-dist/bin /app/bin
COPY --from=build /app/polaris-service/build/docker-dist/lib /app/lib
COPY --from=build /app/polaris-server.yml /app
COPY --from=build /app/extension/persistence/eclipselink/src/main/resources/META-INF/persistence.xml /app

EXPOSE 8181

# Run the resulting java binary
ENTRYPOINT ["/app/bin/polaris-service"]
CMD ["server", "polaris-server.yml"]
