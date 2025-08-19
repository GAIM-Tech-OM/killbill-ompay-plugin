#!/bin/sh
#
# Copyright 2025 GAIM-TECH-OM
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at:
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations
# under the License.
#
# In order to generate the Jooq classes either copy and paste the code below or run this through "sh" like:
# sh ./src/main/resources/README.txt
# Load the DDL schema in PostgreSQL
#DDL_DIR="`dirname \"$0\"`"
# Create the killbill database if it doesn't exist
#psql -U postgres -c "CREATE DATABASE killbill;" 2>/dev/null || true
# Load the DDL schema
#psql -U postgres -d killbill -f "${DDL_DIR}"/ddl.sql

# Download the required jars
JOOQ_VERSION=3.15.12
POSTGRES_VERSION=42.5.4
REACTIVE_STREAM_VERSION=1.0.4
R2DBC_SPI_VERSION=1.0.0.RELEASE
JAXB_VERSION=2.3.1
DEPENDENCIES="
  org.jooq:jooq:$JOOQ_VERSION
  org.jooq:jooq-meta:$JOOQ_VERSION
  org.jooq:jooq-codegen:$JOOQ_VERSION
  org.reactivestreams:reactive-streams:$REACTIVE_STREAM_VERSION
  org.postgresql:postgresql:$POSTGRES_VERSION
  io.r2dbc:r2dbc-spi:$R2DBC_SPI_VERSION
  javax.xml.bind:jaxb-api:$JAXB_VERSION
"
for dep in $DEPENDENCIES; do
  mvn org.apache.maven.plugins:maven-dependency-plugin:3.1.2:get \
    -Dartifact=$dep -DremoteRepositories=https://repo.maven.apache.org/maven2
done

M2_REPOS=~/.m2/repository
JARS=$(find "$M2_REPOS" \( -name "jooq-$JOOQ_VERSION.jar" -o \
                         -name "jooq-meta-$JOOQ_VERSION.jar" -o \
                         -name "jooq-codegen-$JOOQ_VERSION.jar" -o \
                         -name "postgresql-$POSTGRES_VERSION.jar" -o \
                         -name "reactive-streams-$REACTIVE_STREAM_VERSION.jar" -o \
                         -name "r2dbc-spi-$R2DBC_SPI_VERSION.jar" -o \
                         -name "jaxb-api-$JAXB_VERSION.jar" \) \
                         | tr '\n' ':')

export JAVA_OPTS='-XX:+IgnoreUnrecognizedVMOptions --add-modules java.se.ee'
# Run jOOQ's generation tool
java -cp $JARS org.jooq.codegen.GenerationTool ./src/main/resources/gen.xml