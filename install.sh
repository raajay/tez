#!/bin/bash
mvn package -DskipTests=true -Dmaven.javadoc.skip=true
mvn install -DskipTests=true -Dmaven.javadoc.skip=true
