#!/bin/bash
GROUPID=com.latencybusters 
VERSION=6.12

JARFILE=UMS_6.12.jar
ARTIFACT=UMS
echo mvn install:install-file -Dfile=$JARFILE -DgroupId=$GROUPID -DartifactId=$ARTIFACT -Dversion=6.12 -Dpackaging=jar
JARFILE=UMSPDM_6.12.jar
ARTIFACT=UMSPDM
echo mvn install:install-file -Dfile=$JARFILE -DgroupId=$GROUPID -DartifactId=$ARTIFACT -Dversion=6.12 -Dpackaging=jar
JARFILE=UMSSDM_6.12.jar
ARTIFACT=UMSSDM
echo mvn install:install-file -Dfile=$JARFILE -DgroupId=$GROUPID -DartifactId=$ARTIFACT -Dversion=6.12 -Dpackaging=jar
