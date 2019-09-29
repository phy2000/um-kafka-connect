# um-kafka-connect
(WIP) Kafka Connect Sink and Source Connector for Ultramessaging/29West

This README will be updated when the connector is functionally complete. 

## To build:
1. Add the Ultramessaging jars to your local Maven repository:
```
mvn install:install-file -Dfile=UMS_6.12.jar -DgroupId=com.latencybusters \
    -DartifactId=UMS -Dversion=6.12 -Dpackaging=jar
mvn install:install-file -Dfile=UMSSDM_6.12.jar -DgroupId=com.latencybusters \
    -DartifactId=UMSSDM -Dversion=6.12 -Dpackaging=jar
mvn install:install-file -Dfile=UMSPDM_6.12.jar -DgroupId=com.latencybusters \
    -DartifactId=UMSPDM -Dversion=6.12 -Dpackaging=jar
```
2. Maven build:
```
mvn clean install package
```
3. An uber jar package is in:
```
target/ultramessaging-<version>.jar
```
