This is a Spark connector for the FIWARE Orion Context Broker. It has two parts:

-   **`OrionReceiver`**: Source for receiving NGSIv2 events in the shape of HTTP messages from subscriptions.

-   **`NGSILDReceiver`**: Source for receiving NGSI-LD events from subscriptions via HTTP.

-   **`OrionSink`**: Sink for writing back to the Context Broker.

In order to install the connector, first you must download the JAR from the latest release.

In your project directory run:

```bash
mvn install:install-file -Dfile=$(PATH_DOWNLOAD)/orion.spark.connector-1.2.2.jar -DgroupId=org.fiware.cosmos -DartifactId=orion.spark.connector -Dversion=1.2.2 -Dpackaging=jar
```

Add it to your `pom.xml` file inside the dependencies section.

```xml
<dependency>
    <groupId>org.fiware.cosmos</groupId>
    <artifactId>orion.spark.connector</artifactId>
    <version>1.2.2</version>
</dependency>
```
