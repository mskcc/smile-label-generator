# CMO Label Generator 🔍

CMO Label Generator is a distributed microservices system. It receives messages from LIMS when a request is marked delivered. This message is processed and a unique CMO label Id is generated based on specifications that can be found in CMOPatientIDandSampleIDgeneration.pdf. Any new message is then published to downstream subscribers. The message published is a JSON reqpresentation of SmileRequest (more details can be found in smile-server repository). 

Label Generator is dependent on SMILE, Nats Jetstream and Nats RequestReply.

## Run

### Custom properties

Make an `application.properties` based on [application.properties.EXAMPLE](src/main/resources/application.properties.EXAMPLE).

Nats Pub/Sub topics:
- `igo.cmo_label_generator_topic` : receives requests to process
- `request_reply.patient_samples_topic` : used to patient samples set from SMILE Server
- `igo.new_request_topic` : publishes the processed request to this topic

All properties are required with the exception of some NATS connection-specific properties. The following are only required if `nats.tls_channel` is set to `true`:

Nats Connection props:
- `nats.keystore_path` : path to client keystore
- `nats.truststore_path` : path to client truststore
- `nats.key_password` : keystore password
- `nats.store_password` : truststore password

### Locally

**Requirements:**
- maven 3.6.1
- java 8

Add `application.properties` to the local application resources: `src/main/resources`

Build with

```
mvn clean install
```

Run with

```
java -jar server/target/smile_label_generator.jar
```

### With Docker

**Requirements**
- docker

Build image with Docker

```
docker build -t <repo>/<tag>:<version> .
```

Push image to DockerHub

```
docker push <repo>/<tag>:<version>
```

If the Docker image is built with the properties baked in then simply run with:


```
docker run --name cmo-label-generator <repo>/<tag>:<version> \
	-jar /label-generator/smile_label_generator.jar
```

Otherwise use a bind mount to make the local files available to the Docker image and add  `--spring.config.location` to the java arg

```
docker run --mount type=bind,source=<local path to properties files>,target=/label-generator/src/main/resources \
	--name label-generator <repo>/<tag>:<version> \
	-jar /label-generator/smile_label_generator.jar \
	--spring.config.location=/label-generator/src/main/resources/application.properties
```
