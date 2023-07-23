# Sample Kafka Stream applications
This project is to demo a simple Kafka Stream project using both Spring and Spring Cloud.
The purpose is to show the differences in both solutions.

## How to start
* **Via Docker to prepare environment**
```bash 
cd standalone/ && docker compose up
```
* Run the main in the core/ folder or in the cloud/ folder
* Produce to the input topic
```bash
docker exec -ti kafka /bin/bash
kafka-console-producer --broker-list localhost:9092 --topic favorite-color-input
# enter some messages, e.g. 
# john,red
# jane,blue
# john,green 
* ```

* See the output via consumers
```bash
docker exec -ti kafka /bin/bash
kafka-console-consumer --bootstrap-server localhost:9092 --topic user-keys-and-colors --from-beginning --property print.key=true --property print.value=true
* ```
```bash
docker exec -ti kafka /bin/bash
kafka-console-consumer --bootstrap-server localhost:9092 --topic favorite-color-output --from-beginning --property print.key=true --property print.value=true \
--property key.deserializer=org.apache.kafka.common.serialization.StringDeserializer \
--property value.deserializer=org.apache.kafka.common.serialization.LongDeserializer
```

* Or you can use postman to see the result via REST
```markdown
/GET localhost:8080/count/{color}
```

## Explain
Let's say we have an input topic that has message <"null">:<"user">,<"color"> indicating a user just likes a color.
Now we want to count the total number of liked color with the following criterias:
* Each person could only have 1 favorite color.
* We only care about "red", "green", "blue" colors.

**Input**
* john,red
* john,blue
* jane,green
* harry,green

**Expecting output**
* red: 0
* blue: 1
* green: 2