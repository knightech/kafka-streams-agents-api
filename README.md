##kafka-streams-agents-api

##### Get the current schemas held in the confluent regsitry

`curl -s http://localhost:8081/subjects/`

##### Generate test data from here:
* `http://www.convertcsv.com/generate-test-data.htm`
##### or here:
* `https://mockaroo.com`
* `https://www.generatedata.com/`
##### one script for mockaroo using endpoint:
* `curl "https://api.mockaroo.com/api/58605010?count=1000&key=25fd9c80" > "csv-spooldir-source.csv"`

---

##### Add a new agent:
* `curl -d '{"id":"6","firstName":"Betty","lastName":"Jonson","email":"boris@leave.eu"}' -H "Content-Type: application/json" -X POST http://localhost:8009/v1/agents`

##### Get an existing agent:
* `curl localhost:8009/v1/agents/2`

##### Get all agents via consumer (local):
* `confluent local consume agents -- --value-format avro --property print.key=true --from-beginning --property print.key=true --key-deserializer=org.apache.kafka.common.serialization.StringDeserializer`