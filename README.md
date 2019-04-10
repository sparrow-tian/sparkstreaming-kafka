# sparkstreaming-kafka

Using spark to consume streaming data by sparkstreaming moudle, there are two different way to consume kafka streaming data, the first one is Receiver-based Approach, using the Kafka high-level consumer API, and the second way is Direct Approach, This way has the much more advantages than the first way. Eg:
* **Simplified Parallelism**
* **Efficiency**
* **Exactly-once semantics**

for more detail can check [Spark Streaming + Kafka Integration Guide](https://spark.apache.org/docs/2.2.0/streaming-kafka-0-8-integration.html)

Direct Approach has two different ways to save offset, utilize checkpoint and save data to zookpper.
this demo demonstrate two different ways to save offset and restart from last position respectively.
