# Kafka Topic Mirror (KTM)

Kafka Topic Mirror (KTM) is a service designed to replicate messages from one Kafka cluster to another Kafka cluster or within the same cluster from one topic to another topic. It will have single kafka consumer from source cluster/topic and a producer to destination cluster/topic. 

## Features
- **Cluster to Cluster Mirroring**: Replicate messages from a source Kafka cluster to a destination Kafka cluster.
- **Topic to Topic Mirroring**: Mirror messages within the same Kafka cluster from one topic to another.
- **High Throughput**: Designed to handle high message throughput with minimal latency. Its single threaded, can be improved both from consumer and producer side by having one consumer per partition and same desination side.
- **ZERO loss of messages** : Service guranteed delivery of each msg from source topic to destination topic. This has been done by commiting to source kafka cluster it has produced successfully to destination kafka cluster.   
Side effect of this - if service crashes or not able to commit - it can produce the same message again in destination cluster.
- **Inbuilt Prometheus metrics** - metrics for messages consumed and produced over time.


## Workflow

1. **KafkaConsumer Initialization**
   - The `KafkaConsumer` is initialized with configuration settings, output message channel, and commit message channel.

2. **Message Consumption**
   - The `KafkaConsumer` consumes messages from the source Kafka topic.
   - Consumed messages are sent to the `outputMsgs` channel.
   - Messages are also tracked in the `commitMsgs` channel for later commiting to source kafka cluster.

3. **KafkaProducer Initialization**
   - The `KafkaProducer` is initialized with configuration settings, input message channel, and commit message channel.

4. **Message Production**
   - The `KafkaProducer` reads messages from the `inputMsgs` channel.
   - Messages are batched and sent to the destination Kafka topic.
   - Upon successful production, messages are acknowledged and committed using the `commitMsgs` channel.

5. **Message Commit**
   - The `KafkaConsumer` Commits messages from `commitMsgs` channel to source kafka cluster.
   

## Installation

To install Kafka Topic Mirror, clone the repository and build the project:

```sh
git clone https://github.com/your-repo/kafka-topic-mirror.git
cd kafka-topic-mirror
go build

```
