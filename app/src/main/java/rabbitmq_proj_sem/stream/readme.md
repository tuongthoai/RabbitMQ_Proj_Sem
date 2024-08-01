# RabbitMQ Stream
## Definition 
Stream is a persistent replicated data structure that can complete the same task as queue.
> The differences are in 2 points
> + Stream is always persistent and replicated, append-only log of messages and can be read multiple times.
> + Consumers which subscribed to it can read the same message as many times as they want

## Uses case
Stream and Queue are complement. Stream usually prefer in the following scenarios:

+ Large Fan-outs
  + Current Limitation: Delivering the same message to multiple subscribers requires a dedicated queue for each consumer, leading to inefficiencies.
  + Streams Solution: Allows any number of consumers to consume the same messages from the same queue non-destructively, eliminating the need for multiple queues.
  + Replication: Consumers can read from replicas, distributing read load across the cluster.
+ Replay (Time-traveling)
  + Current Limitation: Existing RabbitMQ queues delete messages once consumed, preventing re-reading.
  + Streams Solution: Consumers can attach at any point in the log and re-read messages from that position.
+ Throughput Performance
  + Current Limitation: No persistent queue types can match the throughput of log-based messaging systems.
  + Streams Solution: Designed with performance as a primary goal to deliver high throughput.
+ Large Backlogs
  + Current Limitation: Most RabbitMQ queues perform poorly with millions of messages, as they are optimized to be empty.
  + Streams Solution: Efficiently stores large amounts of data with minimal in-memory overhead, performing well even with large backlogs.

## How to use a Stream 

We must use the client library to be able to declare the stream or using the ManagementUI
```java
ConnectionFactory factory = new ConnectionFactory();
Connection connection = factory.newConnection();
Channel channel = connection.createChannel();
channel.queueDeclare(
  "my-stream", // stream_name
  true,         // durable
  false, false, // not exclusive, not auto-delete
  Collections.singletonMap("x-queue-type", "stream")
);
```
The most important parameter is to set an additional parameters `x-queue-type` to `stream`.

We as users can configure some additional parameters:
+ `x-max-length-bytes`: Sets the maximum size of the stream in bytes.
+ `x-max-age`: Sets the maximum age of the stream.
+ `x-stream-max-segment-size-bytes`: A stream is divided up into fixed size segment files on disk. This setting controls the size of these. Default: (500 000 000 bytes)
+ `x-stream-filter-size-bytes`: 

The code that demonstrate how to add these parameters:
```java
Map<String, Object> arguments = new HashMap<>();
arguments.put("x-queue-type", "stream");
arguments.put("x-max-length-bytes", 20_000_000_000); // maximum stream size: 20 GB
arguments.put("x-stream-max-segment-size-bytes", 100_000_000); // size of segment files: 100 MB
channel.queueDeclare(
  "my-stream",
  true,         // durable
  false, false, // not exclusive, not auto-delete
  arguments
);
```

## Client Operations (The services which subscribe to the stream)

By set the `x-stream-offset` parameter at the client code we can consume the stream from anywhere.
+ `first`, `last`, `next`: the first and the second i read from the beginning or the latest chunk (a batch of messages) and the final is read the latest record.
+ Offset: passing a long value which determine the offset.
+ Timestamp: passing a Date object since the RabbitMQ can hold up to second precision.
+ Interval:  a string value specifying the time interval relative to current time to attach the log at. Uses the same specification as `x-max-age`

```java
channel.basicQos(100); // QoS must be specified
// Collections.singletonMap("x-stream-offset", 5000), // offset value
// Collections.singletonMap("x-stream-offset", timestamp), // timestamp offset
channel.basicConsume(
  "my-stream",
  false,
  Collections.singletonMap("x-stream-offset", "first"), // "first" offset specification, 
  (consumerTag, message) -> {
    // message processing
    // ...
   channel.basicAck(message.getEnvelope().getDeliveryTag(), false); // ack is required
  },
  consumerTag -> { });
```

### Other operations
* Declaration
* Queue deletion
* Publisher confirms
* Consumption (subscription): consumption requires QoS prefetch to be set. The acks works as a credit mechanism to advance the current offset of the consumer.
* Setting QoS prefetch for consumers
* Consumer acknowledgements (keep QoS Prefetch Limitations in mind)
* Cancellation of consumers

All of them are the same as queue.

## Single Active Consumer Feature for Streams
It provides exclusive consumption and consumption continuity on a stream. 
When several consumer instances sharing the same stream and name enable single active consumer, 
only one of these instances will be active at a time and so will receive messages. The other instances will be idle.

The single active consumer feature provides 2 benefits:

* Messages are processed in order: there is only one consumer at a time.
* Consumption continuity is maintained: a consumer from the group will take over if the active one stops or crashes.

This feature acts as a cluster of consumers and only one of them will process the data but when one or more nodes are down.
One of the remaining nodes will replace the missing one.

We'll start the 3 consumers in the same process for simplicity's sake. Here is the definition of a consumer with the stream Java client:
```java
System.out.println("Starting consumer instance " + i);
Consumer consumer = environment.consumerBuilder()
    .stream(stream)
    .name(reference) // this one is the name of the application
    .singleActiveConsumer()
    .autoTrackingStrategy()
        .messageCountBeforeStorage(10)
    .builder()
    .messageHandler((context, message) -> {
        System.out.printf(
            "Consumer instance %d received a message (%d).%n",
            i, sequence.incrementAndGet()
        );
    })
    .build();
```
The code to declare a single active consumer is almost the same as for a regular consumer: you must provide a name and use ConsumerBuilder#singleActiveConsumer(). The i and sequence variables are just there to help understand what's going on.


https://www.rabbitmq.com/blog/2022/07/05/rabbitmq-3-11-feature-preview-single-active-consumer-for-streams

## Compare to simple queue (https://www.rabbitmq.com/blog/2021/07/13/rabbitmq-streams-overview)
![img.png](img.png)
![img_1.png](img_1.png)
![img_2.png](img_2.png)
![img_3.png](img_3.png)
![img_4.png](img_4.png)
![img_5.png](img_5.png)
![img_6.png](img_6.png)
![img_7.png](img_7.png)

## Performance Comparison

![img_8.png](img_8.png)
![img_9.png](img_9.png)

## To start a Docker Container for locally development with RabbitMQ
https://rabbitmq.github.io/rabbitmq-stream-java-client/stable/htmlsingle/

## RabbitMQ Stream Java API
Source: https://rabbitmq.github.io/rabbitmq-stream-java-client/stable/htmlsingle/#rabbitmq-stream-java-api

### Main components
1. com.rabbitmq.stream.Environment for connecting to a node and optionally managing streams.
2. com.rabbitmq.stream.Producer to publish messages.
3. com.rabbitmq.stream.Consumer to consume messages

### Environment

The most crucial part as it is the main entry point to a node or a cluster of nodes and the Consumer and Producer is created from this `Environment`

> Localhost:
> 
> Environment environment = Environment.builder().build(); // to localhost:5552  
> // ...
> environment.close();

Single Node:
```java
Environment environment = Environment.builder()
        .uri("rabbitmq-stream://guest:guest@localhost:5552/%2f")  // https://www.rabbitmq.com/docs/uri-spec****
        .build();
```

MultiNode:
```java
Environment environment = Environment.builder()
        .uris(Arrays.asList(                     
                "rabbitmq-stream://host1:5552",
                "rabbitmq-stream://host2:5552",
                "rabbitmq-stream://host3:5552")
        )
        .build();
```

### Managing Stream 
To create a stream:
> environment.streamCreator().stream("my-stream").create();

The "my-stream" is the stream name

To delete stream 
> environment.deleteStream("my-stream");

#### Note:
It is necessary to create a stream with the same name and properties multiple times since it will create once.
However, create multiple stream with the same name and different properties is throwing an exception 

#### Stream retention policy:
The rabbitMq supports 2 types:
First is Size base.
```java
environment.streamCreator()
        .stream("my-stream")
        .maxLengthBytes(ByteCapacity.GB(10))  
        .maxSegmentSizeBytes(ByteCapacity.MB(500))  
        .create();
```
> Set the maximum size to 10 GB

> Set the segment size to 500 MB

Segment size is a single file to store a single part of a stream. If RabbitMQ Stream remove data it will remove entire segment.

The second one is time base.
```java
environment.streamCreator()
        .stream("my-stream")
        .maxAge(Duration.ofHours(6))  
        .maxSegmentSizeBytes(ByteCapacity.MB(500))  
        .create();
```

### Producer
#### 1. Creation
It must be created using the `Environment`. The Producer should be use to send multiple records (not one record only).

```java
Producer producer = environment.producerBuilder()  
        .stream("my-stream")  
        .build();  
// ...
producer.close();
```
+ Use Environment#producerBuilder() to define the producer
+ Specify the stream to publish to
+ Create the producer instance with build()
+ Close the producer after usage
> Producer is thread-safe

The parameter that we can set to create a producer

| Parameter Name           | Description                                                                                                                                                                                                                                                                                                                                                                                            | Default                                  |
|--------------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|------------------------------------------|
| `stream`                 | The stream to publish to.                                                                                                                                                                                                                                                                                                                                                                              | No default, mandatory setting.           |
| `name`                   | The logical name of the producer. Specify a name to enable [message deduplication](link).                                                                                                                                                                                                                                                                                                              | `null` (no deduplication)                |
| `batchSize`              | The maximum number of messages to accumulate before sending them to the broker.                                                                                                                                                                                                                                                                                                                        | 100                                      |
| `subEntrySize`           | The number of messages to put in a sub-entry. A sub-entry is one "slot" in a publishing frame, meaning outbound messages are not only batched in publishing frames, but in sub-entries as well. Use this feature to increase throughput at the cost of increased latency and potential duplicated messages even when deduplication is enabled. See the [dedicated section](link) for more information. | 1 (meaning no use of sub-entry batching) |
| `compression`            | Compression algorithm to use when sub-entry batching is in use. See the [dedicated section](link) for more information.                                                                                                                                                                                                                                                                                | `Compression.NONE`                       |
| `maxUnconfirmedMessages` | The maximum number of unconfirmed outbound messages. `Producer#send` will start blocking when the limit is reached.                                                                                                                                                                                                                                                                                    | 10,000                                   |
| `batchPublishingDelay`   | Period to send a batch of messages.                                                                                                                                                                                                                                                                                                                                                                    | 100 ms                                   |
| `confirmTimeout`         | Time before the client calls the confirm callback to signal outstanding unconfirmed messages timed out.                                                                                                                                                                                                                                                                                                | 30 seconds                               |
| `enqueueTimeout`         | Time before enqueueing of a message fail when the maximum number of unconfirmed is reached. The callback of the message will be called with a negative status. Set the value to `Duration.ZERO` if there should be no timeout.                                                                                                                                                                         | 10 seconds                               |

#### 2. Sending message

