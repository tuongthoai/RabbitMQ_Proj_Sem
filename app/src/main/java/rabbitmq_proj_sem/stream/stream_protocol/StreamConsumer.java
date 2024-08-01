package rabbitmq_proj_sem.stream.stream_protocol;

import com.rabbitmq.stream.Environment;
import com.rabbitmq.stream.*;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.IntStream;
import com.rabbitmq.stream.Consumer;
import com.rabbitmq.stream.Producer;

public class StreamConsumer {
  public static void main(String[] args) throws InterruptedException {
    // tag::sample-environment[]
    System.out.println("Connecting...");
    Environment environment = Environment.builder().build(); // <1>
    String stream = UUID.randomUUID().toString();
    environment.streamCreator().stream(stream).create(); // <2>
    // end::sample-environment[]
    // tag::sample-publisher[]
    System.out.println("Starting publishing...");
    int messageCount = 10000;
    CountDownLatch publishConfirmLatch = new CountDownLatch(messageCount);
    Producer producer = environment.producerBuilder().stream(stream).build();
    IntStream.range(0, messageCount)
        .forEach(
            i ->
                producer.send( // <2>
                    producer
                        .messageBuilder() // <3>
                        .addData(String.valueOf(i).getBytes()) // <3>
                        .build(), // <3>
                    confirmationStatus -> publishConfirmLatch.countDown() // <4>
                    ));
    publishConfirmLatch.await(10, TimeUnit.SECONDS); // <5>
    ((com.rabbitmq.stream.Producer) producer).close(); // <6>
    System.out.printf("Published %,d messages%n", messageCount);
    // end::sample-publisher[]

    // tag::sample-consumer[]
    System.out.println("Starting consuming...");
    AtomicLong sum = new AtomicLong(0);
    CountDownLatch consumeLatch = new CountDownLatch(messageCount);
    Consumer consumer =
        environment
            .consumerBuilder() // <1>
            .stream(stream)
            .offset(OffsetSpecification.first()) // <2>
            .messageHandler(
                (offset, message) -> { // <3>
                  sum.addAndGet(Long.parseLong(new String(message.getBodyAsBinary()))); // <4>
                  consumeLatch.countDown(); // <5>
                })
            .build();

    consumeLatch.await(10, TimeUnit.SECONDS); // <6>

    System.out.println("Sum: " + sum.get()); // <7>

    consumer.close(); // <8>
    // end::sample-consumer[]

    // tag::sample-environment-close[]
    environment.deleteStream(stream); // <1>
    environment.close(); // <2>
    // end::sample-environment-close[]
  }
}
