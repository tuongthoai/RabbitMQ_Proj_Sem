package rabbitmq_proj_sem.stream.simple_AMQP_0_9_1;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.AMQP.BasicProperties;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import rabbitmq_proj_sem.work_queue.Receiver;

public class Producer {
  private static final String STREAM_NAME = "hello_stream";
  private static final String EXCHANGE_NAME = "stream_exchange";

  public static void main(String[] argv) throws Exception {
    ConnectionFactory factory = new ConnectionFactory();
    factory.setHost("localhost");
    Connection connection = factory.newConnection();
    Channel channel = connection.createChannel();

    channel.exchangeDeclare(EXCHANGE_NAME, "fanout");
    Map<String, Object> arguments = new HashMap<>();
    arguments.put("x-queue-type", "stream");
    arguments.put("x-max-length-bytes", 20_000_000_000L); // maximum stream size: 20 GB
    arguments.put("x-stream-max-segment-size-bytes", 100_000_000); // size of segment files: 100 MB
    channel.queueDeclare(
        STREAM_NAME,
        true, // durable
        false,
        false, // not exclusive, not auto-delete
        arguments);

    channel.queueBind(STREAM_NAME, EXCHANGE_NAME, "");

    for (int i = 0; i < 100; ++i) {
      channel.basicPublish(
          EXCHANGE_NAME,
          "hello",
          new BasicProperties() // remove the header "x-stream-filter-value" if not  use filter
              .builder()
              .headers(
                  Collections.singletonMap(
                      "x-stream-filter-value",
                      "california")) // set filter value if not filter remove the Properties
              .build(),
          ("Hello World!" + i).getBytes(StandardCharsets.UTF_8));
    }

    channel.close();
    connection.close();
    //        // Keep the main thread alive to continue listening for messages
    //        Runtime.getRuntime()
    //            .addShutdownHook(
    //                new Thread(
    //                    () -> {
    //                        try {
    //                            System.out.println("Shutting down...");
    //                            channel.close();
    //                            connection.close();
    //                        } catch (Exception e) {
    //                            e.printStackTrace();
    //                        }
    //                    }));
    //
    //        // Keep the application running indefinitely
    //        synchronized (Producer.class) {
    //            Receiver.class.wait();
    //        }
  }
}
