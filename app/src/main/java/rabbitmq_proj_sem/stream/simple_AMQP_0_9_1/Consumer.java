package rabbitmq_proj_sem.stream.simple_AMQP_0_9_1;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.naming.event.ObjectChangeListener;
import rabbitmq_proj_sem.sender_receiver.Receiver;

public class Consumer {
  private static final String STREAM_NAME = "hello_stream";
  private static final String EXCHANGE_NAME = "stream_exchange";

  public static void main(String[] args) {
    ConnectionFactory cf = new ConnectionFactory();
    cf.setHost("localhost");
    try (Connection conn = cf.newConnection();
        Channel channel = conn.createChannel()) {

      channel.exchangeDeclare(EXCHANGE_NAME, "fanout");
      Map<String, Object> arguments = new HashMap<>();
      arguments.put("x-queue-type", "stream");
      arguments.put("x-max-length-bytes", 20_000_000_000L); // maximum stream size: 20 GB
      arguments.put(
          "x-stream-max-segment-size-bytes", 100_000_000); // size of segment files: 100 MB
      // arguments.put("x-stream-match-unfiltered", true); this is used when the consumer is set to
      // use the filter feature but also want to receive unfiltered messages too.
      channel.queueDeclare(
          STREAM_NAME,
          true, // durable
          false,
          false, // not exclusive, not auto-delete
          arguments);

      ///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
      // FILTER MESSAGE IN STREAM

      //      channel.basicQos(100); // QoS must be specified
      //      channel.basicConsume(
      //          STREAM_NAME,
      //          false,
      //          Collections.singletonMap("x-stream-offset", 30), // "first" offset specification
      //          (consumerTag, message) -> {
      //              String msg = new String(message.getBody(), StandardCharsets.UTF_8);
      //            System.out.println("RECEIVED: " + msg);
      //            channel.basicAck(message.getEnvelope().getDeliveryTag(), false); // ack is
      // required
      //          },
      //          consumerTag -> {});

      Map<String, Object> properties = new HashMap<>();
      properties.put("x-stream-offset", "first");
      properties.put("x-stream-filter", "california");
//      properties.put("x-stream-match-unfiltered", true);
      //      properties.put(
      //          "x-stream-filter", new ArrayList<String>(List.of(new String[] {"california",
      // "usa"})));

      channel.basicQos(100); // QoS must be specified
      channel.basicConsume(
          STREAM_NAME,
          false, // must be false on stream
          properties, // "first" offset specification
          (consumerTag, message) -> {
            Map<String, Object> headers = message.getProperties().getHeaders();

            // there must be some client-side filter logic
            System.out.println(headers);

            if ("california".equals(String.valueOf(headers.get("x-stream-filter-value")))) {
              String msg = new String(message.getBody(), StandardCharsets.UTF_8);
              System.out.println("RECEIVED FILTERED MESSAGE: " + msg);
            } else {
              String msg = new String(message.getBody(), StandardCharsets.UTF_8);
              System.out.println("RECEIVED MESSAGE: " + msg);
            }
            channel.basicAck(message.getEnvelope().getDeliveryTag(), false); // ack is required
          },
          consumerTag -> {});

      // Keep the main thread alive to continue listening for messages
      Runtime.getRuntime()
          .addShutdownHook(
              new Thread(
                  () -> {
                    try {
                      System.out.println("Shutting down...");
                      channel.close();
                      conn.close();
                    } catch (Exception e) {
                      e.printStackTrace();
                    }
                  }));

      // Keep the application running indefinitely
      synchronized (Receiver.class) {
        Receiver.class.wait();
      }

    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
