package rabbitmq_proj_sem.work_queue;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;
import java.nio.charset.StandardCharsets;

public class Receiver {

  private static final String QUEUE_NAME = "hello_queue";

  public static void main(String[] argv) throws Exception {
    ConnectionFactory factory = new ConnectionFactory();
    factory.setHost("localhost");
    Connection connection = factory.newConnection();
    Channel channel = connection.createChannel();

    boolean durable = true;
    channel.queueDeclare(QUEUE_NAME, durable, false, false, null);
    System.out.println(" [*] Waiting for messages. To exit press CTRL+C");

    DeliverCallback deliverCallback =
        (consumerTag, delivery) -> {
          String message = new String(delivery.getBody(), StandardCharsets.UTF_8);

          System.out.println(" [x] Received '" + message + "'");
          try {
            doWork(message);
          } catch (Exception ex) {
            ex.printStackTrace();
          } finally {
            System.out.println(" [x] Done");
            channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
          }
        };

    boolean autoAck = false;
    channel.basicConsume(QUEUE_NAME, autoAck, deliverCallback, consumerTag -> {});
    channel.basicQos(1); // accept only one unack-ed message at a time

    // Keep the main thread alive to continue listening for messages
    Runtime.getRuntime()
        .addShutdownHook(
            new Thread(
                () -> {
                  try {
                    System.out.println("Shutting down...");
                    channel.close();
                    connection.close();
                  } catch (Exception e) {
                    e.printStackTrace();
                  }
                }));

    // Keep the application running indefinitely
    synchronized (Receiver.class) {
      Receiver.class.wait();
    }
  }

  private static void doWork(String task) throws InterruptedException {
    for (char ch : task.toCharArray()) {
      if (ch == '.') Thread.sleep(1000);
    }
  }
}
