package rabbitmq_proj_sem.topic;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

public class ReceiveLogsTopic {

  private static final String EXCHANGE_NAME = "topic_logs";

  public static void main(String[] argv) throws Exception {
    if (argv.length < 1) {
      argv = getFromResourcesFile("params_for_topic_receivelogs");
    }
    ConnectionFactory factory = new ConnectionFactory();
    factory.setHost("localhost");
    Connection connection = factory.newConnection();
    Channel channel = connection.createChannel();

    channel.exchangeDeclare(EXCHANGE_NAME, "topic");
    String queueName = channel.queueDeclare().getQueue();

    if (argv.length < 1) {
      System.err.println("Usage: ReceiveLogsTopic [binding_key]...");
      System.exit(1);
    }

    for (String bindingKey : argv) {
      channel.queueBind(queueName, EXCHANGE_NAME, bindingKey);
    }

    System.out.println(" [*] Waiting for messages. To exit press CTRL+C");

    DeliverCallback deliverCallback =
        (consumerTag, delivery) -> {
          String message = new String(delivery.getBody(), "UTF-8");
          System.out.println(
              " [x] Received '" + delivery.getEnvelope().getRoutingKey() + "':'" + message + "'");
        };
    channel.basicConsume(queueName, true, deliverCallback, consumerTag -> {});
  }

  private static String[] getFromResourcesFile(String filename) throws Exception {
    String content = null;
    try (InputStream input = EmitLogTopic.class.getClassLoader().getResourceAsStream(filename)) {
      assert input != null;
      try (BufferedReader reader = new BufferedReader(new InputStreamReader(input))) {
        content = reader.readLine();
      }
    } catch (IOException e) {
      e.printStackTrace();
    }
    return content != null ? content.split(",", 2) : new String[0]; // Split into 2 parts only
  }
}
