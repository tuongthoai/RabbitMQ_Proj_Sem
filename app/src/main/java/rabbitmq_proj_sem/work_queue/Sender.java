package rabbitmq_proj_sem.work_queue;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.MessageProperties;

public class Sender {

  private static final String QUEUE_NAME = "hello_queue";

  public static void main(String[] argv) throws Exception {
    ConnectionFactory factory = new ConnectionFactory();
    factory.setHost("localhost");
    try (Connection connection = factory.newConnection();
        Channel channel = connection.createChannel()) {
      channel.queueDeclare(QUEUE_NAME, false, false, false, null);

      for (int i = 0; i < 10; i++) {
        String message = "ID: " + i + " ACTION: Hello World!";
        channel.basicPublish(
            "",
            QUEUE_NAME,
            MessageProperties.PERSISTENT_TEXT_PLAIN,
            message
                .getBytes());
        // the diff is the MessageProperties text plain to allow the Broker to
        // persist the data.
        System.out.println(" [x] Sent '" + message + "'");
      }
    }
  }
}
