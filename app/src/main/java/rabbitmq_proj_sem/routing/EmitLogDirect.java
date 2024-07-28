package rabbitmq_proj_sem.routing;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import java.nio.charset.StandardCharsets;

public class EmitLogDirect {

  private static final String EXCHANGE_NAME = "direct_logs";

  public static void main(String[] argv) throws Exception {
    ConnectionFactory factory = new ConnectionFactory();
    factory.setHost("localhost");
    try (Connection connection = factory.newConnection();
        Channel channel = connection.createChannel()) {
      channel.exchangeDeclare(EXCHANGE_NAME, "direct");

      String[] serverities = new String[] {"1", "2", "3"};

      for (int msgCnt = 0; msgCnt < 1000s000; msgCnt++) {
        for (String server : serverities) {

          String severity = server;
          String message = "THIS IS THE " + msgCnt + "th OF SERVERITY " + severity;

          channel.basicPublish(
              EXCHANGE_NAME, severity, null, message.getBytes(StandardCharsets.UTF_8));
          System.out.println(" [x] Sent '" + severity + "':'" + message + "'");
        }
      }
    }
  }
  // ..
}
