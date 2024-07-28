package rabbitmq_proj_sem.topic;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

public class EmitLogTopic {

  private static final String EXCHANGE_NAME = "topic_logs";

  public static void main(String[] argv) throws Exception {
    //    argv = new String[]{"kern.critical", "A critical kernel error"};
    if (argv.length < 1) {
      argv = getFromResourcesFile("params_for_topic_emitlog");
    }
    ConnectionFactory factory = new ConnectionFactory();
    factory.setHost("localhost");
    try (Connection connection = factory.newConnection();
        Channel channel = connection.createChannel()) {

      channel.exchangeDeclare(EXCHANGE_NAME, "topic");

      String routingKey = getRouting(argv);
      String message = getMessage(argv);

      channel.basicPublish(EXCHANGE_NAME, routingKey, null, message.getBytes("UTF-8"));
      System.out.println(" [x] Sent '" + routingKey + "':'" + message + "'");
    }
  }

  private static String getRouting(String[] strings) {
    if (strings.length < 1) return "anonymous.info";
    return strings[0];
  }

  private static String getMessage(String[] strings) {
    if (strings.length < 2) return "Hello World!";
    return joinStrings(strings, " ", 1);
  }

  private static String joinStrings(String[] strings, String delimiter, int startIndex) {
    int length = strings.length;
    if (length == 0) return "";
    if (length < startIndex) return "";
    StringBuilder words = new StringBuilder(strings[startIndex]);
    for (int i = startIndex + 1; i < length; i++) {
      words.append(delimiter).append(strings[i]);
    }
    return words.toString();
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
