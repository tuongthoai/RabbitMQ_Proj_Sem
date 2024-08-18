package rabbitmq_proj_sem.routing;

import com.rabbitmq.client.*;

public class ReceiveLogsDirect {

  private static final String EXCHANGE_NAME = "direct_logs";

  public static void main(String[] argv) throws Exception {
    ConnectionFactory factory = new ConnectionFactory();
    factory.setHost("localhost");
    Connection connection = factory.newConnection();
    Channel channel = connection.createChannel();

    channel.exchangeDeclare(EXCHANGE_NAME, "direct");
    String queueName = channel.queueDeclare().getQueue();

    String[] routingKeys = new String[] {"1", "2", "3"};
    channel.queueBind(queueName, EXCHANGE_NAME, routingKeys[0]);

    System.out.println(" [*] Waiting for messages. To exit press CTRL+C");

    DeliverCallback deliverCallback =
        (consumerTag, delivery) -> {
          String message = new String(delivery.getBody(), "UTF-8");
          System.out.println(
              " [x] Received '" + delivery.getEnvelope().getRoutingKey() + "':'" + message + "'");
        };
    channel.basicConsume(queueName, true, deliverCallback, consumerTag -> {});
  }
}
