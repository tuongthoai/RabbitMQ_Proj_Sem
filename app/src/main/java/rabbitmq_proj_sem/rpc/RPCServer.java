package rabbitmq_proj_sem.rpc;

import com.rabbitmq.client.*;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class RPCServer {
  private static final String RPC_QUEUE_NAME = "rpc_queue";
  private static final Map<String, String> cache = new ConcurrentHashMap<>();

  private static int fib(int n) {
    if (n == 0) return 0;
    if (n == 1) return 1;
    return fib(n - 1) + fib(n - 2);
  }

  public static void main(String[] argv) throws Exception {
    ConnectionFactory factory = new ConnectionFactory();
    factory.setHost("localhost");

    try (Connection connection = factory.newConnection();
        Channel channel = connection.createChannel()) {
      channel.queueDeclare(RPC_QUEUE_NAME, false, false, false, null);
      channel.queuePurge(RPC_QUEUE_NAME);
      channel.basicQos(1);

      System.out.println(" [x] Awaiting RPC requests");

      Object monitor = new Object();
      DeliverCallback deliverCallback =
          (consumerTag, delivery) -> {
            AMQP.BasicProperties replyProps =
                new AMQP.BasicProperties.Builder()
                    .correlationId(delivery.getProperties().getCorrelationId())
                    .build();

            String response = "";

            try {
              String corrId = delivery.getProperties().getCorrelationId();
              String message = new String(delivery.getBody(), StandardCharsets.UTF_8);
              int n = Integer.parseInt(message);

              if (cache.containsKey(corrId)) {
                System.out.println(" [.] Duplicate request! Cached response: " + cache.get(corrId));
                response = "Duplicate request! Cached response: " + cache.get(corrId);
              } else {
                int result = fib(n);
                response += "Response: " + result;
                cache.put(corrId, response);
              }

              System.out.println(" [.] fib(" + message + ") " + corrId);
            } catch (RuntimeException e) {
              System.out.println(" [.] " + e.toString());
            } finally {
              channel.basicPublish(
                  "",
                  delivery.getProperties().getReplyTo(),
                  replyProps,
                  response.getBytes("UTF-8"));
              channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false); // ack

              synchronized (monitor) {
                monitor.notify();
              }
            }
          };

      channel.basicConsume(RPC_QUEUE_NAME, false, deliverCallback, (consumerTag -> {}));

      while (true) {
        synchronized (monitor) {
          try {
            monitor.wait();
          } catch (InterruptedException e) {
            e.printStackTrace();
          }
        }
      }
    }
  }
}
