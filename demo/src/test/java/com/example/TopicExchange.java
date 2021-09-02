package com.example;
import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;

import java.io.IOException;
import java.util.concurrent.TimeoutException;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
  // Declare a Topic Exchange with the name my-topic-exchange.
   
  public class TopicExchange {

    

    public static void declareExchange() throws IOException, TimeoutException {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        try (Connection connection = factory.newConnection();
             Channel channel = connection.createChannel()) {
                channel.exchangeDeclare("my-topic-exchange", BuiltinExchangeType.TOPIC, true);
                
                       }
                    }


  /**
   * Declare Queues to receive respective interested messages.
   *
   * @throws IOException
   * @throws TimeoutException
   */
  public static void declareQueues() throws IOException, TimeoutException {
    //Create a channel - do not share the Channel instance
    ConnectionFactory factory = new ConnectionFactory();
    factory.setHost("localhost");
    try (Connection connection = factory.newConnection();
         Channel channel = connection.createChannel()) {
    

    //Create the Queues
    channel.queueDeclare("NormalQ", true, false, false, null);
    channel.queueDeclare("WarningQ", true, false, false, null);
    

   // channel.close();
}
  }

  /*
   * Declare Bindings - register interests using routing key patterns.
   *
   * @throws IOException
   * @throws TimeoutException
   */
  public static void declareBindings() throws IOException, TimeoutException {
    ConnectionFactory factory = new ConnectionFactory();
    factory.setHost("localhost");
    try (Connection connection = factory.newConnection();
         Channel channel = connection.createChannel()) {
    //Create bindings - (queue, exchange, routingKey) - routingKey != null

channel.queueBind("NormalQ", "my-topic-exchange", "normal.*");
channel.queueBind("WarningQ", "my-topic-exchange", "#.warning.*");
}
  }

  /*
   * Assign Consumers to each of the Queue.
   *
   * @throws IOException
   * @throws TimeoutException
   */
  public static void subscribeMessage() throws IOException, TimeoutException {
    ConnectionFactory factory = new ConnectionFactory();
    factory.setHost("localhost");
    try (Connection connection = factory.newConnection();
         Channel channel = connection.createChannel()) {
    channel.basicConsume("NormalQ", true, ((consumerTag, message) -> {
      System.out.println("\n\n=========== NormalQueue ==========");
      System.out.println(consumerTag);
      System.out.println("NormalQ: " + new String(message.getBody()));
      System.out.println(message.getEnvelope());
    }), consumerTag -> {
      System.out.println(consumerTag);
    });

    channel.basicConsume("WarningQ", true, ((consumerTag, message) -> {
      System.out.println("\n\n ============ Warning Queue ==========");
      System.out.println(consumerTag);
      System.out.println("WarningQ: " + new String(message.getBody()));
      System.out.println(message.getEnvelope());
    }), consumerTag -> {
      System.out.println(consumerTag);
    });

}
  }

  /*
   * Publish Messages with different routing keys.
   *
   * @throws IOException
   * @throws TimeoutException
   */
  public static void publishMessage() throws IOException, TimeoutException {
    ConnectionFactory factory = new ConnectionFactory();
    factory.setHost("localhost");
    try (Connection connection = factory.newConnection();
         Channel channel = connection.createChannel()) {
    String message = "{client_id:1,temperature:35°c,heartrate:145bpm,oxygen_saturation:98%}";

    channel.basicPublish("my-topic-exchange", "normal.health", null, message.getBytes());

     message ="{client_id:3,alertid:1,temperature:37°c}" ;
    channel.basicPublish("my-topic-exchange", "warning.aware", null, message.getBytes());

    
}
  }

  /*
   * Execute the methods.
   *
   * @param args
   * @throws IOException
   * @throws TimeoutException
   */
  public static void main(String[] args) throws IOException, TimeoutException {
    TopicExchange.declareExchange();
    TopicExchange.declareQueues();
    TopicExchange.declareBindings();

    Thread subscribe = new Thread(() -> {
      try {
        TopicExchange.subscribeMessage();
      } catch (IOException | TimeoutException e) {
        e.printStackTrace();
      }
    });

    Thread publish = new Thread(() -> {
      try {
        TopicExchange.publishMessage();
      } catch (IOException | TimeoutException e) {
        e.printStackTrace();
      }
    });
    subscribe.start();
    publish.start();
  }
}
