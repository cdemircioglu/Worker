import com.rabbitmq.client.*;

import java.io.IOException;

public class ReceiveWorkServer {
	//Exchange name
	private static final String EXCHANGE_NAME = "worknumber";
	private static int ServerNumber = 0;
	
	//Main method to get the number of workers
	public static void CheckWorker() throws Exception {
	    ConnectionFactory factory = new ConnectionFactory();
		factory.setHost("hwcontrol.cloudapp.net");
		factory.setUsername("controller");
		factory.setPassword("KaraburunCe2");
	    Connection connection = factory.newConnection();
	    Channel channel = connection.createChannel();

	    channel.exchangeDeclare(EXCHANGE_NAME, "fanout");
	    String queueName = channel.queueDeclare().getQueue();
	    channel.queueBind(queueName, EXCHANGE_NAME, "");


	    Consumer consumer = new DefaultConsumer(channel) {
	      @Override
	      public void handleDelivery(String consumerTag, Envelope envelope,
	                                 AMQP.BasicProperties properties, byte[] body) throws IOException {
	        String message = new String(body, "UTF-8");
	        System.out.println(" [x] Received '" + message + "'");
	        ServerNumber = Integer.parseInt(message);
	      }
	    };
	    channel.basicConsume(queueName, true, consumer);
	}
	public static int getNum() {
	    return ServerNumber;
	}
	
	
	
}
