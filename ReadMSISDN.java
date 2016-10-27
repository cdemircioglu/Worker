import com.rabbitmq.client.*;
import java.io.IOException;
import java.util.concurrent.TimeoutException;


public class ReadMSISDN {

	  private static final String TASK_QUEUE_NAME = "workermsisdn";
	  private static int ServerNumber = 0;

	  public static void setNum(int CurrentServerNumber) {
		    ServerNumber = CurrentServerNumber;
	  }

	  
	  public static void Read() throws Exception {
		//Create the connection 
	    ConnectionFactory factory = new ConnectionFactory();
	    factory.setHost("localhost");
	    final Connection connection = factory.newConnection();
	    final Channel channel = connection.createChannel();

	    channel.queueDeclare(TASK_QUEUE_NAME, false, false, false, null);
	    
	    //Set the number of message to grab. 
	    channel.basicQos(1);

	    final Consumer consumer = new DefaultConsumer(channel) {
	      @Override
	      public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
	        String message = new String(body, "UTF-8");

	        if (ServerNumber != 10)
	        {	        		        	
	        	channel.basicNack(envelope.getDeliveryTag(), false, true);
	        	while(true)
	        	{
	        		wantSleep();
	        		System.out.println("Server number is " + ServerNumber + " ,rejecting the message.");
	        		if (ServerNumber == 10)
	        			break;
	        	}
	        } else
	        {
	        	System.out.println(" [x] Received '" + message + "'");	       
	        	channel.basicAck(envelope.getDeliveryTag(), false);
	        	wantSleep();
	        }
	      }

		public void wantSleep() {
			try {
				Thread.sleep(5000);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	    };
	    
	    channel.basicConsume(TASK_QUEUE_NAME, false, consumer);
	    
	    
	  }


	
	
}
