import com.rabbitmq.client.*;
import java.io.IOException;
import java.util.concurrent.TimeoutException;
import java.net.InetAddress;
import java.net.UnknownHostException;


public class ReadMSISDN {

	  private static final String TASK_QUEUE_NAME = "workermsisdn";
	  private static int ServerNumber = 0;
	  private static int hostnamenumber = 0;

	  public static void setNum(int CurrentServerNumber) {
		    ServerNumber = CurrentServerNumber;
	  }

	  public static void getServerName() {
		  try {
	            InetAddress addr = java.net.InetAddress.getLocalHost();
	            String hostname = addr.getHostName();
	            hostnamenumber = Integer.parseInt(hostname.replace("HWWorker", ""));
	        } catch (UnknownHostException e) {
	            System.out.println(e);
	        }
	  }
	  
	  public static void Read() throws Exception {
		//Create the connection 
	    ConnectionFactory factory = new ConnectionFactory();
		factory.setHost("hwlinux.cloudapp.net");
		factory.setUsername("controller");
		factory.setPassword("KaraburunCe2");

	    final Connection connection = factory.newConnection();
	    final Channel channel = connection.createChannel();

	    channel.queueDeclare(TASK_QUEUE_NAME, false, false, false, null);
	    
	    //Set the number of message to grab. 
	    channel.basicQos(1);

	    final Consumer consumer = new DefaultConsumer(channel) {
	      @Override
	      public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
	        String message = new String(body, "UTF-8");

	        if (hostnamenumber > ServerNumber)
	        {	        		        	
	        	channel.basicNack(envelope.getDeliveryTag(), false, true);
	        	while(true)
	        	{
	        		wantSleep();
	        		System.out.println("Server number is " + ServerNumber + ", rejecting the message.");
	        		if (hostnamenumber <= ServerNumber)
	        			break;
	        	}
	        } else
	        {
	        	System.out.println(" [x] Received '" + message + "'");	   
	        	final Runtime rt = Runtime.getRuntime();
	        	String command = "Rscript RCode_Worker.R \"" + message+ "\"";
	        	System.out.println(command);
	        	
	        	Process myp = rt.exec(command);	
	        	try {
					myp.waitFor();
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
	        	
	        	channel.basicAck(envelope.getDeliveryTag(), false);
	        	//wantSleep();
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
