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
	        	//String command = "Rscript RCode_Worker.R \"" + message+ "\"";
	        	
	            try{
	                Process p = Runtime.getRuntime().exec("/usr/bin/Rscript /home/cem/worker/RCode_Worker.R ''<ShinnyParameters><parameter><name>servercnt</name><value>3</value></parameter><parameter><name>marketInterest</name><value>INVESTING</value></parameter><parameter><name>perceivedValue</name><value>40</value></parameter><parameter><name>costtoDeliver</name><value>10</value></parameter><parameter><name>runnumber</name><value>20161031131350</value></parameter><parameter><name>runtime</name><value>1000</value></parameter><parameter><name>msisdn</name><value>67110,169215,172683,173382,176704,176849,181783,183256,189459,191313</value></parameter></ShinnyParameters>'");

	                int processComplete = p.waitFor();

	                   if (processComplete == 0) {
	                        System.out.println("successfull");
	                   } else {
	                        System.out.println("Could not complete");
	                   }
	                }
	                catch (Exception e)
	                {
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
