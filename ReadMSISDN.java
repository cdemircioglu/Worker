import com.rabbitmq.client.*;

import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
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
	  
	  public static void writeFile3(String cmd) throws IOException {
          PrintWriter pw = new PrintWriter(new FileWriter("RCode_WorkerCurrent.R"));
                  pw.write(cmd);
          pw.close();
	  }

	  public static String readFile(String filename) throws IOException
      {
          String content = null;
          File file = new File(filename); //for ex foo.txt
          FileReader reader = null;
          try {
              reader = new FileReader(file);
              char[] chars = new char[(int) file.length()];
              reader.read(chars);
              content = new String(chars);
              reader.close();
          } catch (IOException e) {
              e.printStackTrace();
          } finally {
              if(reader !=null){reader.close();}
          }
          return content;
      }

	  
	  public static void Read() throws Exception {
		//Read R Template
		String template = readFile("RCode_Worker.R");
		
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
	        getServerName();
	        
	        if (hostnamenumber > ServerNumber)
	        {	        		        	
	        	channel.basicNack(envelope.getDeliveryTag(), false, true);
	        	while(true)
	        	{
	        		wantSleep();
	        		getServerName();
	        		System.out.println("Server number is " + ServerNumber + ", rejecting the message.");
	        		if (hostnamenumber <= ServerNumber)
	        			break;
	        	}
	        } else
	        {
	        	System.out.println(" [x] Received '" + message + "'");	   
	        	final Runtime rt = Runtime.getRuntime();	        	
	        	writeFile3(template.replace("RRRR",message)); //Write to a new file
	        	
	        	try{    
	                Process p = Runtime.getRuntime().exec("Rscript RCode_WorkerCurrent.R");

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
