import java.net.InetAddress;

//javac GetJob.java -Xlint:unchecked

class GetJob implements Runnable {
	   private Thread t;
	   private String threadName;
	   
	   GetJob( String name, int myvalue) {
	      threadName = name;
	   }
	   
	   public void run() {
	      System.out.println("Running " +  threadName );
	      try {
	            if (threadName == "T1" )
	            {
	            	System.out.println("Calling the exchange.");
	            	ReceiveWorkServer.CheckWorker();		            	
	            }
	            if (threadName == "T2" )
	            {	
	            	System.out.println("Calling MSISDN.");
	            	ReadMSISDN.Read();	            
	            }
	            if (threadName == "T3" )
	            {
	            	System.out.println("Passing number of servers.");
	            	while (true) //Loop indefinetly
	            	{	            			            		
	            		ReadMSISDN.setNum(ReceiveWorkServer.getNum());
	            		   InetAddress addr = java.net.InetAddress.getLocalHost();
	       	               String hostname = addr.getHostName();
	       	            if (hostname.equals("HWControl"))
	    	            	hostname = "HWWorker1";
	            		 //System.out.println(" [x] Worker server number '" + ReceiveWorkServer.getNum() + "'");
	            		 //System.out.println(" [x] Worker server number '" +Integer.parseInt(hostname.replace("HWWorker", ""))+ "'");
	            		Thread.sleep(2000);	   //Sleep for two seconds 
	            	}
	            }	            

	      }catch (InterruptedException e) {
	         //System.out.println("Thread " +  threadName + " interrupted.");
	      } catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	      
	   }
	   
	   public void start () {
	      //System.out.println("Starting " +  threadName );
	      if (t == null) {
	         t = new Thread (this, threadName);
	         t.start ();
	      }
	   }
	   
	   
	}

