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
	            		Thread.sleep(2000);	   //Sleep for two seconds 
	            	}
	            }	            

	      }catch (InterruptedException e) {
	         //System.out.println("Thread " +  threadName + " interrupted.");
	      } catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	      System.out.println("Thread " +  threadName + " exiting.");
	   }
	   
	   public void start () {
	      //System.out.println("Starting " +  threadName );
	      if (t == null) {
	         t = new Thread (this, threadName);
	         t.start ();
	      }
	   }
	   
	   
	}

