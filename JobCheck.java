public class JobCheck {

   public static int myvalue = 0;
   public static void main(String args[]) {
	  GetJob R1 = new GetJob( "T1",myvalue);
      R1.start();
      
      GetJob R2 = new GetJob( "T2",myvalue);
      R2.start();
      
      GetJob R3 = new GetJob( "T3",myvalue);
      R3.start();
   }   
}