package redij.util;

public class Buffer {

   private static final int INITIAL_BUFFER_SIZE = 1024;
   public byte[] bData;
   public char[] cData;

   public Buffer() {
      bData = new byte[INITIAL_BUFFER_SIZE];
      cData = new char[INITIAL_BUFFER_SIZE];
   }

   public void expandB() {
      byte[] newData = new byte[(INITIAL_BUFFER_SIZE + bData.length) * 2];
      System.arraycopy(bData, 0, newData, 0, bData.length);
      bData = newData;
   }

   public void expandC() {
      char[] newData = new char[(INITIAL_BUFFER_SIZE + cData.length) * 2];
      System.arraycopy(cData, 0, newData, 0, cData.length);
      cData = newData;
   }

   public void clear() {
      bData = new byte[0];
      cData = new char[0];
   }
}
