package redij.util;

public class Buffer {

   public static int INITIAL_BUFFER_SIZE = 1024;
   public static int EXPAND_FACTOR = 2;
   public byte[] bData;
   public char[] cData;

   public Buffer(byte[] buffer) {

   }

   public Buffer(int size) {
      bData = new byte[size];
      cData = new char[size];
   }

   public Buffer() {
      this(INITIAL_BUFFER_SIZE);
   }

   public void expandB(int factor) {
      byte[] newData = new byte[bData.length * factor];
      System.arraycopy(bData, 0, newData, 0, bData.length);
      bData = newData;
   }

   public void expandB() {
      expandB(EXPAND_FACTOR);
   }

   public void expandC(int factor) {
      char[] newData = new char[cData.length * factor];
      System.arraycopy(cData, 0, newData, 0, cData.length);
      cData = newData;
   }

   public void expandC() {
      expandC(EXPAND_FACTOR);
   }
}
