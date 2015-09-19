package redij.util;

import java.util.Arrays;

public class Buffer {

   public static int INITIAL_BUFFER_SIZE = 32;
   public byte[] data;

   public Buffer(byte[] buffer) {
      this.data = buffer;
   }

   public Buffer(int size) {
      this(new byte[size]);
   }

   public Buffer() {
      this(INITIAL_BUFFER_SIZE);
   }

   public void expand(boolean keepData, int factor) {
      if (keepData) {
         byte[] newBuffer = new byte[data.length * factor];
         System.arraycopy(data, 0, newBuffer, 0, data.length);
         data = newBuffer;
      } else {
         data = new byte[data.length * factor];
      }
      data = Arrays.copyOf(data, data.length * factor);
   }

   public void expand() {
      expand(true, 2);
   }

   public void expand(boolean keepData) {
      expand(keepData, 2);
   }

   public void expand(int factor) {
      expand(true, factor);
   }
}
