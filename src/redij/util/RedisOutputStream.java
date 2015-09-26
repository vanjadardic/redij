package redij.util;

import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.OutputStream;

public class RedisOutputStream extends FilterOutputStream {

   private byte buf[];
   private int count;

   public RedisOutputStream(OutputStream out, int size) {
      super(out);
      if (size <= 0) {
         throw new IllegalArgumentException("Buffer size <= 0");
      }
      buf = new byte[size];
      count = 0;
   }

   private void flushBuffer() throws IOException {
      if (count > 0) {
         out.write(buf, 0, count);
         count = 0;
      }
   }

   @Override
   public void write(int b) throws IOException {
      if (count >= buf.length) {
         flushBuffer();
      }
      buf[count++] = (byte) b;
   }

   @Override
   public void write(byte b[], int off, int len) throws IOException {
      if (len >= buf.length) {
         flushBuffer();
         out.write(b, off, len);
         return;
      }
      if (len > buf.length - count) {
         flushBuffer();
      }
      System.arraycopy(b, off, buf, count, len);
      count += len;
   }

   @Override
   public void flush() throws IOException {
      flushBuffer();
      out.flush();
   }

   @Override
   public void close() throws IOException {
      super.close();
      buf = null;
      count = 0;
   }

   private static final int[] SIZE_TABLE = {9, 99, 999, 9999, 99999, 999999, 9999999, 99999999, 999999999, Integer.MAX_VALUE};
   private static final byte[] DIGIT_TENS = {
      '0', '0', '0', '0', '0', '0', '0', '0', '0', '0',
      '1', '1', '1', '1', '1', '1', '1', '1', '1', '1',
      '2', '2', '2', '2', '2', '2', '2', '2', '2', '2',
      '3', '3', '3', '3', '3', '3', '3', '3', '3', '3',
      '4', '4', '4', '4', '4', '4', '4', '4', '4', '4',
      '5', '5', '5', '5', '5', '5', '5', '5', '5', '5',
      '6', '6', '6', '6', '6', '6', '6', '6', '6', '6',
      '7', '7', '7', '7', '7', '7', '7', '7', '7', '7',
      '8', '8', '8', '8', '8', '8', '8', '8', '8', '8',
      '9', '9', '9', '9', '9', '9', '9', '9', '9', '9'
   };
   private static final byte[] DIGIT_ONES = {
      '0', '1', '2', '3', '4', '5', '6', '7', '8', '9',
      '0', '1', '2', '3', '4', '5', '6', '7', '8', '9',
      '0', '1', '2', '3', '4', '5', '6', '7', '8', '9',
      '0', '1', '2', '3', '4', '5', '6', '7', '8', '9',
      '0', '1', '2', '3', '4', '5', '6', '7', '8', '9',
      '0', '1', '2', '3', '4', '5', '6', '7', '8', '9',
      '0', '1', '2', '3', '4', '5', '6', '7', '8', '9',
      '0', '1', '2', '3', '4', '5', '6', '7', '8', '9',
      '0', '1', '2', '3', '4', '5', '6', '7', '8', '9',
      '0', '1', '2', '3', '4', '5', '6', '7', '8', '9'
   };

   public void writeInt(int i) throws IOException {
      if (i < 0) {
         write('-');
         i = -i;
      }

      int size = 0;
      while (i > SIZE_TABLE[size]) {
         size++;
      }
      size++;

      if (size >= buf.length - count) {
         flushBuffer();
      }

      int q, r;
      int charPos = count + size;
      // Generate two digits per iteration
      while (i >= 65536) {
         q = i / 100;
         // really: r = i - (q * 100);
         r = i - ((q << 6) + (q << 5) + (q << 2));
         i = q;
         buf[--charPos] = DIGIT_ONES[r];
         buf[--charPos] = DIGIT_TENS[r];
      }

      // Fall through to fast mode for smaller numbers
      while (true) {
         q = (i * 52429) >>> (16 + 3);
         r = i - ((q << 3) + (q << 1)); // r = i - (q * 10) ...
         buf[--charPos] = DIGIT_ONES[r];
         i = q;
         if (i == 0) {
            break;
         }
      }
      count += size;
   }

   private static boolean isSurrogate(char ch) {
      return ch >= Character.MIN_SURROGATE && ch <= Character.MAX_SURROGATE;
   }

   public void writeUtf8Length(String string) throws IOException {
      int utf8Len = 0;
      for (int i = 0, len = string.length(); i < len; i++) {
         char c = string.charAt(i);
         if (c < 0x80) {
            utf8Len++;
         } else if (c < 0x800) {
            utf8Len += 2;
         } else if (!isSurrogate(c)) {
            utf8Len += 3;
         } else {
            utf8Len += 4;
            i++;
         }
      }
      writeInt(utf8Len);
   }

   public void writeUtf8(String string) throws IOException {
      for (int i = 0, len = string.length(); i < len; i++) {
         char c = string.charAt(i);
         if (c < 0x80) {
            if (count == buf.length) {
               flushBuffer();
            }
            buf[count++] = (byte) c;
         } else if (c < 0x800) {
            if (buf.length - count < 2) {
               flushBuffer();
            }
            buf[count++] = (byte) (0xc0 | (c >> 6));
            buf[count++] = (byte) (0x80 | (c & 0x3f));
         } else if (!isSurrogate(c)) {
            if (buf.length - count < 3) {
               flushBuffer();
            }
            buf[count++] = ((byte) (0xe0 | ((c >> 12))));
            buf[count++] = ((byte) (0x80 | ((c >> 6) & 0x3f)));
            buf[count++] = ((byte) (0x80 | (c & 0x3f)));
         } else {
            if (buf.length - count < 4) {
               flushBuffer();
            }
            int uc = Character.toCodePoint(c, string.charAt(i++));
            buf[count++] = ((byte) (0xf0 | ((uc >> 18))));
            buf[count++] = ((byte) (0x80 | ((uc >> 12) & 0x3f)));
            buf[count++] = ((byte) (0x80 | ((uc >> 6) & 0x3f)));
            buf[count++] = ((byte) (0x80 | (uc & 0x3f)));
         }
      }
   }
}
