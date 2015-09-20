package redij.util;

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import redij.exception.ClientException;

public class RedisInputStream extends FilterInputStream {

   public static final int SKIP_BUFFER_SIZE = 2048;
   private byte buf[];
   private int count;
   private int pos;
   private byte skipBuf[];

   public RedisInputStream(InputStream in, int size) {
      super(in);
      if (size <= 0) {
         throw new IllegalArgumentException("Buffer size <= 0");
      }
      buf = new byte[size];
      count = 0;
      pos = 0;
      skipBuf = null;
   }

   private void fill() throws IOException {
      pos = 0;
      count = in.read(buf, 0, buf.length);
   }

   @Override
   public int read() throws IOException {
      if (pos >= count) {
         fill();
         if (pos >= count) {
            throw new ClientException("Unexpected end of stream");
         }
      }
      return buf[pos++] & 0xff;
   }

   private int read1(byte[] b, int off, int len) throws IOException {
      int avail = count - pos;
      if (avail <= 0) {
         if (len >= buf.length) {
            return in.read(b, off, len);
         }
         fill();
         avail = count - pos;
         if (avail <= 0) {
            throw new ClientException("Unexpected end of stream");
         }
      }
      int cnt = (avail < len) ? avail : len;
      System.arraycopy(buf, pos, b, off, cnt);
      pos += cnt;
      return cnt;
   }

   @Override
   public int read(byte b[], int off, int len) throws IOException {
      if (len == 0) {
         return 0;
      }
      int n = 0;
      while (true) {
         int nread = read1(b, off + n, len - n);
         if (nread == -1) {
            throw new ClientException("Unexpected end of stream");
         } else if (nread == 0) {
            return n;
         }
         n += nread;
         if (n == len) {
            return n;
         }
         if (in.available() == 0) {
            return n;
         }
      }
   }

   public long skip1(long n) throws IOException {
      if (skipBuf == null) {
         skipBuf = new byte[SKIP_BUFFER_SIZE];
      }
      long remaining = n;
      while (remaining > 0) {
         int readn = read(skipBuf, 0, (int) Math.min(SKIP_BUFFER_SIZE, remaining));
         if (readn < 0) {
            throw new ClientException("Unexpected end of stream");
         }
         remaining -= readn;
      }
      return n - remaining;
   }

   @Override
   public long skip(long n) throws IOException {
      if (n <= 0) {
         return 0;
      }
      long avail = count - pos;
      if (avail == 0) {
         return skip1(n);
      }
      long skipped = (avail < n) ? avail : n;
      pos += skipped;
      return skipped;
   }

   @Override
   public int available() throws IOException {
      int n = count - pos;
      int avail = in.available();
      return n > (Integer.MAX_VALUE - avail) ? Integer.MAX_VALUE : n + avail;
   }

   @Override
   public boolean markSupported() {
      return false;
   }

   public long readLong() throws IOException {
      int b = read();
      boolean isNegative = (b == '-');
      long value = (isNegative ? 0 : b);
      while (true) {
         b = read();
         if (b == '\r') {
            skip(1);
            break;
         } else {
            value = (value << 3) + (value << 1) + b - '0';
         }
      }
      return (isNegative ? -value : value);
   }

   public String readUtf8(Buffer buf) throws IOException {
      int pos1 = 0;
      while (true) {
         int b1 = read();
         if (b1 == '\r') {
            skip(1);
            break;
         } else {
            if ((b1 & 0x80) == 0x00) {
               if (pos1 == buf.cData.length) {
                  buf.expandC();
               }
               buf.cData[pos1++] = (char) b1;
            } else if ((b1 & 0xe0) == 0xc0) {
               if (pos1 == buf.cData.length) {
                  buf.expandC();
               }
               int b2 = read() & 0x3f;
               buf.cData[pos1++] = (char) (((b1 << 6) ^ b2) ^ 0x3000);
            } else if ((b1 & 0xf0) == 0xe0) {
               if (pos1 == buf.cData.length) {
                  buf.expandC();
               }
               int b2 = read() & 0x3f;
               int b3 = read() & 0x3f;
               buf.cData[pos1++] = (char) (((b1 << 12) ^ (b2 << 6) ^ b3) ^ 0xe0000);
            } else if ((b1 & 0xf8) == 0xf0) {
               if (buf.cData.length - pos1 < 2) {
                  buf.expandC();
               }
               int b2 = read() & 0x3f;
               int b3 = read() & 0x3f;
               int b4 = read() & 0x3f;
               int uc = (b1 << 18) | (b2 << 12) | (b3 << 06) | b4;
               buf.cData[pos1++] = Character.highSurrogate(uc);
               buf.cData[pos1++] = Character.lowSurrogate(uc);
            }
         }
      }
      return new String(buf.cData, 0, pos1);
   }
}
