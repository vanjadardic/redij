package redij.util;

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import redij.exception.ClientException;

public class BulkStringInputStream extends FilterInputStream {

   private int remaining;

   public BulkStringInputStream(InputStream in, int size) {
      super(in);
      if (size < 0) {
         throw new IllegalArgumentException("Stream size < 0");
      }
      this.remaining = size;
   }

   @Override
   public int read() throws IOException {
      int b = -1;
      if (remaining > 0) {
         b = in.read();
         if (b == -1) {
            throw new ClientException("Unexpected end of stream");
         }
         remaining--;
      }
      if (remaining == 0) {
         in.skip(2);
         remaining = -1;
      }
      return b;
   }

   @Override
   public int read(byte b[], int off, int len) throws IOException {
      int readn = -1;
      if (remaining > 0) {
         readn = in.read(b, off, Math.min(len, remaining));
         if (readn == -1) {
            throw new ClientException("Unexpected end of stream");
         }
         remaining -= readn;
      }
      if (remaining == 0) {
         in.skip(2);
         remaining = -1;
      }
      return readn;
   }

   @Override
   public long skip(long n) throws IOException {
      long skipped = 0;
      if (remaining > 0) {
         skipped = in.skip(Math.min(n, remaining));
         remaining -= skipped;
      }
      if (remaining == 0) {
         in.skip(2);
         remaining = -1;
      }
      return skipped;
   }

   public long skipAll() throws IOException {
      long remainingTmp = 0;
      if (remaining > 0) {
         remainingTmp = remaining;
         while (remaining > 0) {
            remaining -= in.skip(remaining);
         }
      }
      if (remaining == 0) {
         in.skip(2);
         remaining = -1;
      }
      return remainingTmp;
   }

   @Override
   public int available() throws IOException {
      int available = 0;
      if (remaining > 0) {
         available = Math.min(in.available(), remaining);
      }
      if (remaining == 0) {
         in.skip(2);
         remaining = -1;
      }
      return available;
   }

   @Override
   public void close() throws IOException {
      skipAll();
   }

   @Override
   public boolean markSupported() {
      return false;
   }

   public String readUtf8(Buffer buf) throws IOException {
      int pos = 0;
      while (true) {
         int b1 = read();
         if (b1 == -1) {
            break;
         } else {
            if ((b1 & 0x80) == 0x00) {
               if (pos == buf.cData.length) {
                  buf.expandC();
               }
               buf.cData[pos++] = (char) b1;
            } else if ((b1 & 0xe0) == 0xc0) {
               if (pos == buf.cData.length) {
                  buf.expandC();
               }
               int b2 = read() & 0x3f;
               buf.cData[pos++] = (char) (((b1 << 6) ^ b2) ^ 0x3000);
            } else if ((b1 & 0xf0) == 0xe0) {
               if (pos == buf.cData.length) {
                  buf.expandC();
               }
               int b2 = read() & 0x3f;
               int b3 = read() & 0x3f;
               buf.cData[pos++] = (char) (((b1 << 12) ^ (b2 << 6) ^ b3) ^ 0xe0000);
            } else if ((b1 & 0xf8) == 0xf0) {
               if (buf.cData.length - pos < 2) {
                  buf.expandC();
               }
               int b2 = read() & 0x3f;
               int b3 = read() & 0x3f;
               int b4 = read() & 0x3f;
               int uc = (b1 << 18) | (b2 << 12) | (b3 << 06) | b4;
               buf.cData[pos++] = Character.highSurrogate(uc);
               buf.cData[pos++] = Character.lowSurrogate(uc);
            }
         }
      }
      return new String(buf.cData, 0, pos);
   }
}
