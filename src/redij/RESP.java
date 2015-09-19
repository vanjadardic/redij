package redij;

import redij.util.Buffer;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import redij.exception.ClientException;
import redij.exception.RedisException;

public class RESP {

   public static Charset DEFAULT_CHARSET = Charset.defaultCharset();
   private static final int TYPE_SIMPLE_STRING = '+';
   private static final int TYPE_ERROR = '-';
   private static final int TYPE_INTEGER = ':';
   private static final int TYPE_BULK_STRING = '$';
   private static final int TYPE_ARRAY = '*';

   public static Object read(InputStream in, Buffer buf) throws IOException {
      int type = in.read();
      switch (type) {
         case TYPE_SIMPLE_STRING:
            return readSimpleString(in, buf);
         case TYPE_ERROR:
            throw new RedisException(readSimpleString(in, buf));
         case TYPE_INTEGER:
            return readInteger(in, buf);
         case TYPE_BULK_STRING:
            return readBulkString(in, buf);
         case TYPE_ARRAY:
            break;
      }
      throw new ClientException("Unexpected response type: " + (char) type);
   }

   public static String readAsSimpleString(InputStream in, Buffer buf) throws IOException {
      Object response = read(in, buf);
      if (response == null) {
         return null;
      } else if (response instanceof String) {
         return (String) response;
      } else {
         throw new ClientException("Expected " + String.class.getSimpleName() + " but got " + response.getClass().getSimpleName());
      }
   }

   public static Long readAsInteger(InputStream in, Buffer buf) throws IOException {
      Object response = read(in, buf);
      if (response == null) {
         return null;
      } else if (response instanceof Long) {
         return (Long) response;
      } else {
         throw new ClientException("Expected " + Long.class.getSimpleName() + " but got " + response.getClass().getSimpleName());
      }
   }

   public static byte[] readAsBulkString(InputStream in, Buffer buf) throws IOException {
      Object response = read(in, buf);
      if (response == null) {
         return null;
      } else if (response instanceof byte[]) {
         return (byte[]) response;
      } else {
         throw new ClientException("Expected " + byte[].class.getSimpleName() + " but got " + response.getClass().getSimpleName());
      }
   }

   private static String readSimpleString(InputStream in, Buffer buf) throws IOException {
      int pos = 0;
      do {
         pos += in.read(buf.data, pos, buf.data.length - pos);
         if (pos >= 2) {
            if (buf.data[pos - 2] == '\r' && buf.data[pos - 1] == '\n') {
               break;
            } else if (buf.data.length == pos) {
               buf.expand();
            }
         }
      } while (true);
      return new String(buf.data, 0, pos - 2, DEFAULT_CHARSET);
   }

   private static Long readInteger(InputStream in, Buffer buf) throws IOException {
      return Long.valueOf(readSimpleString(in, buf), 10);
   }

   private static byte[] readBulkString(InputStream in, Buffer buf) throws IOException {
      int length = readInteger(in, buf).intValue();
      if (length != -1) {
         byte[] data = new byte[length];
         int pos = 0;
         while (pos < length) {
            pos += in.read(data, pos, length - pos);
         }
         readSimpleString(in, buf);
         return data;
      } else {
         return null;
      }
   }
}
