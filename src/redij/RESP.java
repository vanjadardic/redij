package redij;

import redij.util.Buffer;
import java.io.IOException;
import redij.exception.ClientException;
import redij.exception.RedisException;
import redij.util.RedisInputStream;

public class RESP {

   private static final int TYPE_SIMPLE_STRING = '+';
   private static final int TYPE_ERROR = '-';
   private static final int TYPE_INTEGER = ':';
   private static final int TYPE_BULK_STRING = '$';
   private static final int TYPE_ARRAY = '*';

   public static Object read(RedisInputStream in, Buffer buf) throws IOException {
      int type = in.read();
      switch (type) {
         case TYPE_SIMPLE_STRING:
            return readSimpleString(in, buf);
         case TYPE_ERROR:
            throw new RedisException(readSimpleString(in, buf));
         case TYPE_INTEGER:
            return readInteger(in);
         case TYPE_BULK_STRING:
            return readBulkString(in);
         case TYPE_ARRAY:
            break;
      }
      throw new ClientException("Unexpected response type: " + (char) type);
   }

   public static String readAsSimpleString(RedisInputStream in, Buffer buf) throws IOException {
      Object response = read(in, buf);
      if (response == null) {
         return null;
      } else if (response instanceof String) {
         return (String) response;
      } else {
         throw new ClientException("Expected " + String.class.getSimpleName() + " but got " + response.getClass().getSimpleName());
      }
   }

   public static Long readAsInteger(RedisInputStream in, Buffer buf) throws IOException {
      Object response = read(in, buf);
      if (response == null) {
         return null;
      } else if (response instanceof Long) {
         return (Long) response;
      } else {
         throw new ClientException("Expected " + Long.class.getSimpleName() + " but got " + response.getClass().getSimpleName());
      }
   }

   public static byte[] readAsBulkString(RedisInputStream in, Buffer buf) throws IOException {
      Object response = read(in, buf);
      if (response == null) {
         return null;
      } else if (response instanceof byte[]) {
         return (byte[]) response;
      } else {
         throw new ClientException("Expected " + byte[].class.getSimpleName() + " but got " + response.getClass().getSimpleName());
      }
   }

   private static String readSimpleString(RedisInputStream in, Buffer buf) throws IOException {
      return in.readUtf8(buf);
   }

   private static Long readInteger(RedisInputStream in) throws IOException {
      return in.readLong();
   }

   private static byte[] readBulkString(RedisInputStream in) throws IOException {
      int length = readInteger(in).intValue();
      if (length != -1) {
         byte[] data = new byte[length];
         int pos = 0;
         while (pos < length) {
            pos += in.read(data, pos, length - pos);
         }
         in.skip(2);
         return data;
      } else {
         return null;
      }
   }
}
