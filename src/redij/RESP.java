package redij;

import redij.util.Buffer;
import java.io.IOException;
import redij.exception.ClientException;
import redij.exception.RedisException;
import redij.util.BulkStringInputStream;
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
            return in.readUtf8(buf);
         case TYPE_ERROR:
            throw new RedisException(in.readUtf8(buf));
         case TYPE_INTEGER:
            return in.readLong();
         case TYPE_BULK_STRING:
            return readBulkString(in);
         case TYPE_ARRAY:
            return readArray(in, buf);
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

   public static BulkStringInputStream readAsBulkString(RedisInputStream in, Buffer buf) throws IOException {
      Object response = read(in, buf);
      if (response == null) {
         return null;
      } else if (response instanceof BulkStringInputStream) {
         return (BulkStringInputStream) response;
      } else {
         throw new ClientException("Expected " + BulkStringInputStream.class.getSimpleName() + " but got " + response.getClass().getSimpleName());
      }
   }

   public static String readAsBulkStringString(RedisInputStream in, Buffer buf) throws IOException {
      BulkStringInputStream bsin = readAsBulkString(in, buf);
      if (bsin == null) {
         return null;
      } else {
         String string = bsin.readUtf8(buf);
         bsin.close();
         return string;
      }
   }

   public static Object[] readAsArray(RedisInputStream in, Buffer buf) throws IOException {
      Object response = read(in, buf);
      if (response == null) {
         return null;
      } else if (response instanceof Object[]) {
         return (Object[]) response;
      } else {
         throw new ClientException("Expected " + Object[].class.getSimpleName() + " but got " + response.getClass().getSimpleName());
      }
   }

   private static BulkStringInputStream readBulkString(RedisInputStream in) throws IOException {
      int length = (int) in.readLong();
      return length != -1 ? new BulkStringInputStream(in, length) : null;
   }

   private static Object[] readArray(RedisInputStream in, Buffer buf) throws IOException {
      int length = (int) in.readLong();
      if (length == -1) {
         return null;
      }
      Object[] array = new Object[length];
      for (int i = 0; i < array.length; i++) {
         array[i] = read(in, buf);
         if (array[i] instanceof BulkStringInputStream) {
            try (BulkStringInputStream bsin = (BulkStringInputStream) array[i]) {
               array[i] = bsin.readUtf8(buf);
            }
         }
      }
      return array;
   }
}
