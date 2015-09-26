package redij;

import redij.util.Buffer;
import java.io.IOException;
import redij.exception.ClientException;
import redij.util.RedisOutputStream;

public class Node {

   public final NodeConnection con;
   public final Buffer buf;
   private static final byte[] CRLF = "\r\n".getBytes();
   private static final byte[] BULK_STRING_PREFIX = "$".getBytes();
   private static final byte[] ARRAY_PREFIX = "*".getBytes();
   private static final byte[] PING0 = createCommand("PING", 0);
   private static final byte[] PING1 = createCommand("PING", 1);
   private static final byte[] INCR = createCommand("INCR", 1);
   private static final byte[] HSET = createCommand("HSET", 3);
   private static final byte[] HGETALL = createCommand("HGETALL", 1);
   private static final byte[] HMGET = createCommand("HMGET", -1);
   private static final byte[] INFO0 = createCommand("INFO", 0);
   private static final byte[] INFO1 = createCommand("INFO", 1);

   public Node(NodeConnection connection) {
      con = connection;
      buf = new Buffer();
   }

   private static byte[] createCommand(String command, int numArguments) {
      String tmp = "";
      if (numArguments >= 0) {
         tmp += "*" + (numArguments + 1);
      }
      tmp += "\r\n$" + command.length() + "\r\n" + command + "\r\n";
      if (numArguments > 0 || numArguments == -1) {
         tmp += "$";
      }
      return tmp.getBytes();
   }

   private static void writeBulkString(RedisOutputStream out, String string) throws IOException {
      out.writeUtf8Length(string);
      out.write(CRLF);
      out.writeUtf8(string);
      out.write(CRLF);
   }

   private static void writeBulkStringPrefix(RedisOutputStream out, String string) throws IOException {
      out.write(BULK_STRING_PREFIX);
      out.writeUtf8Length(string);
      out.write(CRLF);
      out.writeUtf8(string);
      out.write(CRLF);
   }

   public String PING() throws IOException {
      con.out.write(PING0);
      con.out.flush();
      return RESP.readAsSimpleString(con.in, buf);
   }

   public String PING(String arg) throws IOException {
      con.out.write(PING1);
      writeBulkString(con.out, arg);
      con.out.flush();
      return RESP.readAsBulkStringString(con.in, buf);
   }

   public Long INCR(String key) throws IOException {
      con.out.write(INCR);
      writeBulkString(con.out, key);
      con.out.flush();
      return RESP.readAsInteger(con.in, buf);
   }

   public Long HSET(String key, String field, String value) throws IOException {
      con.out.write(HSET);
      writeBulkString(con.out, key);
      writeBulkStringPrefix(con.out, field);
      writeBulkStringPrefix(con.out, value);
      con.out.flush();
      return RESP.readAsInteger(con.in, buf);
   }

   public Object[] HGETALL(String key) throws IOException {
      con.out.write(HGETALL);
      writeBulkString(con.out, key);
      con.out.flush();
      return RESP.readAsArray(con.in, buf);
   }

   public Object[] HMGET(String key, String... fields) throws IOException {
      if (fields.length == 0) {
         throw new ClientException("At least one fields must be specified");
      }
      con.out.write(ARRAY_PREFIX);
      con.out.writeInt(fields.length + 2);
      con.out.write(HMGET);
      writeBulkString(con.out, key);
      for (String field : fields) {
         writeBulkStringPrefix(con.out, field);
      }
      con.out.flush();
      return RESP.readAsArray(con.in, buf);
   }

   public String INFO() throws IOException {
      con.out.write(INFO0);
      con.out.flush();
      return RESP.readAsBulkStringString(con.in, buf);
   }

   public String INFO(String section) throws IOException {
      con.out.write(INFO1);
      writeBulkString(con.out, section);
      con.out.flush();
      return RESP.readAsBulkStringString(con.in, buf);
   }
}
