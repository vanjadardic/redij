package redij;

import redij.util.Buffer;
import java.io.IOException;
import java.net.Socket;
import redij.exception.ClientException;
import redij.util.RedisInputStream;
import redij.util.RedisOutputStream;

public class Node {

   private final String host;
   private final int port;
   private final Buffer buf;
   private Socket socket;
   private RedisOutputStream out;
   private RedisInputStream in;
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

   public Node(String host, int port) {
      this.host = host;
      this.port = port;
      buf = new Buffer();
   }

   public void openConnection() throws IOException {
      socket = new Socket(host, port);
      out = new RedisOutputStream(socket.getOutputStream(), 8 * 1024);
      in = new RedisInputStream(socket.getInputStream(), 32 * 1024);
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
      out.write(PING0);
      out.flush();
      return RESP.readAsSimpleString(in, buf);
   }

   public String PING(String arg) throws IOException {
      out.write(PING1);
      writeBulkString(out, arg);
      out.flush();
      return RESP.readAsBulkStringString(in, buf);
   }

   public Long INCR(String key) throws IOException {
      out.write(INCR);
      writeBulkString(out, key);
      out.flush();
      return RESP.readAsInteger(in, buf);
   }

   public Long HSET(String key, String field, String value) throws IOException {
      out.write(HSET);
      writeBulkString(out, key);
      writeBulkStringPrefix(out, field);
      writeBulkStringPrefix(out, value);
      out.flush();
      return RESP.readAsInteger(in, buf);
   }

   public Object[] HGETALL(String key) throws IOException {
      out.write(HGETALL);
      writeBulkString(out, key);
      out.flush();
      return RESP.readAsArray(in, buf);
   }

   public Object[] HMGET(String key, String... fields) throws IOException {
      if (fields.length == 0) {
         throw new ClientException("At least one fields must be specified");
      }
      out.write(ARRAY_PREFIX);
      out.writeInt(fields.length + 2);
      out.write(HMGET);
      writeBulkString(out, key);
      for (String field : fields) {
         writeBulkStringPrefix(out, field);
      }
      out.flush();
      return RESP.readAsArray(in, buf);
   }

   public String INFO() throws IOException {
      out.write(INFO0);
      out.flush();
      return RESP.readAsBulkStringString(in, buf);
   }

   public String INFO(String section) throws IOException {
      out.write(INFO1);
      writeBulkString(out, section);
      out.flush();
      return RESP.readAsBulkStringString(in, buf);
   }
}
