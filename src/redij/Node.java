package redij;

import redij.util.Buffer;
import java.io.IOException;
import redij.util.RedisOutputStream;

public class Node {

   public final NodeConnection con;
   public final Buffer buf;
   public final NodePipeline pipe;
   public final NodeTransaction trans;
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
   private static final byte[] MULTI = createCommand("MULTI", 0);
   private static final byte[] WATCH = createCommand("WATCH", -1);
   private static final byte[] UNWATCH = createCommand("UNWATCH", 0);
   private static final byte[] GET = createCommand("GET", 1);

   public Node(NodeConnection connection) {
      con = connection;
      buf = new Buffer();
      pipe = new NodePipeline(this);
      trans = new NodeTransaction(this);
   }

   public static byte[] createCommand(String command, int numArguments) {
      String tmp = "";
      if (numArguments >= 0) {
         tmp += "*" + (numArguments + 1);
      }
      tmp += "\r\n$" + command.length() + "\r\n" + command + "\r\n";
      return tmp.getBytes();
   }

   private static void writeBulkString(RedisOutputStream out, String string) throws IOException {
      out.write(BULK_STRING_PREFIX);
      out.writeUtf8Length(string);
      out.write(CRLF);
      out.writeUtf8(string);
      out.write(CRLF);
   }

   protected void PINGreq() throws IOException {
      con.out.write(PING0);
   }

   public String PING() throws IOException {
      PINGreq();
      con.out.flush();
      return RESP.readAsSimpleString(con.in, buf);
   }

   protected void PINGreq(String arg) throws IOException {
      con.out.write(PING1);
      writeBulkString(con.out, arg);
   }

   public String PING(String arg) throws IOException {
      PINGreq(arg);
      con.out.flush();
      return RESP.readAsBulkStringString(con.in, buf);
   }

   protected void INCRreq(String key) throws IOException {
      con.out.write(INCR);
      writeBulkString(con.out, key);
   }

   public Long INCR(String key) throws IOException {
      INCRreq(key);
      con.out.flush();
      return RESP.readAsInteger(con.in, buf);
   }

   protected void HSETreq(String key, String field, String value) throws IOException {
      con.out.write(HSET);
      writeBulkString(con.out, key);
      writeBulkString(con.out, field);
      writeBulkString(con.out, value);
   }

   public Long HSET(String key, String field, String value) throws IOException {
      HSETreq(key, field, value);
      con.out.flush();
      return RESP.readAsInteger(con.in, buf);
   }

   protected void HGETALLreq(String key) throws IOException {
      con.out.write(HGETALL);
      writeBulkString(con.out, key);
   }

   public Object[] HGETALL(String key) throws IOException {
      HGETALLreq(key);
      con.out.flush();
      return RESP.readAsArray(con.in, buf);
   }

   protected void HMGETreq(String key, String... fields) throws IOException {
      con.out.write(ARRAY_PREFIX);
      con.out.writeInt(fields.length + 2);
      con.out.write(HMGET);
      writeBulkString(con.out, key);
      for (String field : fields) {
         writeBulkString(con.out, field);
      }
   }

   public Object[] HMGET(String key, String... fields) throws IOException {
      HMGETreq(key, fields);
      con.out.flush();
      return RESP.readAsArray(con.in, buf);
   }

   protected void INFOreq() throws IOException {
      con.out.write(INFO0);
   }

   public String INFO() throws IOException {
      INFOreq();
      con.out.flush();
      return RESP.readAsBulkStringString(con.in, buf);
   }

   protected void INFOreq(String section) throws IOException {
      con.out.write(INFO1);
      writeBulkString(con.out, section);
   }

   public String INFO(String section) throws IOException {
      INFOreq(section);
      con.out.flush();
      return RESP.readAsBulkStringString(con.in, buf);
   }

   public NodeTransaction MULTI() throws IOException {
      con.out.write(MULTI);
      con.out.flush();
      RESP.readAsSimpleString(con.in, buf);
      return trans;
   }

   protected void WATCHreq(String... keys) throws IOException {
      con.out.write(ARRAY_PREFIX);
      con.out.writeInt(keys.length + 1);
      con.out.write(WATCH);
      for (String key : keys) {
         writeBulkString(con.out, key);
      }
   }

   public String WATCH(String... keys) throws IOException {
      WATCHreq(keys);
      con.out.flush();
      return RESP.readAsSimpleString(con.in, buf);
   }

   protected void UNWATCHreq() throws IOException {
      con.out.write(UNWATCH);
   }

   public String UNWATCH() throws IOException {
      UNWATCHreq();
      con.out.flush();
      return RESP.readAsSimpleString(con.in, buf);
   }

   protected void GETreq(String key) throws IOException {
      con.out.write(GET);
      writeBulkString(con.out, key);
   }

   public String GET(String key) throws IOException {
      GETreq(key);
      con.out.flush();
      return RESP.readAsBulkStringString(con.in, buf);
   }
}
