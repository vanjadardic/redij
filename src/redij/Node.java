package redij;

import java.io.ByteArrayInputStream;
import redij.util.Buffer;
import java.io.IOException;
import java.net.Socket;
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
   private static final byte[] PING1 = "*1\r\n$4\r\nPING\r\n".getBytes();
   private static final byte[] PING2 = "*2\r\n$4\r\nPING\r\n$".getBytes();
   private static final byte[] INCR = "*2\r\n$4\r\nINCR\r\n$".getBytes();

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

   private static void writeBulkString(RedisOutputStream out, String string) throws IOException {
      out.writeUtf8Length(string);
      out.write(CRLF);
      out.writeUtf8(string);
      out.write(CRLF);
   }

   public String PING() throws IOException {
      out.write(PING1);
      out.flush();
      return RESP.readAsSimpleString(in, buf);
   }

   public String PING(String param1) throws IOException {
      out.write(PING2);
      writeBulkString(out, param1);
      out.flush();
      return new String(RESP.readAsBulkString(in, buf));
   }

   public Long INCR(String param1) throws IOException {
      out.write(INCR);
      writeBulkString(out, param1);
      out.flush();
      return RESP.readAsInteger(in, buf);
   }
}
