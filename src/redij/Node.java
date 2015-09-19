package redij;

import java.io.BufferedReader;
import redij.util.Buffer;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;

public class Node {

   private final String host;
   private final int port;
   private final Buffer buf;
   private Socket socket;
   private OutputStream out;
   private InputStream in;
   /**
    * ******** COMMANDS *********
    */
   private static final byte[] SEPARATOR = " ".getBytes();
   private static final byte[] NEWLINE = "\r\n".getBytes();
   private static final byte[] PING = "PING".getBytes();
   private static final byte[] INCR = "INCR".getBytes();

   public Node(String host, int port) {
      this.host = host;
      this.port = port;
      buf = new Buffer();
   }

   public void openConnection() throws IOException {
      socket = new Socket(host, port);
      out = socket.getOutputStream();
      in = socket.getInputStream();

   }

   public String PING(String message) throws IOException {
      out.write(PING);
      if (message != null) {
         out.write(SEPARATOR);
         out.write(message.getBytes(RESP.DEFAULT_CHARSET));
      }
      out.write(NEWLINE);
      if (message != null) {
         return new String(RESP.readAsBulkString(in, buf), RESP.DEFAULT_CHARSET);
      } else {
         return RESP.readAsSimpleString(in, buf);
      }
   }

   public String PING() throws IOException {
      return PING(null);
   }

   public Long INCR(String key) throws IOException {
      out.write(INCR);
      out.write(SEPARATOR);
      out.write(key.getBytes(RESP.DEFAULT_CHARSET));
      out.write(NEWLINE);
      return RESP.readAsInteger(in, buf);
   }
}
