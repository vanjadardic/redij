package redij;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import redij.util.Buffer;
import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;

public class Node {

   private final String host;
   private final int port;
   private final Buffer buf;
   private Socket socket;
   private BufferedOutputStream out;
   private BufferedInputStream in;
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
      out = new BufferedOutputStream(socket.getOutputStream(), 512);
      in = new BufferedInputStream(socket.getInputStream(), 4096);
   }

   private static void writeBulkString(OutputStream out, String value) throws IOException {
      byte[] valueBytes = value.getBytes(RESP.DEFAULT_CHARSET);
      out.write(Integer.toString(valueBytes.length).getBytes(RESP.DEFAULT_CHARSET));
      out.write(CRLF);
      out.write(valueBytes);
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
      return new String(RESP.readAsBulkString(in, buf), RESP.DEFAULT_CHARSET);
   }

   public Long INCR(String param1) throws IOException {
      out.write(INCR);
      writeBulkString(out, param1);
      out.flush();
      return RESP.readAsInteger(in, buf);
   }
}
