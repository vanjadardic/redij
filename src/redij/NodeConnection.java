package redij;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import redij.util.RedisInputStream;
import redij.util.RedisOutputStream;

public class NodeConnection {

   private final String host;
   private final int port;
   private final int socketTimeout;
   private final int soTimeout;
   private final int outputBufferSize;
   private final int inputBufferSize;
   private Socket socket;
   public RedisOutputStream out;
   public RedisInputStream in;

   public NodeConnection(String host, int port, int socketTimeout, int soTimeout, int outputBufferSize, int inputBufferSize) {
      this.host = host;
      this.port = port;
      this.socketTimeout = socketTimeout;
      this.soTimeout = soTimeout;
      this.outputBufferSize = outputBufferSize;
      this.inputBufferSize = inputBufferSize;
      socket = null;
      out = null;
      in = null;
   }

   public void open() throws IOException {
      close();
      socket = new Socket();
      socket.setReuseAddress(true);
      socket.setKeepAlive(true);
      socket.setTcpNoDelay(true);
      socket.setSoLinger(true, 0);
      socket.setSoTimeout(soTimeout);
      socket.connect(new InetSocketAddress(host, port), socketTimeout);
      out = new RedisOutputStream(socket.getOutputStream(), outputBufferSize);
      in = new RedisInputStream(socket.getInputStream(), inputBufferSize);
   }

   public void close() {
      if (in != null) {
         try {
            in.close();
         } catch (IOException ex) {
         }
         in = null;
      }
      if (out != null) {
         try {
            out.close();
         } catch (IOException ex) {
         }
         out = null;
      }
      if (socket != null) {
         try {
            socket.close();
         } catch (IOException ex) {
         }
         socket = null;
      }
   }

   public boolean isConnected() {
      return socket != null && socket.isBound() && !socket.isClosed() && socket.isConnected()
            && !socket.isInputShutdown() && !socket.isOutputShutdown();
   }
}
