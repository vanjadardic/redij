package redij;

import java.io.IOException;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.PooledObjectFactory;
import org.apache.commons.pool2.impl.DefaultPooledObject;

public class NodeFactory implements PooledObjectFactory<Node> {

   public static final int DEFAULT_TIMEOUT = 2000;
   public static final int DEFAULT_OUTPUT_BUFFER_SIZE = 8 * 1024;
   public static final int DEFAULT_INPUT_BUFFER_SIZE = 32 * 1024;
   private final String host;
   private final int port;
   private final int socketTimeout;
   private final int soTimeout;
   private final int outputBufferSize;
   private final int inputBufferSize;

   public NodeFactory(String host, int port, int socketTimeout, int soTimeout, int outputBufferSize, int inputBufferSize) {
      this.host = host;
      this.port = port;
      this.socketTimeout = socketTimeout;
      this.soTimeout = soTimeout;
      this.outputBufferSize = outputBufferSize;
      this.inputBufferSize = inputBufferSize;
   }

   public NodeFactory(String host, int port) {
      this.host = host;
      this.port = port;
      this.socketTimeout = DEFAULT_TIMEOUT;
      this.soTimeout = DEFAULT_TIMEOUT;
      this.outputBufferSize = DEFAULT_OUTPUT_BUFFER_SIZE;
      this.inputBufferSize = DEFAULT_INPUT_BUFFER_SIZE;
   }

   @Override
   public PooledObject<Node> makeObject() throws Exception {
      NodeConnection connection = new NodeConnection(host, port, socketTimeout, soTimeout, outputBufferSize, inputBufferSize);
      connection.open();
      return new DefaultPooledObject<>(new Node(connection));
   }

   @Override
   public void destroyObject(PooledObject<Node> p) throws Exception {
      Node node = p.getObject();
      node.con.close();
      node.buf.clear();
      node.pipe.clear();
   }

   @Override
   public boolean validateObject(PooledObject<Node> p) {
      try {
         Node node = p.getObject();
         return node.con.isConnected() && "PONG".equals(node.PING());
      } catch (IOException ex) {
      }
      return false;
   }

   @Override
   public void activateObject(PooledObject<Node> p) throws Exception {
   }

   @Override
   public void passivateObject(PooledObject<Node> p) throws Exception {
   }
}
