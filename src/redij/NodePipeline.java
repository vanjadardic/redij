package redij;

import java.io.IOException;
import redij.exception.RedisException;

public class NodePipeline {

   private static final Object[] EMPTY_RESPONSE = new Object[0];
   private final Node node;
   private int count;

   public NodePipeline(Node node) {
      this.node = node;
      count = 0;
   }

   public void PING() throws IOException {
      node.PINGreq();
      count++;
   }

   public void PING(String arg) throws IOException {
      node.PINGreq(arg);
      count++;
   }

   public void INCR(String key) throws IOException {
      node.INCRreq(key);
      count++;
   }

   public void HSET(String key, String field, String value) throws IOException {
      node.HSETreq(key, field, value);
      count++;
   }

   public void HGETALL(String key) throws IOException {
      node.HGETALLreq(key);
      count++;
   }

   public void HMGET(String key, String... fields) throws IOException {
      node.HMGETreq(key, fields);
      count++;
   }

   public void INFO() throws IOException {
      node.INFOreq();
      count++;
   }

   public void INFO(String section) throws IOException {
      node.INFOreq(section);
      count++;
   }

   public void GET(String key) throws IOException {
      node.GETreq(key);
      count++;
   }

   public Object[] sync() throws IOException {
      if (count == 0) {
         return EMPTY_RESPONSE;
      }
      node.con.out.flush();
      Object[] responses = new Object[count];
      for (int i = 0; i < responses.length; i++) {
         try {
            responses[i] = RESP.read(node.con.in, node.buf);
         } catch (RedisException ex) {
            responses[i] = ex;
         }
      }
      return responses;
   }

   public void clear() {
      count = 0;
   }
}
