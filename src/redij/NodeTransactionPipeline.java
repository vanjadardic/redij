package redij;

import java.io.IOException;
import redij.exception.RedisException;

public class NodeTransactionPipeline {

   private static final Object[] EMPTY_RESPONSE = new Object[0];
   public final NodeTransaction trans;
   private int count;

   public NodeTransactionPipeline(NodeTransaction trans) {
      this.trans = trans;
      count = 0;
   }

   public void PING() throws IOException {
      trans.node.PINGreq();
      count++;
   }

   public void PING(String arg) throws IOException {
      trans.node.PINGreq(arg);
      count++;
   }

   public void INCR(String key) throws IOException {
      trans.node.INCRreq(key);
      count++;
   }

   public void HSET(String key, String field, String value) throws IOException {
      trans.node.HSETreq(key, field, value);
      count++;
   }

   public void HGETALL(String key) throws IOException {
      trans.node.HGETALLreq(key);
      count++;
   }

   public void HMGET(String key, String... fields) throws IOException {
      trans.node.HMGETreq(key, fields);
      count++;
   }

   public void INFO() throws IOException {
      trans.node.INFOreq();
      count++;
   }

   public void INFO(String section) throws IOException {
      trans.node.INFOreq(section);
      count++;
   }

   public void GET(String key) throws IOException {
      trans.node.GETreq(key);
      count++;
   }

   public Object[] EXEC() throws IOException {
      trans.EXECreq();
      count++;
      Object[] responses = sync(false);
      Object execResponse = responses[responses.length - 1];
      if (execResponse instanceof RedisException) {
         throw (RedisException) execResponse;
      }
      return (Object[]) responses[responses.length - 1];
   }

   public Object[] DISCARD() throws IOException {
      trans.DISCARDreq();
      count++;
      return sync();
   }

   private Object[] sync() throws IOException {
      return sync(true);
   }

   private Object[] sync(boolean discardOnError) throws IOException {
      if (count == 0) {
         return EMPTY_RESPONSE;
      }
      trans.node.con.out.flush();
      Object[] responses = new Object[count];
      boolean hasException = false;
      for (int i = 0; i < responses.length; i++) {
         responses[i] = RESP.read(trans.node.con.in, trans.node.buf);
         if (responses[i] instanceof RedisException) {
            hasException = true;
         }
         count--;
      }
      if (discardOnError && hasException) {
         trans.DISCARD();
      }
      return responses;
   }

   public void clear() throws IOException {
      sync();
   }
}
