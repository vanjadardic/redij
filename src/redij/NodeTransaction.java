package redij;

import java.io.IOException;
import redij.exception.RedisException;

public class NodeTransaction {

   private static final byte[] EXEC = Node.createCommand("EXEC", 0);
   private static final byte[] DISCARD = Node.createCommand("DISCARD", 0);

   public final Node node;
   public final NodeTransactionPipeline transPipe;

   public NodeTransaction(Node node) {
      this.node = node;
      this.transPipe = new NodeTransactionPipeline(this);
   }

   private void handleCommand() throws IOException {
      try {
         RESP.readAsSimpleString(node.con.in, node.buf);
      } catch (RedisException ex) {
         DISCARD();
         throw ex;
      }
   }

   public void PING() throws IOException {
      node.PINGreq();
      node.con.out.flush();
      handleCommand();
   }

   public void PING(String arg) throws IOException {
      node.PINGreq(arg);
      node.con.out.flush();
      handleCommand();
   }

   public void INCR(String key) throws IOException {
      node.INCRreq(key);
      node.con.out.flush();
      handleCommand();
   }

   public void HSET(String key, String field, String value) throws IOException {
      node.HSETreq(key, field, value);
      node.con.out.flush();
      handleCommand();
   }

   public void HGETALL(String key) throws IOException {
      node.HGETALLreq(key);
      node.con.out.flush();
      handleCommand();
   }

   public void HMGET(String key, String... fields) throws IOException {
      node.HMGETreq(key, fields);
      node.con.out.flush();
      handleCommand();
   }

   public void INFO() throws IOException {
      node.INFOreq();
      node.con.out.flush();
      handleCommand();
   }

   public void INFO(String section) throws IOException {
      node.INFOreq(section);
      node.con.out.flush();
      handleCommand();
   }

   public void GET(String key) throws IOException {
      node.GETreq(key);
      node.con.out.flush();
      handleCommand();
   }

   public void UNWATCH() throws IOException {
      node.UNWATCHreq();
      node.con.out.flush();
      handleCommand();
   }

   protected void EXECreq() throws IOException {
      node.con.out.write(EXEC);
   }

   public Object[] EXEC() throws IOException {
      EXECreq();
      node.con.out.flush();
      return RESP.readAsArray(node.con.in, node.buf);
   }

   protected void DISCARDreq() throws IOException {
      node.con.out.write(DISCARD);
   }

   public String DISCARD() throws IOException {
      DISCARDreq();
      node.con.out.flush();
      return RESP.readAsSimpleString(node.con.in, node.buf);
   }

   public void clear() throws IOException {
      transPipe.clear();
   }
}
