package redij;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;

public class JRedicPoolConfig extends GenericObjectPoolConfig {

   public JRedicPoolConfig() {
      setTestWhileIdle(true);
      setMinEvictableIdleTimeMillis(60000);
      setTimeBetweenEvictionRunsMillis(30000);
      setNumTestsPerEvictionRun(-1);
   }
}
