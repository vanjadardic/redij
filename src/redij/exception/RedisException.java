package redij.exception;

public class RedisException extends RuntimeException {

   private String type;
   private String details;

   public RedisException(String s) {
      super(s);
      if (s != null) {
         int firstSpace = s.indexOf(' ');
         if (firstSpace != -1) {
            type = s.substring(0, firstSpace);
            if (s.length() >= firstSpace + 1) {
               details = s.substring(firstSpace + 1);
            }
         } else {
            type = s;
         }
      }
   }

   public String getType() {
      return type;
   }

   public String getDetails() {
      return details;
   }
}
