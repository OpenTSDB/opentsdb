package net.opentsdb.configuration;

public class Scratch {

  public static void main(final String[] args) throws Exception {
    try (Configuration c = new Configuration(args)) {
      
      c.register("my.key", "42", true, "testing");
      
      System.out.println(Configuration.OBJECT_MAPPER.writeValueAsString(c.getView()));
    }
  }
}
