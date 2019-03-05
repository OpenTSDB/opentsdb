package net.opentsdb.events;

public class Bucket {

  public String key;

  public long count;

  public Bucket(String key, long count) {
    this.key = key;
    this.count = count;
  }


  public String key() {
    return key;
  }

  public long count() {
    return count;
  }


}
