package net.opentsdb.utils;

public interface TSDBQueryQueue<T> {

  void put(T t);

  T take() throws InterruptedException;

  int size();

  void shutdown();
}
