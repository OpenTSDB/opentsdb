package net.opentsdb.expression;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class TimestampValues implements Iterable<TimestampValue> {
  private List<TimestampValue> values;

  public TimestampValues() {
    this.values = new ArrayList<TimestampValue>();
  }

  public void add(TimestampValue value) {
    this.values.add(value);
  }

  @Override
  public Iterator<TimestampValue> iterator() {
    return values.iterator();
  }

  public boolean isEmpty() {
    return values.isEmpty();
  }
}
