package net.opentsdb.expression;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class ArithmeticNodeResult implements Iterable<TimestampValue> {
  private String name;
  private List<TimestampValue> values;

  public ArithmeticNodeResult(String name) {
    this.name = name;
    this.values = new ArrayList<TimestampValue>();
  }

  public String getName() {
    return name;
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
