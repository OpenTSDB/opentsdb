package net.opentsdb.query.expression;

import java.util.HashMap;
import java.util.Map;

import com.google.common.annotations.VisibleForTesting;

public class ExpressionFactory {
  
  private static Map<String, Expression> availableFunctions = 
      new HashMap<String, Expression>();
  
  static {
    availableFunctions.put("scale", new Scale());
    availableFunctions.put("absolute", new Absolute());
    availableFunctions.put("movingAverage", new MovingAverage());
    availableFunctions.put("highestCurrent", new HighestCurrent());
    availableFunctions.put("highestMax", new HighestMax());
  }
  
  @VisibleForTesting
  static void addFunction(String name, Expression expr) {
    availableFunctions.put(name, expr);
  }
  
  public static Expression getByName(String funcName) {
    return availableFunctions.get(funcName);
  }
}
