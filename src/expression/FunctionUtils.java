package net.opentsdb.expression;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FunctionUtils {
  private static final Logger LOG = LoggerFactory
      .getLogger(FunctionUtils.class);

  private FunctionUtils() {
    // do not instantiate
  }

  public static final Map<String, FunctionCalculator> getFunctions() {
    Map<String, FunctionCalculator> result = new HashMap<String, FunctionCalculator>();
    String property = System.getProperty("tsd.expression.fncalculators");

    if (property != null && !property.isEmpty()) {
      String[] definitions = property.split(",");

      for (String definition : definitions) {
        if (definition.indexOf(':') > 0) {
          String[] keyValue = definition.split(":");

          if (keyValue.length == 2) {
            String key = keyValue[0];
            FunctionCalculator value = instantiate(keyValue[1]);

            if (value != null) {
              result.put(key, value);
            }
          }
        }
      }
    }

    return result;
  }

  public static final Set<String> getFunctionNames() {
    Map<String, FunctionCalculator> functions = getFunctions();

    return functions.keySet();
  }

  @SuppressWarnings("unchecked")
  private static final FunctionCalculator instantiate(String className) {
    FunctionCalculator result = null;

    try {
      Class<FunctionCalculator> clazz = (Class<FunctionCalculator>) Class
          .forName(className);

      result = clazz.newInstance();
    } catch (ClassNotFoundException e) {
      LOG.error(e.getMessage(), e);
    } catch (InstantiationException e) {
      LOG.error(e.getMessage(), e);
    } catch (IllegalAccessException e) {
      LOG.error(e.getMessage(), e);
    }

    return result;
  }
}
