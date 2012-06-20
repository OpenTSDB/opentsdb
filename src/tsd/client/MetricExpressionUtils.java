package tsd.client;

import com.google.gwt.core.client.GWT;
import com.google.gwt.regexp.shared.RegExp;

public class MetricExpressionUtils {
  // find non-metric characters before and after metric fragments
  private static final RegExp REG_EXP_METRIC_SEPARATORS = RegExp
      .compile("[\\s\\+\\-\\*/\\(\\)]");
  public static final char METRIC_INDEX_LEFT_BRACKET = '[';
  public static final char METRIC_INDEX_RIGHT_BRACKET = ']';

  private static MetricExpressionUtils INSTANCE;

  public static synchronized MetricExpressionUtils getInstance() {
    if (INSTANCE == null) {
      INSTANCE = new MetricExpressionUtils();
    }

    return INSTANCE;
  }

  public String replaceOperands(final String expression, final String operand,
      final String replacement) {
    String result = expression;
    int fromIndex = 0;
    int index;

    if (expression.length() > 0 && !operand.equals(replacement)) {
      while ((index = result.indexOf(operand, fromIndex)) != -1) {
        fromIndex += operand.length();
        final String beforeToken = index == 0 ? "" : result.substring(0, index);
        final int afterTokenIndex = index + operand.length();
        final String afterToken = afterTokenIndex == result.length() ? ""
            : result.substring(afterTokenIndex);

        if ((beforeToken.length() == 0 || REG_EXP_METRIC_SEPARATORS.test(String
            .valueOf(beforeToken.charAt(beforeToken.length() - 1))))
            && (afterToken.length() == 0 || REG_EXP_METRIC_SEPARATORS
                .test(String.valueOf(afterToken.charAt(0))))) {
          StringBuilder queryBuilder = new StringBuilder();

          queryBuilder.append(beforeToken).append(replacement)
              .append(afterToken);

          result = queryBuilder.toString();
        }
      }
    }

    return result;
  }

  private MetricExpressionUtils() {
  }
}