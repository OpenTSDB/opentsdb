package tsd.client;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.google.gwt.user.client.ui.HorizontalPanel;
import com.google.gwt.user.client.ui.InlineLabel;
import com.google.gwt.user.client.ui.SuggestBox;
import com.google.gwt.user.client.ui.VerticalPanel;
import com.google.gwt.user.client.ui.Widget;

public class MetricFormulaForm extends HorizontalPanel {
  private final VerticalPanel formulas = new VerticalPanel();

  private Set<String> metricNames = new HashSet<String>();

  private final MetricExpressionUtils metricExpressionUtils = MetricExpressionUtils
      .getInstance();

  public MetricFormulaForm() {
    super();

    this.add(new InlineLabel("Formula"));
    this.add(formulas);

    this.setSpacing(2);
    this.setWidth("100%");

    formulas.setWidth("100%");

    addFormula();
  }

  public void updateAutoSuggestions(Set<String> metricNames) {
    if (metricNames != null) {
      this.metricNames = metricNames;
    }

    for (int i = 0; i < formulas.getWidgetCount(); i++) {
      final Widget widget = formulas.getWidget(i);

      if (widget instanceof SuggestBox) {
        final SuggestBox suggestBox = (SuggestBox) widget;
        final MetricArithmeticExpressionOracle suggestOracle = (MetricArithmeticExpressionOracle) suggestBox
            .getSuggestOracle();

        suggestOracle.setMetrics(this.metricNames);
      }
    }
  }

  public String buildQueryString(List<String> queryMetricNames) {
    StringBuilder result = new StringBuilder();
    for (int i = 0; i < formulas.getWidgetCount(); i++) {
      final Widget widget = formulas.getWidget(i);

      if (widget instanceof SuggestBox) {
        final SuggestBox suggestBox = (SuggestBox) widget;
        String expression = suggestBox.getTextBox().getText();

        for (String metricName : metricNames) {
          final int metricIndex = Integer.valueOf(metricName.replaceAll(
              "[^\\d]", "")) - 1;
          final String queryMetricName = queryMetricNames.get(metricIndex);
          expression = metricExpressionUtils.replaceOperands(expression,
              metricName, queryMetricName);
        }

        if (expression != null) {
          expression = expression.replaceAll("\\s", "")
              .replaceAll("\\+", "%2B");

          result.append("&e=").append(expression);
        }
      }
    }
    return result.toString();
  }

  private void addFormula() {
    final MetricArithmeticExpressionOracle suggestOracle = new MetricArithmeticExpressionOracle();
    final SuggestBox suggestBox = new SuggestBox(suggestOracle);

    suggestBox.setWidth("100%");

    suggestOracle.setMetrics(metricNames);

    formulas.add(suggestBox);
  }
}