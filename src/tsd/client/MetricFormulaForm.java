package tsd.client;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.google.gwt.event.dom.client.BlurEvent;
import com.google.gwt.event.dom.client.BlurHandler;
import com.google.gwt.http.client.Request;
import com.google.gwt.http.client.RequestBuilder;
import com.google.gwt.http.client.RequestCallback;
import com.google.gwt.http.client.RequestException;
import com.google.gwt.http.client.Response;
import com.google.gwt.user.client.ui.CheckBox;
import com.google.gwt.user.client.ui.HorizontalPanel;
import com.google.gwt.user.client.ui.InlineLabel;
import com.google.gwt.user.client.ui.SuggestBox;
import com.google.gwt.user.client.ui.VerticalPanel;
import com.google.gwt.user.client.ui.Widget;

public class MetricFormulaForm extends VerticalPanel {
  private final VerticalPanel formulas = new VerticalPanel();
  private final CheckBox hideMetrics = new CheckBox("Hide Metrics in Graph");

  private final EventsHandler graphRefreshHandler;

  private Set<String> metricNames = new HashSet<String>();
  private Set<String> functionNames = new HashSet<String>();

  private final MetricExpressionUtils metricExpressionUtils = MetricExpressionUtils
      .getInstance();

  public MetricFormulaForm(EventsHandler graphRefreshHandler) {
    super();

    HorizontalPanel formulaContainer = new HorizontalPanel();
    formulaContainer.add(new InlineLabel("Formula"));
    formulaContainer.add(formulas);
    formulaContainer.setWidth("100%");

    this.graphRefreshHandler = graphRefreshHandler;
    this.hideMetrics.addClickHandler(graphRefreshHandler);

    this.setHorizontalAlignment(ALIGN_RIGHT);
    this.add(hideMetrics);
    this.add(formulaContainer);

    this.setSpacing(2);
    this.setWidth("100%");

    formulas.setWidth("100%");

    initFunctionNames();
  }

  public void handleMetricNamesChanged(final Set<String> newMetricNames) {
    if (newMetricNames != null && newMetricNames.size() > 0) {
      final Set<String> removedMetricNames = getRemovedMetricNames(newMetricNames);

      this.metricNames = newMetricNames;

      updateAutoSuggestions();
      removeInvalidFormulas(removedMetricNames);
    }
  }

  public String buildQueryString(final List<String> queryMetricNames) {
    final StringBuilder result = new StringBuilder();

    for (int i = 0; i < formulas.getWidgetCount(); i++) {
      final Widget widget = formulas.getWidget(i);

      if (widget instanceof SuggestBox) {
        final SuggestBox suggestBox = (SuggestBox) widget;
        String expression = suggestBox.getText();

        for (String metricName : metricNames) {
          final int metricIndex = Integer.valueOf(metricName.replaceAll(
              "[^\\d]", "")) - 1;
          final String queryMetricName = queryMetricNames.get(metricIndex);
          expression = metricExpressionUtils.replaceOperands(expression,
              metricName, "\"" + queryMetricName + "\"");
        }

        if (expression != null) {
          expression = expression.replaceAll("\\s", "")
              .replaceAll("\\+", "%2B");

          result.append("&e=").append(expression);
        }
      }
    }

    result.append("&hm=").append(hideMetrics.getValue().booleanValue());

    return result.toString();
  }

  private Set<String> getRemovedMetricNames(Set<String> newMetricNames) {
    final Set<String> result = new HashSet<String>();

    for (final String metricName : this.metricNames) {
      if (!newMetricNames.contains(metricName)) {
        result.add(metricName);
      }
    }

    return result;
  }

  private void updateAutoSuggestions() {
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

  private void removeInvalidFormulas(Set<String> removedMetricNames) {
    if (removedMetricNames.size() > 0) {
      for (int i = 0; i < formulas.getWidgetCount(); i++) {
        final Widget widget = formulas.getWidget(i);

        if (widget instanceof SuggestBox) {
          final SuggestBox suggestBox = (SuggestBox) widget;
          final String expression = suggestBox.getText();

          for (String removedMetricName : removedMetricNames) {
            if (expression.indexOf(removedMetricName) >= 0) {
              suggestBox.setText("");
            }
          }
        }
      }

      cleanup();
    }
  }

  private void addFormula() {
    final MetricArithmeticExpressionOracle suggestOracle = new MetricArithmeticExpressionOracle();
    final SuggestBox suggestBox = new SuggestBox(suggestOracle);

    suggestBox.setWidth("100%");
    suggestBox.getTextBox().addBlurHandler(graphRefreshHandler);
    suggestBox.getTextBox().addBlurHandler(new BlurHandler() {
      public void onBlur(BlurEvent event) {
        addFormula();
      }
    });

    suggestOracle.setMetrics(metricNames);
    suggestOracle.setFunctions(this.functionNames);

    formulas.add(suggestBox);

    cleanup();
  }

  private void cleanup() {
    for (Widget widget : formulas) {
      if (widget instanceof SuggestBox) {
        SuggestBox suggestBox = (SuggestBox) widget;

        if (!isLastFormula(suggestBox) && suggestBox.getText().isEmpty()) {
          formulas.remove(suggestBox);
        }
      }
    }
  }

  private boolean isLastFormula(Widget widget) {
    return formulas.getWidgetIndex(widget) == formulas.getWidgetCount() - 1;
  }

  private void initFunctionNames() {
    final String url = "/functions";
    final RequestBuilder builder = new RequestBuilder(RequestBuilder.GET, url);
    try {
      builder.sendRequest(null, new RequestCallback() {
        public void onResponseReceived(final Request request,
            final Response response) {
          final int code = response.getStatusCode();
          if (code == Response.SC_OK) {
            String[] functions = response.getText().split(",");

            functionNames.addAll(Arrays.asList(functions));

            addFormula();
          }
        }

        @Override
        public void onError(Request request, Throwable exception) {
          // ignore errors, since the only consequence is that no auto-suggest
          // for functions will be available
        }
      });
    } catch (RequestException e) {
      // ignore errors, since the only consequence is that no auto-suggest
      // for functions will be available
    }
  }
}