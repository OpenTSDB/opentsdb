package tsd.client;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.google.gwt.user.client.ui.SuggestOracle;

public class MetricArithmeticExpressionOracle extends SuggestOracle {
  private final Set<String> metrics = new HashSet<String>();
  private final MetricExpressionUtils metricExpressionUtils = MetricExpressionUtils
      .getInstance();

  public MetricArithmeticExpressionOracle() {
  }

  @Override
  public void requestSuggestions(final Request request, final Callback callback) {
    final String query = request.getQuery();
    final String[] tokens = query.replaceAll("[\\(\\)]", "").split(
        "\\s*[\\+\\-\\*/]\\s*");
    List<MetricSuggestion> suggestions = new ArrayList<MetricSuggestion>();

    for (String token : tokens) {
      for (String metric : metrics) {
        if (metric.indexOf(token) != -1) {
          String suggestion = metricExpressionUtils.replaceOperands(query,
              token, metric);

          if (suggestion != null && !suggestion.equals(query)) {
            suggestions.add(new MetricSuggestion(suggestion));
          }
        }
      }
    }

    callback.onSuggestionsReady(request,
        new SuggestOracle.Response(suggestions));
  }

  public void setMetrics(Collection<String> metrics) {
    this.metrics.clear();

    this.metrics.addAll(metrics);
  }

  private class MetricSuggestion implements SuggestOracle.Suggestion {
    private final String suggestion;

    public MetricSuggestion(String suggestion) {
      this.suggestion = suggestion;
    }

    @Override
    public String getDisplayString() {
      return suggestion;
    }

    @Override
    public String getReplacementString() {
      return suggestion;
    }
  }
}