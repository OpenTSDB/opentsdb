package tsd.client;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.google.gwt.core.client.GWT;
import com.google.gwt.user.client.ui.SuggestOracle;

public class MetricArithmeticExpressionOracle extends SuggestOracle {
  private final Set<String> metrics = new HashSet<String>();
  private final Set<String> functions = new HashSet<String>();
  private final MetricExpressionUtils metricExpressionUtils = MetricExpressionUtils
      .getInstance();

  public MetricArithmeticExpressionOracle() {
  }

  @Override
  public void requestSuggestions(final Request request, final Callback callback) {
    final String query = request.getQuery();
    final String[] tokens = query.replaceAll("\\s", "").split(
        "[^\\w\\[\\]\\.]");
    final List<MetricSuggestion> suggestions = new ArrayList<MetricSuggestion>();

    for (String token : tokens) {
      if (token.length() > 0) {
        suggestions.addAll(suggestMetrics(query, token));
        suggestions.addAll(suggestFunctions(query, token));
      }
    }

    callback.onSuggestionsReady(request,
        new SuggestOracle.Response(suggestions));
  }

  private List<MetricSuggestion> suggestMetrics(final String query,
      final String token) {
    return suggestInternal(query, token, metrics);
  }

  private List<MetricSuggestion> suggestFunctions(final String query,
      final String token) {
    return suggestInternal(query, token, functions);
  }

  private List<MetricSuggestion> suggestInternal(final String query,
      final String token, final Set<String> suggestValues) {
    List<MetricSuggestion> result = new ArrayList<MetricArithmeticExpressionOracle.MetricSuggestion>();

    for (String suggestValue : suggestValues) {
      if (suggestValue.indexOf(token) != -1) {
        String suggestion = metricExpressionUtils.replaceOperands(query, token,
            suggestValue);

        if (suggestion != null && !suggestion.equals(query)) {
          result.add(new MetricSuggestion(suggestion));
        }
      }
    }

    return result;
  }

  public void setMetrics(final Collection<String> metrics) {
    this.metrics.clear();
    this.metrics.addAll(metrics);
  }

  public void setFunctions(final Collection<String> functions) {
    this.functions.clear();
    this.functions.addAll(functions);
  }

  private class MetricSuggestion implements SuggestOracle.Suggestion {
    private final String suggestion;

    public MetricSuggestion(final String suggestion) {
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