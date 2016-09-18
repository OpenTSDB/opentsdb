// This file is part of OpenTSDB.
// Copyright (C) 2010-2012  The OpenTSDB Authors.
//
// This program is free software: you can redistribute it and/or modify it
// under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 2.1 of the License, or (at your
// option) any later version.  This program is distributed in the hope that it
// will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty
// of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser
// General Public License for more details.  You should have received a copy
// of the GNU Lesser General Public License along with this program.  If not,
// see <http://www.gnu.org/licenses/>.
package tsd.client;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import com.google.gwt.event.dom.client.BlurEvent;
import com.google.gwt.event.dom.client.BlurHandler;
import com.google.gwt.event.dom.client.ClickEvent;
import com.google.gwt.event.dom.client.ClickHandler;
import com.google.gwt.event.dom.client.DomEvent;
import com.google.gwt.event.shared.EventHandler;
import com.google.gwt.user.client.ui.Button;
import com.google.gwt.user.client.ui.CheckBox;
import com.google.gwt.user.client.ui.FlexTable;
import com.google.gwt.user.client.ui.Focusable;
import com.google.gwt.user.client.ui.HorizontalPanel;
import com.google.gwt.user.client.ui.InlineLabel;
import com.google.gwt.user.client.ui.ListBox;
import com.google.gwt.user.client.ui.SuggestBox;
import com.google.gwt.user.client.ui.TextBox;
import com.google.gwt.user.client.ui.VerticalPanel;
import com.google.gwt.user.client.ui.Widget;

final class MetricForm extends HorizontalPanel implements Focusable {

  public static interface MetricChangeHandler extends EventHandler {
    void onMetricChange(MetricForm widget);
  }

  private static final String TSDB_ID_CLASS = "[-_./a-zA-Z0-9]";
  private static final String TSDB_ID_RE = "^" + TSDB_ID_CLASS + "*$";
  private static final String TSDB_TAGVALUE_RE =
    "^(\\*?"                                       // a `*' wildcard or nothing
    + "|" + TSDB_ID_CLASS + "+(\\|" + TSDB_ID_CLASS + "+)*)$"; // `foo|bar|...'

  private final EventsHandler events_handler;
  private MetricChangeHandler metric_change_handler;

  private final CheckBox downsample = new CheckBox("Downsample");
  private final ListBox downsampler = new ListBox();
  private final ValidatedTextBox interval = new ValidatedTextBox();
  private final ListBox fill_policy = new ListBox();
  private final CheckBox rate = new CheckBox("Rate");
  private final CheckBox rate_counter = new CheckBox("Rate Ctr");
  private final TextBox counter_max = new TextBox();
  private final TextBox counter_reset_value = new TextBox();
  private final CheckBox x1y2 = new CheckBox("Right Axis");
  private final ListBox aggregators = new ListBox();
  private final ValidatedTextBox metric = new ValidatedTextBox();
  private final FlexTable tagtable = new FlexTable();

  public MetricForm(final EventsHandler handler) {
    events_handler = handler;
    setupDownsampleWidgets();
    downsample.addClickHandler(handler);
    downsampler.addChangeHandler(handler);
    interval.addBlurHandler(handler);
    interval.addKeyPressHandler(handler);
    fill_policy.addChangeHandler(handler);
    rate.addClickHandler(handler);
    rate_counter.addClickHandler(handler);
    counter_max.addBlurHandler(handler);
    counter_max.addKeyPressHandler(handler);
    counter_reset_value.addBlurHandler(handler);
    counter_reset_value.addKeyPressHandler(handler);
    x1y2.addClickHandler(handler);
    aggregators.addChangeHandler(handler);
    metric.addBlurHandler(handler);
    metric.addKeyPressHandler(handler);
    {
      final EventsHandler metric_handler = new EventsHandler() {
        protected <H extends EventHandler> void onEvent(final DomEvent<H> event) {
          if (metric_change_handler != null) {
            metric_change_handler.onMetricChange(MetricForm.this);
          }
        }
      };
      metric.addBlurHandler(metric_handler);
      metric.addKeyPressHandler(metric_handler);
    }

    metric.setValidationRegexp(TSDB_ID_RE);
    assembleUi();
  }

  public String getMetric() {
    return metric.getText();
  }

  /**
   * Parses the metric and tags out of the given string.
   * @param metric A string of the form "metric" or "metric{tag=value,...}".
   * @return The name of the metric.
   */
  private String parseWithMetric(final String metric) {
    // TODO: Try to reduce code duplication with Tags.parseWithMetric().
    final int curly = metric.indexOf('{');
    if (curly < 0) {
      clearTags();
      return metric;
    }
    final int len = metric.length();
    if (metric.charAt(len - 1) != '}') {  // "foo{"
      clearTags();
      return null;  // Missing '}' at the end.
    } else if (curly == len - 2) {  // "foo{}"
      clearTags();
      return metric.substring(0, len - 2);
    }
    final int num_tags_before = getNumTags();
    
    final List<Filter> filters = new ArrayList<Filter>();
    final int close = metric.indexOf('}');
    int i = 0;
    if (close != metric.length() - 1) { // "foo{...}{tagk=filter}" 
      final int filter_bracket = metric.lastIndexOf('{');
      for (final String filter : metric.substring(filter_bracket + 1, 
          metric.length() - 1).split(",")) {
        if (filter.isEmpty()) {
          break;
        }
        final String[] kv = filter.split("=");
        if (kv.length != 2 || kv[0].isEmpty() || kv[1].isEmpty()) {
          continue;  // Invalid tag.
        }
        final Filter f = new Filter();
        f.tagk = kv[0];
        f.tagv = kv[1];
        f.is_groupby = false;
        filters.add(f);
        i++;  
      }
    }
    
    i = 0;
    for (final String tag : metric.substring(curly + 1, close).split(",")) {
      if (tag.isEmpty() && close != metric.length() - 1){
        break;
      }
      final String[] kv = tag.split("=");
      if (kv.length != 2 || kv[0].isEmpty() || kv[1].isEmpty()) {
        continue;  // Invalid tag.
      }
      final Filter f = new Filter();
      f.tagk = kv[0];
      f.tagv = kv[1];
      f.is_groupby = true;
      filters.add(f);
      i++;
    }
    if (!filters.isEmpty()) {
      Collections.sort(filters);
    }
    
    i = 0;
    for (int x = filters.size() - 1; x >= 0; x--) {
      final Filter filter = filters.get(x);
      if (i < num_tags_before) {
        setTag(i++, filter.tagk, filter.tagv, filter.is_groupby);
      } else {
        addTag(filter.tagk, filter.tagv, filter.is_groupby);
      }
    }
    
    if (i < num_tags_before) {
      setTag(i, "", "", true);
    } else {
      addTag();
    }
    // Remove extra tags.
    for (i++; i < num_tags_before; i++) {
      tagtable.removeRow(i + 1);
    }
    // Return the "foo" part of "foo{a=b,...,x=y}"
    return metric.substring(0, curly);
    
    /*
    // substring the tags out of "foo{a=b,...,x=y}" and parse them.
    int i = 0;  // Tag index.
    final int num_tags_before = getNumTags();
    for (final String tag : metric.substring(curly + 1, len - 1).split(",")) {
      final String[] kv = tag.split("=");
      if (kv.length != 2 || kv[0].isEmpty() || kv[1].isEmpty()) {
        setTag(i, "", "", true);
        continue;  // Invalid tag.
      }
      if (i < num_tags_before) {
        setTag(i, kv[0], kv[1], true);
      } else {
        addTag(kv[0], kv[1]);
      }
      i++;
    }
    // Leave an empty line at the end.
    if (i < num_tags_before) {
      setTag(i, "", "", true);
    } else {
      addTag();
    }
    // Remove extra tags.
    for (i++; i < num_tags_before; i++) {
      tagtable.removeRow(i + 1);
    }
    // Return the "foo" part of "foo{a=b,...,x=y}"
    return metric.substring(0, curly); */
  }

  public void updateFromQueryString(final String m, final String o) {
    // TODO: Try to reduce code duplication with GraphHandler.parseQuery().
    // m is of the following forms:
    //  agg:[interval-agg:][rate[{counter[,max[,reset]]}:]metric[{tag=value,...}]
    // Where the parts in square brackets `[' .. `]' are optional.
    final String[] parts = m.split(":");
    int i = parts.length;
    if (i < 2 || i > 4) {
      return;  // Malformed.
    }

    setSelectedItem(aggregators, parts[0]);

    i--;  // Move to the last part (the metric name).
    metric.setText(parseWithMetric(parts[i]));
    metric_change_handler.onMetricChange(this);

    final boolean rate = parts[--i].startsWith("rate");
    this.rate.setValue(rate, false);
    LocalRateOptions rate_options = parseRateOptions(rate, parts[i]);
    this.rate_counter.setValue(rate_options.is_counter, false);
    final long rate_counter_max = rate_options.counter_max;
    this.counter_max.setValue(
        rate_counter_max == Long.MAX_VALUE ? "" : Long.toString(rate_counter_max), 
        false);
    this.counter_reset_value
        .setValue(Long.toString(rate_options.reset_value), false);
    if (rate) {
      i--;
    }

    // downsampling function & interval.
    if (i > 0) {
      // First dash should have been given.
      final int first_dash = parts[1].indexOf('-', 1); // 1st char can't be `-'.
      if (first_dash < 0) {
        disableDownsample();
        return;  // Invalid downsampling specifier.
      }

      // Second dash (and subsequent fill policy) are optional.
      final int second_dash = parts[1].indexOf('-', first_dash + 1);

      downsample.setValue(true, false);

      downsampler.setEnabled(true);
      fill_policy.setEnabled(true);
      if (-1 == second_dash) {
        // No fill policy given.
        setSelectedItem(downsampler, parts[1].substring(first_dash + 1));

        // So use a default.
        // TODO: don't assume this exists.
        setSelectedItem(fill_policy, "lerp");
      } else {
        // User specified fill policy.
        setSelectedItem(downsampler, parts[1].substring(first_dash + 1,
          second_dash));

        // So use what was given.
        setSelectedItem(fill_policy, parts[1].substring(second_dash + 1));
      }

      interval.setEnabled(true);
      interval.setText(parts[1].substring(0, first_dash));
    } else {
      disableDownsample();
    }

    x1y2.setValue(o.contains("axis x1y2"), false);
  }

  private void disableDownsample() {
    downsample.setValue(false, false);
    interval.setEnabled(false);
    downsampler.setEnabled(false);
    fill_policy.setEnabled(false);
  }

  public CheckBox x1y2() {
    return x1y2;
  }

  private void assembleUi() {
    setWidth("100%");
    {  // Left hand-side panel.
      final HorizontalPanel hbox = new HorizontalPanel();
      final InlineLabel l = new InlineLabel();
      l.setText("Metric:");
      hbox.add(l);
      final SuggestBox suggest = RemoteOracle.newSuggestBox("metrics",
                                                            metric);
      suggest.setLimit(40);
      hbox.add(suggest);
      hbox.setWidth("100%");
      metric.setWidth("100%");

      tagtable.setWidget(0, 0, hbox);
      tagtable.getFlexCellFormatter().setColSpan(0, 0, 3);
      addTag();
      tagtable.setText(1, 0, "Tags");
      add(tagtable);
    }
    {  // Right hand-side panel.
      final VerticalPanel vbox = new VerticalPanel();
      {
        final HorizontalPanel hbox = new HorizontalPanel();
        hbox.add(rate);
        hbox.add(rate_counter);
        hbox.add(x1y2);
        vbox.add(hbox);
      }
      {
        final HorizontalPanel hbox = new HorizontalPanel();
        final InlineLabel l = new InlineLabel("Rate Ctr Max:");
        hbox.add(l);
        hbox.add(counter_max);
        vbox.add(hbox);
      }
      {
        final HorizontalPanel hbox = new HorizontalPanel();
        final InlineLabel l = new InlineLabel("Rate Ctr Reset:");
        hbox.add(l);
        hbox.add(counter_reset_value);
        vbox.add(hbox);
      }
      {
        final HorizontalPanel hbox = new HorizontalPanel();
        final InlineLabel l = new InlineLabel();
        l.setText("Aggregator:");
        hbox.add(l);
        hbox.add(aggregators);
        vbox.add(hbox);
      }
      vbox.add(downsample);
      {
        final HorizontalPanel hbox = new HorizontalPanel();
        hbox.add(downsampler);
        hbox.add(interval);
        hbox.add(fill_policy);
        vbox.add(hbox);
      }
      add(vbox);
    }
  }

  public void setMetricChangeHandler(final MetricChangeHandler handler) {
    metric_change_handler = handler;
  }

  public void setAggregators(final ArrayList<String> aggs) {
    Object[] aggsSortedArray = aggs.toArray();
    Arrays.sort(aggsSortedArray);
    for (final Object agg : aggsSortedArray) {
      aggregators.addItem((String)agg);
      downsampler.addItem((String)agg);
    }
 // TODO: don't assume we will get these.
    setSelectedItem(aggregators, "sum");
    setSelectedItem(downsampler, "avg");
  }
  
  public void setFillPolicies(final List<String> policies) {
    for (final String policy : policies) {
      fill_policy.addItem(policy);
    }
    // TODO: don't assume we will get this.
    setSelectedItem(fill_policy, "lerp");
  }

  public boolean buildQueryString(final StringBuilder url) {
    final String metric = getMetric();
    if (metric.isEmpty()) {
      return false;
    }
    url.append("&m=");
    url.append(selectedValue(aggregators));
    if (downsample.getValue()) {
      url.append(':').append(interval.getValue())
         .append('-').append(selectedValue(downsampler))
         .append('-').append(selectedValue(fill_policy));
    }
    if (rate.getValue()) {
      url.append(":rate");
      if (rate_counter.getValue()) {
        url.append('{');//.append("counter");
        final String max = counter_max.getValue().trim();
        final String reset = counter_reset_value.getValue().trim();
        if (max.isEmpty() && (reset.equals("0") || reset.isEmpty())) {
          url.append("dropcounter");
        } else {
          url.append("counter");
        }
        if (max.length() > 0 && reset.length() > 0) {
          url.append(',').append(max).append(',').append(reset);
        } else if (max.length() > 0 && reset.length() == 0) {
          url.append(',').append(max);
        } else if (max.length() == 0 && reset.length() > 0){
          url.append(",,").append(reset);
        }
        url.append('}');
      }
    }
    url.append(':').append(metric);
    List<Filter> filters = getFilters(true);
    if (!filters.isEmpty()) {
      url.append('{');
      for (int i = 0; i < filters.size(); i++) {
        if (i > 0) {
          url.append(",");
        }
        url.append(filters.get(i).tagk)
           .append("=")
           .append(filters.get(i).tagv);
      }
      url.append('}');
    }
    // now the non-group bys
    filters = getFilters(false);
    if (!filters.isEmpty()) {
      url.append('{');
      for (int i = 0; i < filters.size(); i++) {
        if (i > 0) {
          url.append(",");
        }
        url.append(filters.get(i).tagk)
           .append("=")
           .append(filters.get(i).tagv);
      }
      url.append('}');
    }
    url.append("&o=");
    if (x1y2.getValue()) {
      url.append("axis x1y2");
    }
    return true;
  }
  
  /**
   * Helper method to extract the tags from the row set and sort them before
   * sending to the API so that we avoid a bug wherein the sort order changes
   * on reload.
   * @param group_by Whether or not to fetch group by or non-group by filters.
   * @return A non-null list of filters. May be empty.
   */
  private List<Filter> getFilters(final boolean group_by) {
    final int ntags = getNumTags();
    final List<Filter> filters = new ArrayList<Filter>(ntags);
    for (int tag = 0; tag < ntags; tag++) {
      final Filter filter = new Filter();
      filter.tagk = getTagName(tag);
      filter.tagv = getTagValue(tag);
      filter.is_groupby = isTagGroupby(tag);
      if (filter.tagk.isEmpty() || filter.tagv.isEmpty()) {
        continue;
      }
      if (filter.is_groupby = group_by) {
        filters.add(filter);
      }
    }
    // sort on the tagk
    Collections.sort(filters);
    return filters;
  }
  
  private int getNumTags() {
    return tagtable.getRowCount() - 1;
  }

  private String getTagName(final int i) {
    return ((SuggestBox) tagtable.getWidget(i + 1, 1)).getValue();
  }

  private String getTagValue(final int i) {
    return ((SuggestBox) tagtable.getWidget(i + 1, 2)).getValue();
  }
  
  private boolean isTagGroupby(final int i) {
    return ((CheckBox) tagtable.getWidget(i + 1,  3)).getValue();
  }

  private void setTagName(final int i, final String value) {
    ((SuggestBox) tagtable.getWidget(i + 1, 1)).setValue(value);
  }

  private void setTagValue(final int i, final String value) {
    ((SuggestBox) tagtable.getWidget(i + 1, 2)).setValue(value);
  }

  private void isTagGroupby(final int i, final boolean groupby) {
    ((CheckBox) tagtable.getWidget(i + 1,  3)).setValue(groupby);
  }
  
  /**
   * Changes the name/value of an existing tag.
   * @param i The index of the tag to change.
   * @param name The new name of the tag.
   * @param value The new value of the tag.
   * Requires: {@code i < getNumTags()}.
   */
  private void setTag(final int i, final String name, final String value, 
      final boolean groupby) {
    setTagName(i, name);
    setTagValue(i, value);
    isTagGroupby(i, groupby);
  }

  private void addTag() {
    addTag(null, null, true);
  }

  private void addTag(final String default_tagname) {
    addTag(default_tagname, null, true);
  }

  private void addTag(final String default_tagname,
                      final String default_value,
                      final boolean is_groupby) {
    final int row = tagtable.getRowCount();
    
    final ValidatedTextBox tagname = new ValidatedTextBox();
    final SuggestBox suggesttagk = RemoteOracle.newSuggestBox("tagk", tagname);
    final ValidatedTextBox tagvalue = new ValidatedTextBox();
    final SuggestBox suggesttagv = RemoteOracle.newSuggestBox("tagv", tagvalue);
    final CheckBox groupby = new CheckBox();
    groupby.setValue(is_groupby);
    groupby.setTitle("Group by");
    groupby.addClickHandler(events_handler);
    tagname.setValidationRegexp(TSDB_ID_RE);
    tagvalue.setValidationRegexp(TSDB_TAGVALUE_RE);
    tagname.setWidth("100%");
    tagvalue.setWidth("100%");
    tagname.addBlurHandler(recompact_tagtable);
    tagname.addBlurHandler(events_handler);
    tagname.addKeyPressHandler(events_handler);
    tagvalue.addBlurHandler(recompact_tagtable);
    tagvalue.addBlurHandler(events_handler);
    tagvalue.addKeyPressHandler(events_handler);

    tagtable.setWidget(row, 1, suggesttagk);
    tagtable.setWidget(row, 2, suggesttagv);
    tagtable.setWidget(row, 3, groupby);
    if (row > 2) {
      final Button remove = new Button("x");
      remove.addClickHandler(removetag);
      tagtable.setWidget(row - 1, 0, remove);
    }
    if (default_tagname != null) {
      tagname.setText(default_tagname);
      if (default_value == null) {
        tagvalue.setFocus(true);
      }
    }
    if (default_value != null) {
      tagvalue.setText(default_value);
    }
  }

  private void clearTags() {
    setTag(0, "", "", true);
    for (int i = getNumTags() - 1; i > 1; i++) {
      tagtable.removeRow(i + 1);
    }
  }

  public void autoSuggestTag(final String tag) {
    // First try to see if the tag is already in the table.
    final int nrows = tagtable.getRowCount();
    int unused_row = -1;
    for (int row = 1; row < nrows; row++) {
      final SuggestBox tagname = ((SuggestBox) tagtable.getWidget(row, 1));
      final SuggestBox tagvalue = ((SuggestBox) tagtable.getWidget(row, 2));
      final String thistag = tagname.getValue();
      if (thistag.equals(tag)) {
        return;  // This tag is already in the table.
      } if (thistag.isEmpty() && tagvalue.getValue().isEmpty()) {
        unused_row = row;
        break;
      }
    }
    if (unused_row >= 0) {
      ((SuggestBox) tagtable.getWidget(unused_row, 1)).setValue(tag);
    } else {
      addTag(tag);
    }
  }

  private final BlurHandler recompact_tagtable = new BlurHandler() {
    public void onBlur(final BlurEvent event) {
      int ntags = getNumTags();
      // Is the first line empty?  If yes, move everything up by 1 line.
      if (getTagName(0).isEmpty() && getTagValue(0).isEmpty()) {
        for (int tag = 1; tag < ntags; tag++) {
          final String tagname = getTagName(tag);
          final String tagvalue = getTagValue(tag);
          // todo - groupby
          setTag(tag - 1, tagname, tagvalue, isTagGroupby(tag));
        }
        setTag(ntags - 1, "", "", true);
      }
      // Try to remove empty lines from the tag table (but never remove the
      // first line or last line, even if they're empty).  Walk the table
      // from the end to make it easier to delete rows as we iterate.
      for (int tag = ntags - 1; tag >= 1; tag--) {
        final String tagname = getTagName(tag);
        final String tagvalue = getTagValue(tag);
        if (tagname.isEmpty() && tagvalue.isEmpty()) {
          tagtable.removeRow(tag + 1);
        }
      }
      ntags = getNumTags();  // How many lines are left?
      // If the last line isn't empty, add another one.
      final String tagname = getTagName(ntags - 1);
      final String tagvalue = getTagValue(ntags - 1);
      if (!tagname.isEmpty() && !tagvalue.isEmpty()) {
        addTag();
      }
    }
  };

  private final ClickHandler removetag = new ClickHandler() {
    public void onClick(final ClickEvent event) {
      if (!(event.getSource() instanceof Button)) {
        return;
      }
      final Widget source = (Widget) event.getSource();
      final int ntags = getNumTags();
      for (int tag = 1; tag < ntags; tag++) {
        if (source == tagtable.getWidget(tag + 1, 0)) {
          tagtable.removeRow(tag + 1);
          events_handler.onClick(event);
          break;
        }
      }
    }
  };

  private void setupDownsampleWidgets() {
    downsampler.setEnabled(false);
    fill_policy.setEnabled(false);
    interval.setEnabled(false);
    interval.setMaxLength(5);
    interval.setVisibleLength(5);
    interval.setValue("10m");
    interval.setValidationRegexp("^[1-9][0-9]*[smhdwy]$");
    downsample.addClickHandler(new ClickHandler() {
      public void onClick(final ClickEvent event) {
        final boolean checked = ((CheckBox) event.getSource()).getValue();
        downsampler.setEnabled(checked);
        fill_policy.setEnabled(checked);
        interval.setEnabled(checked);
        if (checked) {
          downsampler.setFocus(true);
        }
      }
    });
  }

  private static String selectedValue(final ListBox list) {  // They should add
    return list.getValue(list.getSelectedIndex());           // this to GWT...
  }

  /**
   * If the given item is in the list, mark it as selected.
   * @param list The list to manipulate.
   * @param item The item to select if present.
   */
  private void setSelectedItem(final ListBox list, final String item) {
    final int nitems = list.getItemCount();
    for (int i = 0; i < nitems; i++) {
      if (item.equals(list.getValue(i))) {
        list.setSelectedIndex(i);
        return;
      }
    }
  }

  /**
   * Class used for parsing and rate options
   */
  private static class LocalRateOptions {
    public boolean is_counter;
    public long counter_max = Long.MAX_VALUE;
    public long reset_value = 0;
  }
  
  /**
   * Parses the "rate" section of the query string and returns an instance
   * of the LocalRateOptions class that contains the values found.
   * <p/>
   * The format of the rate specification is rate[{counter[,#[,#]]}].
   * If the spec is invalid or we were unable to parse properly, it returns a
   * default options object.
   * @param rate If true, then the query is set as a rate query and the rate
   * specification will be parsed. If false, a default RateOptions instance
   * will be returned and largely ignored by the rest of the processing
   * @param spec The part of the query string that pertains to the rate
   * @return An initialized LocalRateOptions instance based on the specification
   * @since 2.0
   */
  static final public LocalRateOptions parseRateOptions(boolean rate, String spec) {
    if (!rate || spec.length() < 6) {
      return new LocalRateOptions();
    }

    String[] parts = spec.split(spec.substring(5, spec.length() - 1), ',');
    if (parts.length < 1 || parts.length > 3) {
      return new LocalRateOptions();
    }

    try {
      LocalRateOptions options = new LocalRateOptions();
      options.is_counter = parts[0].endsWith("counter");
      options.counter_max = (parts.length >= 2 && parts[1].length() > 0 ? Long
          .parseLong(parts[1]) : Long.MAX_VALUE);
      options.reset_value = (parts.length >= 3 && parts[2].length() > 0 ? Long
          .parseLong(parts[2]) : 0);
      return options;
    } catch (NumberFormatException e) {
      return new LocalRateOptions();
    }
  }
  
  private static class Filter implements Comparable<Filter> {
    String tagk;
    String tagv;
    boolean is_groupby;
    
    @Override
    public int compareTo(final Filter filter) {
      if (filter == this) {
        return 0;
      }
      return tagk.compareTo(filter.tagk) +
          tagv.compareTo(filter.tagv);
    }
  }
  
  // ------------------- //
  // Focusable interface //
  // ------------------- //

  public int getTabIndex() {
    return metric.getTabIndex();
  }

  public void setTabIndex(final int index) {
    metric.setTabIndex(index);
  }

  public void setAccessKey(final char key) {
    metric.setAccessKey(key);
  }

  public void setFocus(final boolean focused) {
    metric.setFocus(focused);
  }

}
