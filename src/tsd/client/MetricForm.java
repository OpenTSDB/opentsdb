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
    // substring the tags out of "foo{a=b,...,x=y}" and parse them.
    int i = 0;  // Tag index.
    final int num_tags_before = getNumTags();
    for (final String tag : metric.substring(curly + 1, len - 1).split(",")) {
      final String[] kv = tag.split("=");
      if (kv.length != 2 || kv[0].isEmpty() || kv[1].isEmpty()) {
        setTag(i, "", "");
        continue;  // Invalid tag.
      }
      if (i < num_tags_before) {
        setTag(i, kv[0], kv[1]);
      } else {
        addTag(kv[0], kv[1]);
      }
      i++;
    }
    // Leave an empty line at the end.
    if (i < num_tags_before) {
      setTag(i, "", "");
    } else {
      addTag();
    }
    // Remove extra tags.
    for (i++; i < num_tags_before; i++) {
      tagtable.removeRow(i + 1);
    }
    // Return the "foo" part of "foo{a=b,...,x=y}"
    return metric.substring(0, curly);
  }

  public void updateFromQueryString(final String m, final String o) {
    // TODO: Try to reduce code duplication with GraphHandler.parseQuery().
    // m is of the following forms:
    //  agg:[interval-agg:][rate[{counter[,max[,reset]]}:]metric[{tag=value,...}]
    // Where the parts in square brackets `[' .. `]' are optional.
    final String[] parts = m.split(":");
    final int nparts = parts.length;
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
    Object[] rate_options = parseRateOptions(rate, parts[i]);
    this.rate_counter.setValue((Boolean) rate_options[0], false);
    final long rate_counter_max = (Long) rate_options[1];
    this.counter_max.setValue(
        rate_counter_max == Long.MAX_VALUE ? "" : Long.toString(rate_counter_max), 
        false);
    this.counter_reset_value
        .setValue(Long.toString((Long) rate_options[2]), false);
    if (rate) {
      i--;
    }

    // downsampling function & interval.
    if (i > 0) {
      final int dash = parts[1].indexOf('-', 1);  // 1st char can't be `-'.
      if (dash < 0) {
        disableDownsample();
        return;  // Invalid downsampling specifier.
      }
      downsample.setValue(true, false);

      downsampler.setEnabled(true);
      setSelectedItem(downsampler, parts[1].substring(dash + 1));

      interval.setEnabled(true);
      interval.setText(parts[1].substring(0, dash));
    } else {
      disableDownsample();
    }

    x1y2.setValue(o.contains("axis x1y2"), false);
  }

  private void disableDownsample() {
    downsample.setValue(false, false);
    interval.setEnabled(false);
    downsampler.setEnabled(false);
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
        vbox.add(hbox);
      }
      add(vbox);
    }
  }

  public void setMetricChangeHandler(final MetricChangeHandler handler) {
    metric_change_handler = handler;
  }

  public void setAggregators(final ArrayList<String> aggs) {
    for (final String agg : aggs) {
      aggregators.addItem(agg);
      downsampler.addItem(agg);
    }
    setSelectedItem(aggregators, "sum");
    setSelectedItem(downsampler, "avg");
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
        .append('-').append(selectedValue(downsampler));
    }
    if (rate.getValue()) {
      url.append(":rate");
      if (rate_counter.getValue()) {
        url.append('{').append("counter");
        final String max = counter_max.getValue().trim();
        final String reset = counter_reset_value.getValue().trim();
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
    {
      final int ntags = getNumTags();
      url.append('{');
      for (int tag = 0; tag < ntags; tag++) {
        final String tagname = getTagName(tag);
        final String tagvalue = getTagValue(tag);
        if (tagname.isEmpty() || tagvalue.isEmpty()) {
          continue;
        }
        url.append(tagname).append('=').append(tagvalue)
          .append(',');
      }
      final int last = url.length() - 1;
      if (url.charAt(last) == '{') {  // There was no tag.
        url.setLength(last);          // So remove the `{'.
      } else {  // Need to replace the last `,' with a `}'.
        url.setCharAt(url.length() - 1, '}');
      }
    }
    url.append("&o=");
    if (x1y2.getValue()) {
      url.append("axis x1y2");
    }
    return true;
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

  private void setTagName(final int i, final String value) {
    ((SuggestBox) tagtable.getWidget(i + 1, 1)).setValue(value);
  }

  private void setTagValue(final int i, final String value) {
    ((SuggestBox) tagtable.getWidget(i + 1, 2)).setValue(value);
  }

  /**
   * Changes the name/value of an existing tag.
   * @param i The index of the tag to change.
   * @param name The new name of the tag.
   * @param value The new value of the tag.
   * Requires: {@code i < getNumTags()}.
   */
  private void setTag(final int i, final String name, final String value) {
    setTagName(i, name);
    setTagValue(i, value);
  }

  private void addTag() {
    addTag(null, null);
  }

  private void addTag(final String default_tagname) {
    addTag(default_tagname, null);
  }

  private void addTag(final String default_tagname,
                      final String default_value) {
    final int row = tagtable.getRowCount();

    final ValidatedTextBox tagname = new ValidatedTextBox();
    final SuggestBox suggesttagk = RemoteOracle.newSuggestBox("tagk", tagname);
    final ValidatedTextBox tagvalue = new ValidatedTextBox();
    final SuggestBox suggesttagv = RemoteOracle.newSuggestBox("tagv", tagvalue);
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
    setTag(0, "", "");
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
          setTag(tag - 1, tagname, tagvalue);
        }
        setTag(ntags - 1, "", "");
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
    interval.setEnabled(false);
    interval.setMaxLength(5);
    interval.setVisibleLength(5);
    interval.setValue("10m");
    interval.setValidationRegexp("^[1-9][0-9]*[smhdwy]$");
    downsample.addClickHandler(new ClickHandler() {
      public void onClick(final ClickEvent event) {
        final boolean checked = ((CheckBox) event.getSource()).getValue();
        downsampler.setEnabled(checked);
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

  static final public Object[] parseRateOptions(boolean rate, String spec) {
    if (!rate || spec.length() == 4) {
      return new Object[] { false, Long.MAX_VALUE, 0 };
    }

    if (spec.length() < 6) {
      return new Object[] { false, Long.MAX_VALUE, 0 };
    }

    String[] parts = spec.split(spec.substring(5, spec.length() - 1), ',');
    if (parts.length < 1 || parts.length > 3) {
      return new Object[] { false, Long.MAX_VALUE, 0 };
    }

    try {
      return new Object[] {
          "counter".equals(parts[0]),
          parts.length >= 2 && parts[1].length() > 0 ? Long.parseLong(parts[1])
              : Long.MAX_VALUE,
          parts.length >= 3 && parts[2].length() > 0 ? Long.parseLong(parts[2])
              : 0 };
    } catch (NumberFormatException e) {
      return new Object[] { false, Long.MAX_VALUE, 0 };
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
