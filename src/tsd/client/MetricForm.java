// This file is part of OpenTSDB.
// Copyright (C) 2010  The OpenTSDB Authors.
//
// This program is free software: you can redistribute it and/or modify it
// under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or (at your
// option) any later version.  This program is distributed in the hope that it
// will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty
// of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser
// General Public License for more details.  You should have received a copy
// of the GNU Lesser General Public License along with this program.  If not,
// see <http://www.gnu.org/licenses/>.
package tsd.client;

import java.util.ArrayList;
import java.util.Map;

import com.google.gwt.event.dom.client.ClickEvent;
import com.google.gwt.event.dom.client.ClickHandler;
import com.google.gwt.event.dom.client.DomEvent;
import com.google.gwt.event.shared.EventHandler;
import com.google.gwt.user.client.ui.CheckBox;
import com.google.gwt.user.client.ui.Focusable;
import com.google.gwt.user.client.ui.HorizontalPanel;
import com.google.gwt.user.client.ui.InlineLabel;
import com.google.gwt.user.client.ui.ListBox;
import com.google.gwt.user.client.ui.SuggestBox;
import com.google.gwt.user.client.ui.VerticalPanel;

final class MetricForm extends HorizontalPanel implements Focusable {

  public static interface MetricChangeHandler extends EventHandler {
    void onMetricChange(MetricForm widget);
  }

  private MetricChangeHandler metric_change_handler;

  private final CheckBox downsample = new CheckBox("Downsample");
  private final ListBox downsampler = new ListBox();
  private final ValidatedTextBox interval = new ValidatedTextBox();
  private final CheckBox rate = new CheckBox("Rate");
  private final CheckBox x1y2 = new CheckBox("Right Axis");
  private final ListBox aggregators = new ListBox();
  private final ValidatedTextBox metric = new ValidatedTextBox();
  private TagsPanel tagsPanel;

  public MetricForm(final EventsHandler handler) {
    setupDownsampleWidgets();
    tagsPanel = new TagsPanel(handler);
    downsample.addClickHandler(handler);
    downsampler.addChangeHandler(handler);
    interval.addBlurHandler(handler);
    interval.addKeyPressHandler(handler);
    rate.addClickHandler(handler);
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

    metric.setValidationRegexp(ClientConstants.TSDB_ID_RE);
    assembleUi();
  }

  public String getMetric() {
    return metric.getText();
  }

  public CheckBox x1y2() {
    return x1y2;
  }
  
  public void autoSuggestTag(String tag) {
    tagsPanel.autoSuggestTag(tag);
  }

  private void assembleUi() {
    setWidth("100%");
    {  // Left hand-side panel.
      final VerticalPanel leftPanel = new VerticalPanel();
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

      leftPanel.add(hbox);
      leftPanel.add(tagsPanel);
      add(leftPanel);
    }
    {  // Right hand-side panel.
      final VerticalPanel vbox = new VerticalPanel();
      {
        final HorizontalPanel hbox = new HorizontalPanel();
        hbox.add(rate);
        hbox.add(x1y2);
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
    }
    url.append(':').append(metric);
    {
      final Map<String, String> tags = tagsPanel.getTags();
      url.append('{');
      for (Map.Entry<String, String> tag : tags.entrySet()) {
        final String tagname = tag.getKey();
        final String tagvalue = tag.getValue();
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
