// This file is part of OpenTSDB.
// Copyright (C) 2010  StumbleUpon, Inc.
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

import com.google.gwt.core.client.GWT;
import com.google.gwt.event.dom.client.BlurEvent;
import com.google.gwt.event.dom.client.BlurHandler;
import com.google.gwt.event.dom.client.ClickEvent;
import com.google.gwt.event.dom.client.ClickHandler;
import com.google.gwt.user.client.ui.Button;
import com.google.gwt.user.client.ui.CheckBox;
import com.google.gwt.user.client.ui.FlexTable;
import com.google.gwt.user.client.ui.HorizontalPanel;
import com.google.gwt.user.client.ui.InlineLabel;
import com.google.gwt.user.client.ui.ListBox;
import com.google.gwt.user.client.ui.SuggestBox;
import com.google.gwt.user.client.ui.VerticalPanel;
import com.google.gwt.user.client.ui.Widget;

final class MetricForm extends HorizontalPanel {

  private static final String TSDB_ID_CLASS = "[-_.a-zA-Z0-9]";
  private static final String TSDB_ID_RE = "^" + TSDB_ID_CLASS + "*$";
  private static final String TSDB_TAGVALUE_RE =
    "^(\\*?"                                       // a `*' wildcard or nothing
    + "|" + TSDB_ID_CLASS + "+(\\|" + TSDB_ID_CLASS + "+)*)$"; // `foo|bar|...'

  private final EventsHandler events_handler;

  private final CheckBox downsample = new CheckBox("Downsample");
  private final ListBox downsampler = new ListBox();
  private final ValidatedTextBox interval = new ValidatedTextBox();
  private final CheckBox rate = new CheckBox("Rate");
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
    aggregators.addChangeHandler(handler);
    metric.addBlurHandler(handler);
    metric.addKeyPressHandler(handler);

    metric.setValidationRegexp(TSDB_ID_RE);
    assembleUi();
  }

  public String getMetric() {
    return metric.getText();
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
      addTag(null);
      tagtable.setText(1, 0, "Tags");
      add(tagtable);
    }
    {  // Right hand-side panel.
      final VerticalPanel vbox = new VerticalPanel();
      vbox.add(rate);
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
    url.append("&m=").append(metric);
    if (downsample.getValue()) {
      url.append("&df=").append(selectedValue(downsampler))
        .append("&di=").append(interval.getValue());
    }
    if (rate.getValue()) {
      url.append("&rate");
    }
    url.append("&af=").append(selectedValue(aggregators));
    final int ntags = getNumTags();
    for (int tag = 0; tag < ntags; tag++) {
      final String tagname = getTagName(tag);
      final String tagvalue = getTagValue(tag);
      if (tagname.isEmpty() || tagvalue.isEmpty()) {
        continue;
      }
      url.append("&tag=").append(tagname)
        .append('=').append(tagvalue);
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

  private void addTag(final String default_tagname) {
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
      tagvalue.setFocus(true);
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
          setTagName(tag - 1, tagname);
          setTagValue(tag - 1, tagvalue);
        }
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
        addTag(null);
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

}
