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

/*
 * DISCLAIMER
 * This my first GWT code ever, so it's most likely horribly wrong as I've had
 * virtually no exposure to the technology except through the tutorial. --tsuna
 */

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import net.opentsdb.graph.Plot;

import com.google.gwt.core.client.EntryPoint;
import com.google.gwt.dom.client.Style;
import com.google.gwt.dom.client.Style.Cursor;
import com.google.gwt.event.dom.client.ClickEvent;
import com.google.gwt.event.dom.client.ClickHandler;
import com.google.gwt.event.dom.client.DomEvent;
import com.google.gwt.event.dom.client.ErrorEvent;
import com.google.gwt.event.dom.client.ErrorHandler;
import com.google.gwt.event.dom.client.LoadEvent;
import com.google.gwt.event.dom.client.LoadHandler;
import com.google.gwt.event.dom.client.MouseDownEvent;
import com.google.gwt.event.dom.client.MouseDownHandler;
import com.google.gwt.event.dom.client.MouseEvent;
import com.google.gwt.event.dom.client.MouseMoveEvent;
import com.google.gwt.event.dom.client.MouseMoveHandler;
import com.google.gwt.event.dom.client.MouseOutEvent;
import com.google.gwt.event.dom.client.MouseOutHandler;
import com.google.gwt.event.dom.client.MouseOverEvent;
import com.google.gwt.event.dom.client.MouseOverHandler;
import com.google.gwt.event.dom.client.MouseUpEvent;
import com.google.gwt.event.dom.client.MouseUpHandler;
import com.google.gwt.event.logical.shared.BeforeSelectionEvent;
import com.google.gwt.event.logical.shared.BeforeSelectionHandler;
import com.google.gwt.event.logical.shared.ValueChangeEvent;
import com.google.gwt.event.logical.shared.ValueChangeHandler;
import com.google.gwt.event.shared.EventHandler;
import com.google.gwt.event.shared.HandlerRegistration;
import com.google.gwt.http.client.Request;
import com.google.gwt.http.client.RequestBuilder;
import com.google.gwt.http.client.RequestCallback;
import com.google.gwt.http.client.RequestException;
import com.google.gwt.http.client.Response;
import com.google.gwt.http.client.URL;
import com.google.gwt.i18n.client.DateTimeFormat;
import com.google.gwt.json.client.JSONArray;
import com.google.gwt.json.client.JSONNumber;
import com.google.gwt.json.client.JSONObject;
import com.google.gwt.json.client.JSONParser;
import com.google.gwt.json.client.JSONString;
import com.google.gwt.json.client.JSONValue;
import com.google.gwt.user.client.Element;
import com.google.gwt.user.client.History;
import com.google.gwt.user.client.HistoryListener;
import com.google.gwt.user.client.Timer;
import com.google.gwt.user.client.Window;
import com.google.gwt.user.client.ui.AbsolutePanel;
import com.google.gwt.user.client.ui.Anchor;
import com.google.gwt.user.client.ui.CheckBox;
import com.google.gwt.user.client.ui.DecoratedTabPanel;
import com.google.gwt.user.client.ui.DecoratorPanel;
import com.google.gwt.user.client.ui.DisclosurePanel;
import com.google.gwt.user.client.ui.FlexTable;
import com.google.gwt.user.client.ui.Grid;
import com.google.gwt.user.client.ui.HTML;
import com.google.gwt.user.client.ui.HorizontalPanel;
import com.google.gwt.user.client.ui.Image;
import com.google.gwt.user.client.ui.InlineLabel;
import com.google.gwt.user.client.ui.Label;
import com.google.gwt.user.client.ui.ListBox;
import com.google.gwt.user.client.ui.RadioButton;
import com.google.gwt.user.client.ui.RootPanel;
import com.google.gwt.user.client.ui.TextBox;
import com.google.gwt.user.client.ui.VerticalPanel;
import com.google.gwt.user.client.ui.Widget;

/**
 * Root class for the 'query UI'.
 * Manages the entire UI, forms to query the TSDB and other misc panels.
 */
public class QueryUi implements EntryPoint, HistoryListener {

  /** Map of available gnuplot data styles. */
  public static Map<String, Integer> stylesMap = new HashMap<String, Integer>();
  static {
      Map<String, Integer> map = new HashMap<String, Integer>();
      map.put("linespoint", 0);
      map.put("points", 1);
      map.put("circles", 3);
      map.put("dots", 4);
      stylesMap = Collections.unmodifiableMap(map);
  }

  // Some URLs we use to fetch data from the TSD.
  private static final String AGGREGATORS_URL = "aggregators";
  private static final String LOGS_URL = "logs?json";
  private static final String STATS_URL = "stats?json";
  private static final String VERSION_URL = "version?json";

  private static final DateTimeFormat FULLDATE =
    DateTimeFormat.getFormat("yyyy/MM/dd-HH:mm:ss");

  private final Label current_error = new Label();

  private final DateTimeBox start_datebox = new DateTimeBox();
  private final DateTimeBox end_datebox = new DateTimeBox();
  private final CheckBox autoreload = new CheckBox("Autoreload");
  private final ValidatedTextBox autoreoload_interval = new ValidatedTextBox();
  private Timer autoreoload_timer;

  private final ValidatedTextBox yrange = new ValidatedTextBox();
  private final ValidatedTextBox y2range = new ValidatedTextBox();
  private final CheckBox ylog = new CheckBox();
  private final CheckBox y2log = new CheckBox();
  private final TextBox ylabel = new TextBox();
  private final TextBox y2label = new TextBox();
  private final ValidatedTextBox yformat = new ValidatedTextBox();
  private final ValidatedTextBox y2format = new ValidatedTextBox();
  private final ValidatedTextBox wxh = new ValidatedTextBox();
  private final CheckBox global_annotations = new CheckBox("Global annotations");

  private String keypos = "";  // Position of the key on the graph.
  private final CheckBox horizontalkey = new CheckBox("Horizontal layout");
  private final CheckBox keybox = new CheckBox("Box");
  private final CheckBox nokey = new CheckBox("No key (overrides others)");

  // Styling options.
  private final CheckBox smooth = new CheckBox();
  private final ListBox styles = new ListBox();
  private String timezone = "";

  /**
   * Handles every change to the query form and gets a new graph.
   * Whenever the user changes one of the parameters of the graph, we want
   * to automatically get a new graph.
   */
  private final EventsHandler refreshgraph = new EventsHandler() {
    protected <H extends EventHandler> void onEvent(final DomEvent<H> event) {
      refreshGraph();
    }
  };

  final MetricForm.MetricChangeHandler metric_change_handler =
    new MetricForm.MetricChangeHandler() {
      public void onMetricChange(final MetricForm metric) {
        final int index = metrics.getWidgetIndex(metric);
        metrics.getTabBar().setTabText(index, getTabTitle(metric));
      }
      private String getTabTitle(final MetricForm metric) {
        final String metrictext = metric.getMetric();
        final int last_period = metrictext.lastIndexOf('.');
        if (last_period < 0) {
          return metrictext;
        }
        return metrictext.substring(last_period + 1);
      }
    };

  final EventsHandler updatey2range = new EventsHandler() {
      protected <H extends EventHandler> void onEvent(final DomEvent<H> event) {
        for (final Widget metric : metrics) {
          if (!(metric instanceof MetricForm)) {
            continue;
          }
          if (((MetricForm) metric).x1y2().getValue()) {
            y2range.setEnabled(true);
            y2log.setEnabled(true);
            y2label.setEnabled(true);
            y2format.setEnabled(true);
            return;
          }
        }
        y2range.setEnabled(false);
        y2log.setEnabled(false);
        y2label.setEnabled(false);
        y2format.setEnabled(false);
      }
    };

  /** List of known aggregation functions.  Fetched once from the server. */
  private final ArrayList<String> aggregators = new ArrayList<String>();
  
  /**
   * List of known downsampling fill policies.
   * TODO: fetch from server.
   */
  private final List<String> fill_policies = Arrays.asList("none", "nan",
    "zero", "null");
  
  private final DecoratedTabPanel metrics = new DecoratedTabPanel();

  /** Panel to place generated graphs and a box for zoom highlighting. */
  private final AbsolutePanel graphbox = new AbsolutePanel();
  private final Image graph = new Image();
  private final ZoomBox zoom_box = new ZoomBox();
  private final Label graphstatus = new Label();
  /** Remember the last URI requested to avoid requesting twice the same. */
  private String lastgraphuri;

  /**
   * We only send one request at a time, how many have we not sent yet?.
   * Note that we don't buffer pending requests.  When there are multiple
   * ones pending, we will only execute the last one and discard the other
   * intermediate ones, since the user is no longer interested in them.
   */
  private int pending_requests = 0;
  /** How many graph requests we make.  */
  private int nrequests = 0;

  // Other misc panels.
  private final FlexTable logs = new FlexTable();
  private final FlexTable stats_table = new FlexTable();
  private final HTML build_data = new HTML("Loading...");

  /**
   * This is the entry point method.
   */
  public void onModuleLoad() {
    asyncGetJson(AGGREGATORS_URL, new GotJsonCallback() {
      public void got(final JSONValue json) {
        // Do we need more manual type checking?  Not sure what will happen
        // in the browser if something other than an array is returned.
        final JSONArray aggs = json.isArray();
        for (int i = 0; i < aggs.size(); i++) {
          aggregators.add(aggs.get(i).isString().stringValue());
        }
        ((MetricForm) metrics.getWidget(0)).setAggregators(aggregators);
        refreshFromQueryString();
        refreshGraph();
      }
    });

    // All UI elements need to regenerate the graph when changed.
    {
      final ValueChangeHandler<Date> vch = new ValueChangeHandler<Date>() {
        public void onValueChange(final ValueChangeEvent<Date> event) {
          refreshGraph();
        }
      };
      TextBox tb = start_datebox.getTextBox();
      tb.addBlurHandler(refreshgraph);
      tb.addKeyPressHandler(refreshgraph);
      start_datebox.addValueChangeHandler(vch);
      tb = end_datebox.getTextBox();
      tb.addBlurHandler(refreshgraph);
      tb.addKeyPressHandler(refreshgraph);
      end_datebox.addValueChangeHandler(vch);
    }
    autoreoload_interval.addBlurHandler(refreshgraph);
    autoreoload_interval.addKeyPressHandler(refreshgraph);
    yrange.addBlurHandler(refreshgraph);
    yrange.addKeyPressHandler(refreshgraph);
    y2range.addBlurHandler(refreshgraph);
    y2range.addKeyPressHandler(refreshgraph);
    ylog.addClickHandler(new AdjustYRangeCheckOnClick(ylog, yrange));
    y2log.addClickHandler(new AdjustYRangeCheckOnClick(y2log, y2range));
    ylog.addClickHandler(refreshgraph);
    y2log.addClickHandler(refreshgraph);
    ylabel.addBlurHandler(refreshgraph);
    ylabel.addKeyPressHandler(refreshgraph);
    y2label.addBlurHandler(refreshgraph);
    y2label.addKeyPressHandler(refreshgraph);
    yformat.addBlurHandler(refreshgraph);
    yformat.addKeyPressHandler(refreshgraph);
    y2format.addBlurHandler(refreshgraph);
    y2format.addKeyPressHandler(refreshgraph);
    wxh.addBlurHandler(refreshgraph);
    wxh.addKeyPressHandler(refreshgraph);
    global_annotations.addBlurHandler(refreshgraph);
    global_annotations.addKeyPressHandler(refreshgraph);
    horizontalkey.addClickHandler(refreshgraph);
    keybox.addClickHandler(refreshgraph);
    nokey.addClickHandler(refreshgraph);
    smooth.addClickHandler(refreshgraph);
    styles.addChangeHandler(refreshgraph);

    yrange.setValidationRegexp("^("                            // Nothing or
                               + "|\\[([-+.0-9eE]+|\\*)?"      // "[start
                               + ":([-+.0-9eE]+|\\*)?\\])$");  //   :end]"
    yrange.setVisibleLength(5);
    yrange.setMaxLength(44);  // MAX=2^26=20 chars: "[-$MAX:$MAX]"
    yrange.setText("[0:]");

    y2range.setValidationRegexp("^("                            // Nothing or
                                + "|\\[([-+.0-9eE]+|\\*)?"      // "[start
                                + ":([-+.0-9eE]+|\\*)?\\])$");  //   :end]"
    y2range.setVisibleLength(5);
    y2range.setMaxLength(44);  // MAX=2^26=20 chars: "[-$MAX:$MAX]"
    y2range.setText("[0:]");
    y2range.setEnabled(false);
    y2log.setEnabled(false);

    ylabel.setVisibleLength(10);
    ylabel.setMaxLength(50);  // Arbitrary limit.
    y2label.setVisibleLength(10);
    y2label.setMaxLength(50);  // Arbitrary limit.
    y2label.setEnabled(false);

    yformat.setValidationRegexp("^(|.*%..*)$");  // Nothing or at least one %?
    yformat.setVisibleLength(10);
    yformat.setMaxLength(16);  // Arbitrary limit.
    y2format.setValidationRegexp("^(|.*%..*)$");  // Nothing or at least one %?
    y2format.setVisibleLength(10);
    y2format.setMaxLength(16);  // Arbitrary limit.
    y2format.setEnabled(false);

    wxh.setValidationRegexp("^[1-9][0-9]{2,}x[1-9][0-9]{2,}$");  // 100x100
    wxh.setVisibleLength(9);
    wxh.setMaxLength(11);  // 99999x99999
    wxh.setText((Window.getClientWidth() - 20) + "x"
                + (Window.getClientHeight() * 4 / 5));

    final FlexTable table = new FlexTable();
    table.setText(0, 0, "From");
    {
      final HorizontalPanel hbox = new HorizontalPanel();
      hbox.add(new InlineLabel("To"));
      final Anchor now = new Anchor("(now)");
      now.addClickHandler(new ClickHandler() {
        public void onClick(final ClickEvent event) {
          end_datebox.setValue(new Date());
          refreshGraph();
        }
      });
      hbox.add(now);
      hbox.add(autoreload);
      hbox.setWidth("100%");
      table.setWidget(0, 1, hbox);
    }
    autoreload.addValueChangeHandler(new ValueChangeHandler<Boolean>() {
      @Override
      public void onValueChange(final ValueChangeEvent<Boolean> event) {
        if (autoreload.getValue()) {
          final HorizontalPanel hbox = new HorizontalPanel();
          hbox.setWidth("100%");
          hbox.add(new InlineLabel("Every:"));
          hbox.add(autoreoload_interval);
          hbox.add(new InlineLabel("seconds"));
          table.setWidget(1, 1, hbox);
          if (autoreoload_interval.getValue().isEmpty()) {
            autoreoload_interval.setValue("15");
          }
          autoreoload_interval.setFocus(true);
          lastgraphuri = "";  // Force refreshGraph.
          refreshGraph();     // Trigger the 1st auto-reload
        } else {
          table.setWidget(1, 1, end_datebox);
        }
      }
    });
    autoreoload_interval.setValidationRegexp("^([5-9]|[1-9][0-9]+)$");  // >=5s
    autoreoload_interval.setMaxLength(4);
    autoreoload_interval.setVisibleLength(8);

    table.setWidget(1, 0, start_datebox);
    table.setWidget(1, 1, end_datebox);
    {
      final HorizontalPanel hbox = new HorizontalPanel();
      hbox.add(new InlineLabel("WxH:"));
      hbox.add(wxh);
      table.setWidget(0, 3, hbox);
    }
    {
      final HorizontalPanel hbox = new HorizontalPanel();
      hbox.add(global_annotations);
      table.setWidget(0, 4, hbox);
    }
    {
      addMetricForm("metric 1", 0);
      metrics.selectTab(0);
      metrics.add(new InlineLabel("Loading..."), "+");
      metrics.addBeforeSelectionHandler(new BeforeSelectionHandler<Integer>() {
        public void onBeforeSelection(final BeforeSelectionEvent<Integer> event) {
          final int item = event.getItem();
          final int nitems = metrics.getWidgetCount();
          if (item == nitems - 1) {  // Last item: the "+" was clicked.
            event.cancel();
            final MetricForm metric = addMetricForm("metric " + nitems, item);
            metrics.selectTab(item);
            metric.setFocus(true);
          }
        }
      });
      table.setWidget(2, 0, metrics);
    }
    table.getFlexCellFormatter().setColSpan(2, 0, 2);
    table.getFlexCellFormatter().setRowSpan(1, 3, 2);
    final DecoratedTabPanel optpanel = new DecoratedTabPanel();
    optpanel.add(makeAxesPanel(), "Axes");
    optpanel.add(makeKeyPanel(), "Key");
    optpanel.add(makeStylePanel(), "Style");
    optpanel.selectTab(0);
    table.setWidget(1, 3, optpanel);
    table.getFlexCellFormatter().setColSpan(1, 3, 2);

    final DecoratorPanel decorator = new DecoratorPanel();
    decorator.setWidget(table);
    final VerticalPanel graphpanel = new VerticalPanel();
    graphpanel.add(decorator);
    {
      final VerticalPanel graphvbox = new VerticalPanel();
      graphvbox.add(graphstatus);

      graph.setVisible(false);

      // Put the graph image element and the zoombox elements inside the absolute panel
      graphbox.add(graph, 0, 0);
      zoom_box.setVisible(false);
      graphbox.add(zoom_box, 0, 0);
      graph.addMouseOverHandler(new MouseOverHandler() {
        public void onMouseOver(final MouseOverEvent event) {
          final Style style = graphbox.getElement().getStyle();
          style.setCursor(Cursor.CROSSHAIR);
        }
      });
      graph.addMouseOutHandler(new MouseOutHandler() {
        public void onMouseOut(final MouseOutEvent event) {
          final Style style = graphbox.getElement().getStyle();
          style.setCursor(Cursor.AUTO);
        }
      });

      graphvbox.add(graphbox);
      graph.addErrorHandler(new ErrorHandler() {
        public void onError(final ErrorEvent event) {
          graphstatus.setText("Oops, failed to load the graph.");
        }
      });
      graph.addLoadHandler(new LoadHandler() {
        public void onLoad(final LoadEvent event) {
          graphbox.setWidth(graph.getWidth() + "px");
          graphbox.setHeight(graph.getHeight() + "px");
        }
      });

      graphpanel.add(graphvbox);
    }
    final DecoratedTabPanel mainpanel = new DecoratedTabPanel();
    mainpanel.setWidth("100%");
    mainpanel.add(graphpanel, "Graph");
    mainpanel.add(stats_table, "Stats");
    mainpanel.add(logs, "Logs");
    mainpanel.add(build_data, "Version");
    mainpanel.selectTab(0);
    mainpanel.addBeforeSelectionHandler(new BeforeSelectionHandler<Integer>() {
      public void onBeforeSelection(final BeforeSelectionEvent<Integer> event) {
        clearError();
        final int item = event.getItem();
        switch (item) {
          case 1: refreshStats(); return;
          case 2: refreshLogs(); return;
          case 3: refreshVersion(); return;
        }
      }
    });
    final VerticalPanel root = new VerticalPanel();
    root.setWidth("100%");
    root.add(current_error);
    current_error.setVisible(false);
    current_error.addStyleName("dateBoxFormatError");
    root.add(mainpanel);
    RootPanel.get("queryuimain").add(root);
    // Must be done at the end, once all the widgets are attached.
    ensureSameWidgetSize(optpanel);

    History.addHistoryListener(this);
  }

  @Override
  public void onHistoryChanged(String historyToken) {
    refreshFromQueryString();
    refreshGraph();
  }

  /** Additional styling options.  */
  private Grid makeStylePanel() {
    for (Entry<String, Integer> item : stylesMap.entrySet()) {
      styles.insertItem(item.getKey(), item.getValue());
    }
    final Grid grid = new Grid(5, 3);
    grid.setText(0, 1, "Smooth");
    grid.setWidget(0, 2, smooth);
    grid.setText(1, 1, "Style");
    grid.setWidget(1, 2, styles);
    return grid;
  }

  /**
   * Builds the panel containing customizations for the axes of the graph.
   */
  private Grid makeAxesPanel() {
    final Grid grid = new Grid(5, 3);
    grid.setText(0, 1, "Y");
    grid.setText(0, 2, "Y2");
    setTextAlignCenter(grid.getRowFormatter().getElement(0));
    grid.setText(1, 0, "Label");
    grid.setWidget(1, 1, ylabel);
    grid.setWidget(1, 2, y2label);
    grid.setText(2, 0, "Format");
    grid.setWidget(2, 1, yformat);
    grid.setWidget(2, 2, y2format);
    grid.setText(3, 0, "Range");
    grid.setWidget(3, 1, yrange);
    grid.setWidget(3, 2, y2range);
    grid.setText(4, 0, "Log scale");
    grid.setWidget(4, 1, ylog);
    grid.setWidget(4, 2, y2log);
    setTextAlignCenter(grid.getCellFormatter().getElement(4, 1));
    setTextAlignCenter(grid.getCellFormatter().getElement(4, 2));
    return grid;
  }

  private MetricForm addMetricForm(final String label, final int item) {
    final MetricForm metric = new MetricForm(refreshgraph);
    metric.x1y2().addClickHandler(updatey2range);
    metric.setMetricChangeHandler(metric_change_handler);
    metric.setAggregators(aggregators);
    metric.setFillPolicies(fill_policies);
    metrics.insert(metric, label, item);
    return metric;
  }

  private final HashMap<String, RadioButton> keypos_map =
    new HashMap<String, RadioButton>(17);

  /**
   * Small helper to build a radio button used to change the position of the
   * key of the graph.
   */
  private RadioButton addKeyRadioButton(final Grid grid,
                                        final int row, final int col,
                                        final String pos) {
    final RadioButton rb = new RadioButton("keypos");
    rb.addClickHandler(new ClickHandler() {
      public void onClick(final ClickEvent event) {
        keypos = pos;
      }
    });
    rb.addClickHandler(refreshgraph);
    grid.setWidget(row, col, rb);
    keypos_map.put(pos, rb);
    return rb;
  }

  /**
   * Builds the panel containing the customizations for the key of the graph.
   */
  private Widget makeKeyPanel() {
    final Grid grid = new Grid(5, 5);
    addKeyRadioButton(grid, 0, 0, "out left top");
    addKeyRadioButton(grid, 0, 2, "out center top");
    addKeyRadioButton(grid, 0, 4, "out right top");
    addKeyRadioButton(grid, 1, 1, "top left");
    addKeyRadioButton(grid, 1, 2, "top center");
    addKeyRadioButton(grid, 1, 3, "top right").setValue(true);
    addKeyRadioButton(grid, 2, 0, "out center left");
    addKeyRadioButton(grid, 2, 1, "center left");
    addKeyRadioButton(grid, 2, 2, "center");
    addKeyRadioButton(grid, 2, 3, "center right");
    addKeyRadioButton(grid, 2, 4, "out center right");
    addKeyRadioButton(grid, 3, 1, "bottom left");
    addKeyRadioButton(grid, 3, 2, "bottom center");
    addKeyRadioButton(grid, 3, 3, "bottom right");
    addKeyRadioButton(grid, 4, 0, "out bottom left");
    addKeyRadioButton(grid, 4, 2, "out bottom center");
    addKeyRadioButton(grid, 4, 4, "out bottom right");
    final Grid.CellFormatter cf = grid.getCellFormatter();
    cf.getElement(1, 1).getStyle().setProperty("borderLeft", "1px solid #000");
    cf.getElement(1, 1).getStyle().setProperty("borderTop", "1px solid #000");
    cf.getElement(1, 2).getStyle().setProperty("borderTop", "1px solid #000");
    cf.getElement(1, 3).getStyle().setProperty("borderTop", "1px solid #000");
    cf.getElement(1, 3).getStyle().setProperty("borderRight", "1px solid #000");
    cf.getElement(2, 1).getStyle().setProperty("borderLeft", "1px solid #000");
    cf.getElement(2, 3).getStyle().setProperty("borderRight", "1px solid #000");
    cf.getElement(3, 1).getStyle().setProperty("borderLeft", "1px solid #000");
    cf.getElement(3, 1).getStyle().setProperty("borderBottom", "1px solid #000");
    cf.getElement(3, 2).getStyle().setProperty("borderBottom", "1px solid #000");
    cf.getElement(3, 3).getStyle().setProperty("borderBottom", "1px solid #000");
    cf.getElement(3, 3).getStyle().setProperty("borderRight", "1px solid #000");
    final VerticalPanel vbox = new VerticalPanel();
    vbox.add(new InlineLabel("Key location:"));
    vbox.add(grid);
    vbox.add(horizontalkey);
    keybox.setValue(true);
    vbox.add(keybox);
    vbox.add(nokey);
    return vbox;
  }

  private void refreshStats() {
    asyncGetJson(STATS_URL, new GotJsonCallback() {
      public void got(final JSONValue json) {
        final JSONArray stats = json.isArray();
        final int nstats = stats.size();
        for (int i = 0; i < nstats; i++) {
          final String stat = stats.get(i).isString().stringValue();
          String part = stat.substring(0, stat.indexOf(' '));
          stats_table.setText(i, 0, part);  // metric
          int pos = part.length() + 1;
          part = stat.substring(pos, stat.indexOf(' ', pos));
          stats_table.setText(i, 1, part);  // timestamp
          pos += part.length() + 1;
          part = stat.substring(pos, stat.indexOf(' ', pos));
          stats_table.setText(i, 2, part);  // value
          pos += part.length() + 1;
          stats_table.setText(i, 3, stat.substring(pos));  // tags
        }
      }
    });
  }

  private void refreshVersion() {
    asyncGetJson(VERSION_URL, new GotJsonCallback() {
      public void got(final JSONValue json) {
        final JSONObject bd = json.isObject();
        final JSONString shortrev = bd.get("short_revision").isString();
        final JSONString status = bd.get("repo_status").isString();
        final JSONString stamp = bd.get("timestamp").isString();
        final JSONString user = bd.get("user").isString();
        final JSONString host = bd.get("host").isString();
        final JSONString repo = bd.get("repo").isString();
        final JSONString version = bd.get("version").isString();
        build_data.setHTML(
          "OpenTSDB version [" + version.stringValue() + "] built from revision " 
          + shortrev.stringValue()
          + " in a " + status.stringValue() + " state<br/>"
          + "Built on " + new Date((Long.parseLong(stamp.stringValue()) * 1000))
          + " by " + user.stringValue() + '@' + host.stringValue()
          + ':' + repo.stringValue());
      }
    });
  }

  private void refreshLogs() {
    asyncGetJson(LOGS_URL, new GotJsonCallback() {
      public void got(final JSONValue json) {
        final JSONArray logmsgs = json.isArray();
        final int nmsgs = logmsgs.size();
        final FlexTable.FlexCellFormatter fcf = logs.getFlexCellFormatter();
        final FlexTable.RowFormatter rf = logs.getRowFormatter();
        for (int i = 0; i < nmsgs; i++) {
          final String msg = logmsgs.get(i).isString().stringValue();
          String part = msg.substring(0, msg.indexOf('\t'));
          logs.setText(i * 2, 0,
                       new Date(Integer.valueOf(part) * 1000L).toString());
          logs.setText(i * 2 + 1, 0, "");  // So we can change the style ahead.
          int pos = part.length() + 1;
          part = msg.substring(pos, msg.indexOf('\t', pos));
          if ("WARN".equals(part)) {
            rf.getElement(i * 2).getStyle().setBackgroundColor("#FCC");
            rf.getElement(i * 2 + 1).getStyle().setBackgroundColor("#FCC");
          } else if ("ERROR".equals(part)) {
            rf.getElement(i * 2).getStyle().setBackgroundColor("#F99");
            rf.getElement(i * 2 + 1).getStyle().setBackgroundColor("#F99");
          } else {
            rf.getElement(i * 2).getStyle().clearBackgroundColor();
            rf.getElement(i * 2 + 1).getStyle().clearBackgroundColor();
            if ((i % 2) == 0) {
              rf.addStyleName(i * 2, "subg");
              rf.addStyleName(i * 2 + 1, "subg");
            }
          }
          pos += part.length() + 1;
          logs.setText(i * 2, 1, part); // level
          part = msg.substring(pos, msg.indexOf('\t', pos));
          pos += part.length() + 1;
          logs.setText(i * 2, 2, part); // thread
          part = msg.substring(pos, msg.indexOf('\t', pos));
          pos += part.length() + 1;
          if (part.startsWith("net.opentsdb.")) {
            part = part.substring(13);
          } else if (part.startsWith("org.hbase.")) {
            part = part.substring(10);
          }
          logs.setText(i * 2, 3, part); // logger
          logs.setText(i * 2 + 1, 0, msg.substring(pos)); // message
          fcf.setColSpan(i * 2 + 1, 0, 4);
          rf.addStyleName(i * 2, "fwf");
          rf.addStyleName(i * 2 + 1, "fwf");
        }
      }
    });
  }

  private void addLabels(final StringBuilder url) {
    final String ylabel = this.ylabel.getText();
    if (!ylabel.isEmpty()) {
      url.append("&ylabel=").append(ylabel);
    }
    if (y2label.isEnabled()) {
      final String y2label = this.y2label.getText();
      if (!y2label.isEmpty()) {
        url.append("&y2label=").append(y2label);
      }
    }
  }

  private void addFormats(final StringBuilder url) {
    final String yformat = this.yformat.getText();
    if (!yformat.isEmpty()) {
      url.append("&yformat=").append(yformat);
    }
    if (y2format.isEnabled()) {
      final String y2format = this.y2format.getText();
      if (!y2format.isEmpty()) {
        url.append("&y2format=").append(y2format);
      }
    }
  }

  private void addYRanges(final StringBuilder url) {
    final String yrange = this.yrange.getText();
    if (!yrange.isEmpty()) {
      url.append("&yrange=").append(yrange);
    }
    if (y2range.isEnabled()) {
      final String y2range = this.y2range.getText();
      if (!y2range.isEmpty()) {
        url.append("&y2range=").append(y2range);
      }
    }
  }

  private void addLogscales(final StringBuilder url) {
    if (ylog.getValue()) {
      url.append("&ylog");
    }
    if (y2log.isEnabled() && y2log.getValue()) {
      url.append("&y2log");
    }
  }

  /**
   * Maybe sets the text of a {@link TextBox} from a query string parameter.
   * @param qs A parsed query string.
   * @param key Name of the query string parameter.
   * If this parameter wasn't passed, the {@link TextBox} will be emptied.
   * @param tb The {@link TextBox} to change.
   */
  private static void maybeSetTextbox(final QueryString qs,
                                      final String key,
                                      final TextBox tb) {
    final ArrayList<String> values = qs.get(key);
    if (values == null) {
      tb.setText("");
      return;
    }
    tb.setText(values.get(0));
  }

  /**
   * Sets the text of a {@link TextBox} from a query string parameter.
   * @param qs A parsed query string.
   * @param key Name of the query string parameter.
   * @param tb The {@link TextBox} to change.
   */
  private static void setTextbox(final QueryString qs,
                                 final String key,
                                 final TextBox tb) {
    final ArrayList<String> values = qs.get(key);
    if (values != null) {
      tb.setText(values.get(0));
    }
  }

  private static QueryString getQueryString(final String qs) {
    return qs.isEmpty() ? new QueryString() : QueryString.decode(qs);
  }

  private void refreshFromQueryString() {
    final QueryString qs = getQueryString(URL.decode(History.getToken()));

    maybeSetTextbox(qs, "start", start_datebox.getTextBox());
    maybeSetTextbox(qs, "end", end_datebox.getTextBox());
    setTextbox(qs, "wxh", wxh);
    global_annotations.setValue(qs.containsKey("global_annotations"));
    autoreload.setValue(qs.containsKey("autoreload"), true);
    maybeSetTextbox(qs, "autoreload", autoreoload_interval);

    //get the tz param value
    final ArrayList<String> tzvalues = qs.get("tz");
    if (tzvalues == null)
      timezone = "";
    else
      timezone = tzvalues.get(0);

    final ArrayList<String> newmetrics = qs.get("m");
    if (newmetrics == null) {  // Clear all metric forms.
      final int toremove = metrics.getWidgetCount() - 1;
      addMetricForm("metric 1", 0);
      metrics.selectTab(0);
      for (int i = 0; i < toremove; i++) {
        metrics.remove(1);
      }
      return;
    }
    final int n = newmetrics.size();  // We want this many metrics.
    ArrayList<String> options = qs.get("o");
    if (options == null) {
      options = new ArrayList<String>(n);
    }
    for (int i = options.size(); i < n; i++) {  // Make both arrays equal size.
      options.add("");  // Add missing o's.
    }

    for (int i = 0; i < newmetrics.size(); ++i) {
      if (i == metrics.getWidgetCount() - 1) {
        addMetricForm("", i);
      }

      final MetricForm metric = (MetricForm) metrics.getWidget(i);
      metric.updateFromQueryString(newmetrics.get(i), options.get(i));
    }
    // Remove extra metric forms.
    final int m = metrics.getWidgetCount() - 1; // We have this many metrics.
    int showing = metrics.getTabBar().getSelectedTab();  // Currently selected.
    for (int i = m - 1; i >= n; i--) {
      if (showing == i) {  // If we're about to remove the currently selected,
        metrics.selectTab(--showing);  // fix focus to not wind up nowhere.
      }
      metrics.remove(i);
    }
    updatey2range.onEvent(null);

    maybeSetTextbox(qs, "ylabel", ylabel);
    maybeSetTextbox(qs, "y2label", y2label);
    maybeSetTextbox(qs, "yformat", yformat);
    maybeSetTextbox(qs, "y2format", y2format);
    maybeSetTextbox(qs, "yrange", yrange);
    maybeSetTextbox(qs, "y2range", y2range);
    ylog.setValue(qs.containsKey("ylog"));
    y2log.setValue(qs.containsKey("y2log"));

    if (qs.containsKey("key")) {
      final String key = qs.getFirst("key");
      keybox.setValue(key.contains(" box"));
      horizontalkey.setValue(key.contains(" horiz"));
      keypos = key.replaceAll(" (box|horiz\\w*)", "");
      keypos_map.get(keypos).setChecked(true);
    } else {
      keybox.setValue(false);
      horizontalkey.setValue(false);
      keypos_map.get("top right").setChecked(true);
      keypos = "";
    }
    nokey.setValue(qs.containsKey("nokey"));
    smooth.setValue(qs.containsKey("smooth"));
    if (stylesMap.containsKey(qs.getFirst("style"))) {
      styles.setSelectedIndex(stylesMap.get(qs.getFirst("style")));
    }
  }

  private void refreshGraph() {
    final Date start = start_datebox.getValue();
    if (start == null) {
      graphstatus.setText("Please specify a start time.");
      return;
    }
    final Date end = end_datebox.getValue();
    if (end != null && !autoreload.getValue()) {
      if (end.getTime() <= start.getTime()) {
        end_datebox.addStyleName("dateBoxFormatError");
        graphstatus.setText("End time must be after start time!");
        return;
      }
    }
    final StringBuilder url = new StringBuilder();
    url.append("q?start=");
    final String start_text = start_datebox.getTextBox().getText();
    if (start_text.endsWith(" ago") || start_text.endsWith("-ago")) {
      url.append(start_text);
    } else {
      url.append(FULLDATE.format(start));
    }
    if (end != null && !autoreload.getValue()) {
      url.append("&end=");
      final String end_text = end_datebox.getTextBox().getText();
      if (end_text.endsWith(" ago") || end_text.endsWith("-ago")) {
        url.append(end_text);
      } else {
        url.append(FULLDATE.format(end));
      }
    } else {
      // If there's no end-time, the graph may change while the URL remains
      // the same.  No browser seems to re-fetch an image once it's been
      // fetched, even if we destroy the DOM object and re-created it with the
      // same src attribute.  This has nothing to do with caching headers sent
      // by the server.  The browsers simply won't retrieve the same URL again
      // through JavaScript manipulations, period.  So as a workaround, we add
      // a special parameter that the server will delete from the query.
      url.append("&ignore=" + nrequests++);
    }
    if (global_annotations.getValue()) {
      url.append("&global_annotations");
    }

    if(timezone.length() > 1)
      url.append("&tz=").append(timezone);

    if (!addAllMetrics(url)) {
      return;
    }
    addLabels(url);
    addFormats(url);
    addYRanges(url);
    addLogscales(url);
    if (nokey.getValue()) {
      url.append("&nokey");
    } else if (!keypos.isEmpty() || horizontalkey.getValue()) {
      url.append("&key=");
      if (!keypos.isEmpty()) {
        url.append(keypos);
      }
      if (horizontalkey.getValue()) {
        url.append(" horiz");
      }
      if (keybox.getValue()) {
        url.append(" box");
      }
    }
    url.append("&wxh=").append(wxh.getText());
    if (smooth.getValue()) {
      url.append("&smooth=csplines");
    }
    url.append("&style=").append(styles.getValue(styles.getSelectedIndex()));
    final String unencodedUri = url.toString();
    final String uri = URL.encode(unencodedUri);
    if (uri.equals(lastgraphuri)) {
      return;  // Don't re-request the same graph.
    } else if (pending_requests++ > 0) {
      return;
    }
    lastgraphuri = uri;
    graphstatus.setText("Loading graph...");
    asyncGetJson(uri + "&json", new GotJsonCallback() {
      public void got(final JSONValue json) {
        if (autoreoload_timer != null) {
          autoreoload_timer.cancel();
          autoreoload_timer = null;
        }
        final JSONObject result = json.isObject();
        final JSONValue err = result.get("err");
        String msg = "";
        if (err != null) {
          displayError("An error occurred while generating the graph: "
                       + err.isString().stringValue());
          graphstatus.setText("Please correct the error above.");
        } else {
          clearError();

          String history = unencodedUri.substring(2)      // Remove "q?".
            .replaceFirst("ignore=[^&]*&", "");  // Unnecessary cruft.
          if (autoreload.getValue()) {
            history += "&autoreload=" + autoreoload_interval.getText();
          }
          if (!history.equals(URL.decode(History.getToken()))) {
            History.newItem(history, false);
          }

          final JSONValue nplotted = result.get("plotted");
          final JSONValue cachehit = result.get("cachehit");
          if (cachehit != null) {
            msg += "Cache hit (" + cachehit.isString().stringValue() + "). ";
          }
          if (nplotted != null && nplotted.isNumber().doubleValue() > 0) {
            graph.setUrl(uri + "&png");
            graph.setVisible(true);

            msg += result.get("points").isNumber() + " points retrieved, "
              + nplotted + " points plotted";
          } else {
            graph.setVisible(false);
            msg += "Your query didn't return anything";
          }
          final JSONValue timing = result.get("timing");
          if (timing != null) {
            msg += " in " + timing + "ms.";
          } else {
            msg += '.';
          }
        }
        final JSONValue info = result.get("info");
        if (info != null) {
          if (!msg.isEmpty()) {
            msg += ' ';
          }
          msg += info.isString().stringValue();
        }
        graphstatus.setText(msg);
        if (result.get("etags") != null) {
          final JSONArray etags = result.get("etags").isArray();
          final int netags = etags.size();
          for (int i = 0; i < netags; i++) {
            if (i >= metrics.getWidgetCount()) {
              break;
            }
            final Widget widget = metrics.getWidget(i);
            if (!(widget instanceof MetricForm)) {
              break;
            }
            final MetricForm metric = (MetricForm) widget;
            final JSONArray tags = etags.get(i).isArray();
            // Skip if no tags were associated with the query.
            if (null != tags) {
              for (int j = 0; j < tags.size(); j++) {
                metric.autoSuggestTag(tags.get(j).isString().stringValue());
              }
            }
          }
        }
        if (autoreload.getValue()) {
          final int reload_in = Integer.parseInt(autoreoload_interval.getValue());
          if (reload_in >= 5) {
            autoreoload_timer = new Timer() {
              public void run() {
                // Verify that we still want auto reload and that the graph
                // hasn't been updated in the mean time.
                if (autoreload.getValue() && lastgraphuri == uri) {
                  // Force refreshGraph to believe that we want a new graph.
                  lastgraphuri = "";
                  refreshGraph();
                }
              }
            };
            autoreoload_timer.schedule(reload_in * 1000);
          }
        }
        if (--pending_requests > 0) {
          pending_requests = 0;
          refreshGraph();
        }
      }
    });
  }

  private boolean addAllMetrics(final StringBuilder url) {
    boolean found_metric = false;
    for (final Widget widget : metrics) {
      if (!(widget instanceof MetricForm)) {
        continue;
      }
      final MetricForm metric = (MetricForm) widget;
      found_metric |= metric.buildQueryString(url);
    }
    if (!found_metric) {
      graphstatus.setText("Please specify a metric.");
    }
    return found_metric;
  }

  private void asyncGetJson(final String url, final GotJsonCallback callback) {
    final RequestBuilder builder = new RequestBuilder(RequestBuilder.GET, url);
    try {
      builder.sendRequest(null, new RequestCallback() {
        public void onError(final Request request, final Throwable e) {
          displayError("Failed to get " + url + ": " + e.getMessage());
          // Since we don't call the callback we've been given, reset this
          // bit of state as we're not going to retry anything right now.
          pending_requests = 0;
        }

        public void onResponseReceived(final Request request,
                                       final Response response) {
          final int code = response.getStatusCode();
          if (code == Response.SC_OK) {
            clearError();
            callback.got(JSONParser.parse(response.getText()));
            return;
          } else if (code >= Response.SC_BAD_REQUEST) {  // 400+ => Oops.
            // Since we don't call the callback we've been given, reset this
            // bit of state as we're not going to retry anything right now.
            pending_requests = 0;
            String err = response.getText();
            // If the response looks like a JSON object, it probably contains
            // an error message.
            if (!err.isEmpty() && err.charAt(0) == '{') {
              final JSONValue json = JSONParser.parse(err);
              final JSONObject result = json == null ? null : json.isObject();
              final JSONValue jerr = result == null ? null : result.get("err");
              final JSONString serr = jerr == null ? null : jerr.isString();
              err = serr.stringValue();
              // If the error message has multiple lines (which is common if
              // it contains a stack trace), show only the first line and
              // hide the rest in a panel users can expand.
              final int newline = err.indexOf('\n', 1);
              final String msg = "Request failed: " + response.getStatusText();
              if (newline < 0) {
                displayError(msg + ": " + err);
              } else {
                displayError(msg);
                final DisclosurePanel dp =
                  new DisclosurePanel(err.substring(0, newline));
                RootPanel.get("queryuimain").add(dp);  // Attach the widget.
                final InlineLabel content =
                  new InlineLabel(err.substring(newline, err.length()));
                content.addStyleName("fwf");  // For readable stack traces.
                dp.setContent(content);
                current_error.getElement().appendChild(dp.getElement());
              }
            } else {
              displayError("Request failed while getting " + url + ": "
                           + response.getStatusText());
              // Since we don't call the callback we've been given, reset this
              // bit of state as we're not going to retry anything right now.
              pending_requests = 0;
            }
            graphstatus.setText("");
          }
        }
      });
    } catch (RequestException e) {
      displayError("Failed to get " + url + ": " + e.getMessage());
    }
  }

  private void displayError(final String errmsg) {
    current_error.setText(errmsg);
    current_error.setVisible(true);
  }

  private void clearError() {
    current_error.setVisible(false);
  }

  static void setTextAlignCenter(final Element element) {
    element.getStyle().setProperty("textAlign", "center");
  }

  /** Zoom box and associated event handlers.  */
  private final class ZoomBox extends HTML
    implements MouseUpHandler, MouseMoveHandler, MouseDownHandler {

    /** "Fudge factor" to account for the axes present on the image. */
    private static final int OFFSET_WITH_AXIS = 45;
    private static final int OFFSET_WITHOUT_AXIS = 15;

    private boolean zoom_selection_active = false;
    /** Rectangle of the selection.  */
    private int start_x;
    private int end_x;
    private int start_y;
    private int end_y;

    private HandlerRegistration graph_move_handler;
    private HandlerRegistration box_move_handler;

    ZoomBox() {
      // Set ourselves up as the event handler for all mouse-draggable events.
      graph.addMouseDownHandler(this);
      graph.addMouseUpHandler(this);

      // Also add the handlers on the actual zoom highlight box (this is in
      // case the cursor gets on the zoombox, so that it keeps responding
      // correctly).
      super.addMouseUpHandler(this);

      final Style style = super.getElement().getStyle();
      style.setProperty("background", "red");
      style.setProperty("filter", "alpha(opacity=50)");
      style.setProperty("opacity", "0.4");
      // Needed to make this object focusable.
      super.getElement().setAttribute("tabindex", "-1");
    }

    @Override
    public void onMouseDown(final MouseDownEvent event) {
      event.preventDefault();

      // Check if the zoom selection is active, if so, it's possible that the
      // mouse left the browser mid-selection and got stuck enabled even
      // though the mouse isn't still pressed. If that's the case, do a similar
      // operation to the onMouseUp event.
      if (zoom_selection_active) {
        endSelection(event);
        return;
      }

      final Element image = graph.getElement();
      zoom_selection_active = true;
      start_x = event.getRelativeX(image);
      start_y = event.getRelativeY(image);
      end_x = 0;
      end_y = 0;

      graphbox.setWidgetPosition(this, start_x, start_y);
      super.setWidth("0px");
      super.setHeight("0px");
      super.setVisible(true);
      // Workaround to steal the focus from whatever had it previously,
      // which may cause the graph to reload as a side effect.
      super.getElement().focus();

      graph_move_handler = graph.addMouseMoveHandler(this);
      box_move_handler = super.addMouseMoveHandler(this);
    }

    @Override
    public void onMouseMove(final MouseMoveEvent event) {
      event.preventDefault();

      final int x = event.getRelativeX(graph.getElement());
      final int y = event.getRelativeY(graph.getElement());
      int left;
      int top;
      int width;
      int height;

      // Figure out the top, left, height, and width of the box based
      // on current cursor location.
      if (x < start_x) {
        left = x;
        width = start_x - x;
      } else {
        left = start_x;
        width = x - start_x;
      }
      if (y < start_y) {
        top = y;
        height = start_y - y;
      } else {
        top = start_y;
        height = y - start_y;
      }

      // Resize / move the box as needed based on cursor location.
      super.setVisible(false);
      graphbox.setWidgetPosition(this, left, top);
      super.setWidth(width + "px");
      super.setHeight(height + "px");
      super.setVisible(true);
    }

    @Override
    public void onMouseUp(final MouseUpEvent event) {
      if (zoom_selection_active) {
        endSelection(event);
      }
    }

    /**
     * Perform operations for when a user completes their selection.
     * This involves removing the highlight box and kicking off the
     * zoom in operation.
     * @param event The event that triggered the end of the selection.
     */
    private <H extends EventHandler> void endSelection(final MouseEvent<H> event) {
      zoom_selection_active = false;

      // Stop tracking cursor movements to improve performance.
      graph_move_handler.removeHandler();
      graph_move_handler = null;
      box_move_handler.removeHandler();
      box_move_handler = null;

      final Element image = graph.getElement();
      end_x = event.getRelativeX(image);
      end_y = event.getRelativeY(image);

      // Hide the zoom box
      super.setVisible(false);
      super.setWidth("0px");
      super.setHeight("0px");

      // Calculate the true start/end points of the zoom area selected by
      // mouse. If the mouse was dragged left on the graph before being
      // let up, then start_x is the right-most edge of the zoomable area.
      // If the mouse was dragged right on the graph before being let up,
      // then start_x is the left-most edge of the zoomable area.
      if (start_x < end_x) {
        start_x = start_x - OFFSET_WITH_AXIS;
        end_x = end_x - OFFSET_WITH_AXIS;
      } else {
        final int saved_start = start_x;
        start_x = end_x - OFFSET_WITH_AXIS;
        end_x = saved_start - OFFSET_WITH_AXIS;
      }
      int actual_width = graph.getWidth() - OFFSET_WITH_AXIS;
      if (y2range.isEnabled()) {  // If we have a second Y axis.
        actual_width -= OFFSET_WITH_AXIS;
      } else {
        actual_width -= OFFSET_WITHOUT_AXIS;
      }

      // Prevent division by zero if image is pathologically small.
      // or: Prevent changing anything if the distance the cursor traveled was
      // too small (as happens during a simple click or unintentional click).
      if (actual_width < 1 || end_x - start_x <= 5) {
        return;
      }

      // Total span of time represented between the start and end times.
      final long duration;
      final long start = start_datebox.getValue().getTime();
      {
        final long end;
        final Date end_date = end_datebox.getValue();
        if (end_date != null) {
          end = end_date.getTime();
        } else {
          end = new Date().getTime();
        }
        duration = end - start;
      }

      // Get the start and end positions of the mouse drag operation on the
      // image as a percentage of the image size.
      final long start_change = start_x * duration / actual_width;
      final long end_change = end_x * duration / actual_width;

      start_datebox.setValue(new Date(start + start_change));
      end_datebox.setValue(new Date(start + end_change));
      refreshGraph();
    }

  };

  private final class AdjustYRangeCheckOnClick implements ClickHandler {

    private final CheckBox box;
    private final ValidatedTextBox range;

    public AdjustYRangeCheckOnClick(final CheckBox box,
                                    final ValidatedTextBox range) {
      this.box = box;
      this.range = range;
    }

    public void onClick(final ClickEvent event) {
      if (box.isEnabled() && box.getValue()
          && "[0:]".equals(range.getValue())) {
        range.setValue("[1:]");
      } else if (box.isEnabled() && !box.getValue()
                 && "[1:]".equals(range.getValue())) {
        range.setValue("[0:]");
      }
    }

  };

  /**
   * Ensures all the widgets in the given panel have the same size.
   * Otherwise by default the panel will automatically resize itself to the
   * contents of the currently active panel's widget, which is annoying
   * because it makes a number of things move around in the UI.
   * @param panel The panel containing the widgets to resize.
   */
  private static void ensureSameWidgetSize(final DecoratedTabPanel panel) {
    if (!panel.isAttached()) {
      throw new IllegalArgumentException("panel not attached: " + panel);
    }
    int maxw = 0;
    int maxh = 0;
    for (final Widget widget : panel) {
      final int w = widget.getOffsetWidth();
      final int h = widget.getOffsetHeight();
      if (w > maxw) {
        maxw = w;
      }
      if (h > maxh) {
        maxh = h;
      }
    }
    if (maxw == 0 || maxh == 0) {
      throw new IllegalArgumentException("maxw=" + maxw + " maxh=" + maxh);
    }
    for (final Widget widget : panel) {
      setOffsetWidth(widget, maxw);
      setOffsetHeight(widget, maxh);
    }
  }

  /**
   * Properly sets the total width of a widget.
   * This takes into account decorations such as border, margin, and padding.
   */
  private static void setOffsetWidth(final Widget widget, int width) {
    widget.setWidth(width + "px");
    final int offset = widget.getOffsetWidth();
    if (offset > 0) {
      width -= offset - width;
      if (width > 0) {
        widget.setWidth(width + "px");
      }
    }
  }

  /**
   * Properly sets the total height of a widget.
   * This takes into account decorations such as border, margin, and padding.
   */
  private static void setOffsetHeight(final Widget widget, int height) {
    widget.setHeight(height + "px");
    final int offset = widget.getOffsetHeight();
    if (offset > 0) {
      height -= offset - height;
      if (height > 0) {
        widget.setHeight(height + "px");
      }
    }
  }

}
