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

import java.util.Date;

import com.google.gwt.event.dom.client.ClickEvent;
import com.google.gwt.event.dom.client.ClickHandler;
import com.google.gwt.event.dom.client.DomEvent;
import com.google.gwt.event.shared.EventHandler;
import com.google.gwt.i18n.client.DateTimeFormat;
import com.google.gwt.user.client.ui.Grid;
import com.google.gwt.user.client.ui.HTMLTable.CellFormatter;
import com.google.gwt.user.client.ui.HorizontalPanel;
import com.google.gwt.user.client.ui.InlineHTML;
import com.google.gwt.user.client.ui.InlineLabel;
import com.google.gwt.user.client.ui.PushButton;
import com.google.gwt.user.client.ui.TextBox;
import com.google.gwt.user.client.ui.VerticalPanel;
import com.google.gwt.user.datepicker.client.DateBox;
import com.google.gwt.user.datepicker.client.DatePicker;

/**
 * A {@link DateBox} with support for time.
 * <p>
 * This class is unnecessarily complicated because {@link DateBox} wasn't
 * designed in an extensible way (semi-purposefully).  A thread gwt-contrib
 * titled "DatePicker DatePickerComponent protected ?" from 2008 shows that
 * back then they were working on an extensible {@link DateBox} and
 * {@link DatePicker} for GWT 2, but as of Sep 2010 this hasn't happened.
 * Their suggestion to copy-paste-hack the code is unacceptable so instead
 * we go through a few hoops to make this work.
 */
final class DateTimeBox extends DateBox {

  private static final DateTimeFormat HHMM_FORMAT =
    DateTimeFormat.getFormat("HH:mm");

  private static final DefaultFormat DATE_FORMAT =
    new DefaultFormat(DateTimeFormat.getFormat("yyyy/MM/dd-HH:mm:ss")) {
      /** Adds support for some human readable dates ("1d ago", "10:30").  */
      @Override
      public Date parse(final DateBox box,
                        final String text,
                        final boolean report_error) {
        if (text.endsWith(" ago")) {  // "1d ago" and such
          int interval;
          final int lastchar = text.length() - 5;
          try {
            interval = Integer.parseInt(text.substring(0, lastchar));
          } catch (NumberFormatException e) {
            setError(box);
            return null;
          }
          if (interval <= 0) {
            setError(box);
            return null;
          }
          switch (text.charAt(lastchar)) {
            case 's': break;                               // seconds
            case 'm': interval *= 60; break;               // minutes
            case 'h': interval *= 3600; break;             // hours
            case 'd': interval *= 3600 * 24; break;        // days
            case 'w': interval *= 3600 * 24 * 7; break;    // weeks
            case 'y': interval *= 3600 * 24 * 365; break;  // years
          }
          final Date d = new Date();
          d.setTime(d.getTime() - interval * 1000);
          return d;
        } else if (text.length() == 5) {  // "HH:MM"
          try {
            return HHMM_FORMAT.parse(text);
          } catch (IllegalArgumentException ignored) {
            setError(box);
            return null;
          }
        }
        return super.parse(box, text, report_error);
      }

      private void setError(final DateBox box) {
        box.addStyleName("dateBoxFormatError");
      }
    };

  public DateTimeBox() {
    super(new DateTimePicker(), null, DATE_FORMAT);
    ((DateTimePicker) getDatePicker()).setDateTimeBox(this);
    final TextBox textbox = getTextBox();
    // Chrome 7.0.5xx versions, Safari 5.0.x and similar render a text box
    // that's too small for 19 characters (WTF?).  So we ask for space for
    // an extra 2 characters.  On Firefox the text box's width is computed
    // properly, so it simply appears slightly wider than necessary.
    textbox.setVisibleLength(19 + 2);
    textbox.setMaxLength(19);
  }

  /**
   * A {@link DatePicker} with a customized UI for time support.
   */
  private static final class DateTimePicker extends DatePicker {

    /** DateTimeBox this picker belongs to.  */
    private DateTimeBox box;
    /** A grid in which we put some buttons we may need to update.  */
    private Grid hours_minutes;

    public DateTimePicker() {
    }

    /**
     * Sets the {@link DateTimeBox} this {@link DateTimePicker} belongs to.
     * This <b>must</b> be called before using this object.
     */
    void setDateTimeBox(final DateTimeBox box) {
      this.box = box;
    }

    /**
     * Sets the date of the {@link DateBox} to the given {@link Date}.
     */
    private void setDate(final Date d) {
      refreshAll();
      box.setValue(d);
      // Put the focus back on the text box to make it easier for the
      // user to manually edit the field.
      box.getTextBox().setFocus(true);
    }

    /**
     * Returns a new button that shifts the date when clicked.
     * @param seconds How many seconds to shift.
     * @param label The label to put on the button.
     */
    private PushButton newShiftDateButton(final int seconds,
                                          final String label) {
      final PushButton button = new PushButton(label);
      button.setStyleName(seconds < 0 ? "datePickerPreviousButton"
                          : "datePickerNextButton");
      button.addClickHandler(new ClickHandler() {
        public void onClick(final ClickEvent event) {
          Date d = box.getValue();
          if (d == null) {
            if (seconds >= 0) {
              return;
            }
            d = new Date();
          }
          d.setTime(d.getTime() + seconds * 1000);
          d.setSeconds(0);
          setDate(d);
        }
      });
      return button;
    }

    /**
     * Returns a new button that sets the hours when clicked.
     * @param hours An hour of the day (0-23).
     * @param label The label to put on the button.
     */
    private PushButton newSetHoursButton(final int hours) {
      final PushButton button = new PushButton(Integer.toString(hours));
      button.addClickHandler(new ClickHandler() {
        public void onClick(final ClickEvent event) {
          @SuppressWarnings(/* GWT requires us to use Date */{"deprecation"})
          Date d = box.getValue();
          if (d == null) {
            d = new Date();
            d.setMinutes(0);
          }
          d.setHours(hours);
          d.setSeconds(0);
          setDate(d);
        }
      });
      return button;
    }

    /**
     * Returns a new button that sets the minutes when clicked.
     * @param minutes A value for minutes (0-59).
     * @param label The label to put on the button.
     */
    private PushButton newSetMinutesButton(final int minutes,
                                           final String label) {
      final PushButton button = new PushButton(label);
      button.addClickHandler(new ClickHandler() {
        public void onClick(final ClickEvent event) {
          @SuppressWarnings(/* GWT requires us to use Date */{"deprecation"})
          Date d = box.getValue();
          if (d == null) {
            d = new Date();
          }
          d.setMinutes(minutes);
          d.setSeconds(0);
          setDate(d);
        }
      });
      return button;
    }

    /** Rebuilds parts of the UI with buttons to set AM hours.  */
    private void setupAmUI() {
      hours_minutes.setWidget(0, 1, newSetHoursButton(0));
      hours_minutes.setWidget(0, 2, newSetHoursButton(1));
      hours_minutes.setWidget(0, 3, newSetHoursButton(2));
      hours_minutes.setWidget(0, 4, newSetHoursButton(3));
      hours_minutes.setWidget(0, 5, newSetHoursButton(4));
      hours_minutes.setWidget(0, 6, newSetHoursButton(5));
      hours_minutes.setWidget(1, 1, newSetHoursButton(6));
      hours_minutes.setWidget(1, 2, newSetHoursButton(7));
      hours_minutes.setWidget(1, 3, newSetHoursButton(8));
      hours_minutes.setWidget(1, 4, newSetHoursButton(9));
      hours_minutes.setWidget(1, 5, newSetHoursButton(10));
      hours_minutes.setWidget(1, 6, newSetHoursButton(11));
    }

    /** Rebuilds parts of the UI with buttons to set PM hours.  */
    private void setupPmUI() {
      hours_minutes.setWidget(0, 1, newSetHoursButton(12));
      hours_minutes.setWidget(0, 2, newSetHoursButton(13));
      hours_minutes.setWidget(0, 3, newSetHoursButton(14));
      hours_minutes.setWidget(0, 4, newSetHoursButton(15));
      hours_minutes.setWidget(0, 5, newSetHoursButton(16));
      hours_minutes.setWidget(0, 6, newSetHoursButton(17));
      hours_minutes.setWidget(1, 1, newSetHoursButton(18));
      hours_minutes.setWidget(1, 2, newSetHoursButton(19));
      hours_minutes.setWidget(1, 3, newSetHoursButton(20));
      hours_minutes.setWidget(1, 4, newSetHoursButton(21));
      hours_minutes.setWidget(1, 5, newSetHoursButton(22));
      hours_minutes.setWidget(1, 6, newSetHoursButton(23));
    }

    /** Sets up the custom UI of the date picker.  */
    @Override
    protected void setup() {
      final HorizontalPanel panel = new HorizontalPanel();
      initWidget(panel);
      setStyleName(panel.getElement(), "gwt-DatePicker");

      {
        final VerticalPanel vbox = new VerticalPanel();
        setStyleName("gwt-DatePicker");
        vbox.add(super.getMonthSelector());
        vbox.add(super.getView());

        panel.add(vbox);
      }

      {
        // This vbox contains all of the "extra" panel on the side
        // of the calendar view.
        final VerticalPanel vbox = new VerticalPanel();
        setStyleName(vbox.getElement(), "datePickerMonthSelector");

        final PushButton now = new PushButton("now");
        now.setStyleName("datePickerNextButton");
        now.addClickHandler(new ClickHandler() {
          public void onClick(final ClickEvent event) {
            box.setValue(new Date());
          }
        });

        {
          final Grid grid = new Grid(2, 9);
          grid.setWidget(0, 0, newShiftDateButton(-3600, "1h"));
          grid.setWidget(0, 1, newShiftDateButton(-600, "10m"));
          grid.setWidget(0, 2, newShiftDateButton(-60, "1m"));
          grid.setWidget(0, 3, new InlineHTML("&lsaquo;"));
          grid.setWidget(0, 4, now);
          grid.setWidget(0, 5, new InlineHTML("&rsaquo;"));
          grid.setWidget(0, 6, newShiftDateButton(+60, "1m"));
          grid.setWidget(0, 7, newShiftDateButton(+600, "10m"));
          grid.setWidget(0, 8, newShiftDateButton(+3600, "1h"));
          grid.setWidget(1, 0, newShiftDateButton(-86400 * 30, "30d"));
          grid.setWidget(1, 1, newShiftDateButton(-86400 * 7, "1w"));
          grid.setWidget(1, 2, newShiftDateButton(-86400, "1d"));
          grid.setWidget(1, 3, new InlineHTML("&laquo;"));
          grid.setWidget(1, 4, new InlineHTML("&nbsp;"));
          grid.setWidget(1, 5, new InlineHTML("&raquo;"));
          grid.setWidget(1, 6, newShiftDateButton(+86400, "1d"));
          grid.setWidget(1, 7, newShiftDateButton(+86400 * 7, "1w"));
          grid.setWidget(1, 8, newShiftDateButton(+86400 * 30, "30d"));
          final CellFormatter formatter = grid.getCellFormatter();
          formatter.setWidth(0, 4, "100%");
          formatter.setWidth(1, 4, "100%");
          vbox.add(grid);
        }

        {
          hours_minutes = new Grid(4, 8);
          setupAmUI();
          hours_minutes.setWidget(0, 0, new InlineLabel("HH"));
          final PushButton set_am = new PushButton("AM");
          set_am.addClickHandler(new ClickHandler() {
            public void onClick(final ClickEvent event) {
              setupAmUI();
            }
          });
          hours_minutes.setWidget(0, 7, set_am);

          final PushButton set_pm = new PushButton("PM");
          set_pm.addClickHandler(new ClickHandler() {
            public void onClick(final ClickEvent event) {
              setupPmUI();
            }
          });
          hours_minutes.setWidget(1, 7, set_pm);

          hours_minutes.setWidget(2, 0, new InlineLabel("MM"));
          hours_minutes.setWidget(2, 1, newSetMinutesButton(0, "00"));
          hours_minutes.setWidget(2, 2, newSetMinutesButton(10, "10"));
          hours_minutes.setWidget(2, 3, newSetMinutesButton(20, "20"));
          hours_minutes.setWidget(2, 4, newSetMinutesButton(30, "30"));
          hours_minutes.setWidget(2, 5, newSetMinutesButton(40, "40"));
          hours_minutes.setWidget(2, 6, newSetMinutesButton(50, "50"));
          vbox.add(hours_minutes);
        }

        {
          final HorizontalPanel hbox = new HorizontalPanel();
          hbox.add(new InlineLabel("UNIX timestamp:"));
          final ValidatedTextBox ts = new ValidatedTextBox();
          ts.setValidationRegexp("^(|[1-9][0-9]{0,9})$");
          ts.setVisibleLength(10);
          ts.setMaxLength(10);
          final EventsHandler handler = new EventsHandler() {
            protected <H extends EventHandler> void onEvent(final DomEvent<H> event) {
              final Date d = new Date(Integer.parseInt(ts.getValue()) * 1000L);
              box.setValue(d, true);
            }
          };
          ts.addBlurHandler(handler);
          ts.addKeyPressHandler(handler);
          hbox.add(ts);
          vbox.add(hbox);
        }
        vbox.setHeight("100%");
        panel.add(vbox);
        panel.setCellHeight(vbox, "100%");
      }
    }

  }

}
