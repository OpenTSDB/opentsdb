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

import com.google.gwt.event.dom.client.BlurEvent;
import com.google.gwt.event.dom.client.BlurHandler;
import com.google.gwt.user.client.Command;
import com.google.gwt.user.client.DeferredCommand;
import com.google.gwt.user.client.ui.TextBox;

class ValidatedTextBox extends TextBox implements BlurHandler {

  private String regexp;

  public ValidatedTextBox() {
  }

  public void setValidationRegexp(final String regexp) {
    if (this.regexp == null) {  // First call to this method.
      super.addBlurHandler(this);
    }
    this.regexp = regexp;
  }

  public String getValidationRegexp() {
    return regexp;
  }

  public void onBlur(final BlurEvent event) {
    final String interval = getText();
    if (!interval.matches(regexp)) {
      // Steal the dateBoxFormatError :)
      addStyleName("dateBoxFormatError");
      event.stopPropagation();
      DeferredCommand.addCommand(new Command() {
        public void execute() {
          // TODO(tsuna): Understand why this doesn't work as expected, even
          // though we cancel the onBlur event and we put the focus afterwards
          // using a deferred command.
          //setFocus(true);
          selectAll();
        }
      });
    } else {
      removeStyleName("dateBoxFormatError");
    }
  }

}
