package tsd.client;

import com.google.gwt.event.dom.client.BlurEvent;
import com.google.gwt.event.dom.client.BlurHandler;
import com.google.gwt.event.dom.client.ClickEvent;
import com.google.gwt.event.dom.client.ClickHandler;
import com.google.gwt.user.client.ui.Button;
import com.google.gwt.user.client.ui.FlexTable;
import com.google.gwt.user.client.ui.SimplePanel;
import com.google.gwt.user.client.ui.SuggestBox;
import com.google.gwt.user.client.ui.Widget;

public class TagsPanel extends SimplePanel {
  private final FlexTable tagtable = new FlexTable();
  private EventsHandler eventsHandler;

  public TagsPanel(final EventsHandler eventsHandler) {
    this.eventsHandler = eventsHandler;
    this.setSize("100%", "100%");
    this.setVisible(true);

    tagtable.setText(0, 0, "Tags");

    add(tagtable);

    addTag(null);
  }

  /**
   * Creates and returns a two-dimensional array with keys and values of all tags in this panel. 
   * 
   * @return two-dimensional array (String[row][col])
   */
  public String[][] getTags() {
    int tagCount = getNumTags();
    String[][] result = new String[tagCount][2];

    for (int i = 0; i < getNumTags(); i++) {
      result[i][0] = getTagName(i);
      result[i][1] = getTagValue(i);
    }

    return result;
  }

  private int getNumTags() {
    int result = 0;
    int rowCount = tagtable.getRowCount();
    int colCount = tagtable.getCellCount(0);

    // there is always a first row (because of the label), 
    // therefore we have to check if there are text boxes for the tags.
    if (colCount > 1) {
      result = rowCount;
    }

    return result;
  }

  private String getTagName(final int i) {
    return ((SuggestBox) tagtable.getWidget(i, 1)).getValue();
  }

  private String getTagValue(final int i) {
    return ((SuggestBox) tagtable.getWidget(i, 2)).getValue();
  }

  private void setTagName(final int i, final String value) {
    ((SuggestBox) tagtable.getWidget(i, 1)).setValue(value);
  }

  private void setTagValue(final int i, final String value) {
    ((SuggestBox) tagtable.getWidget(i, 2)).setValue(value);
  }

  private void addTag(final String default_tagname) {
    final int row = getNumTags();

    final ValidatedTextBox tagname = new ValidatedTextBox();
    final SuggestBox suggesttagk = RemoteOracle.newSuggestBox("tagk", tagname);
    final ValidatedTextBox tagvalue = new ValidatedTextBox();
    final SuggestBox suggesttagv = RemoteOracle.newSuggestBox("tagv", tagvalue);
    tagname.setValidationRegexp(ClientConstants.TSDB_ID_RE);
    tagvalue.setValidationRegexp(ClientConstants.TSDB_TAGVALUE_RE);
    tagname.setWidth("100%");
    tagvalue.setWidth("100%");
    tagname.addBlurHandler(recompact_tagtable);
    tagname.addBlurHandler(eventsHandler);
    tagname.addKeyPressHandler(eventsHandler);
    tagvalue.addBlurHandler(recompact_tagtable);
    tagvalue.addBlurHandler(eventsHandler);
    tagvalue.addKeyPressHandler(eventsHandler);

    tagtable.setWidget(row, 1, suggesttagk);
    tagtable.setWidget(row, 2, suggesttagv);
    if (row > 0) {
      final Button remove = new Button("x");
      remove.addClickHandler(removetag);
      tagtable.setWidget(row, 0, remove);
    }
    if (default_tagname != null) {
      tagname.setText(default_tagname);
      tagvalue.setFocus(true);
    }
  }

  public void autoSuggestTag(final String tag) {
    // First try to see if the tag is already in the table.
    final int nrows = getNumTags();
    int unused_row = -1;
    for (int row = 0; row < nrows; row++) {
      final SuggestBox tagname = ((SuggestBox) tagtable.getWidget(row, 1));
      final SuggestBox tagvalue = ((SuggestBox) tagtable.getWidget(row, 2));
      final String thistag = tagname.getValue();
      if (thistag.equals(tag)) {
        return;  // This tag is already in the table.
      }
      if (thistag.isEmpty() && tagvalue.getValue().isEmpty()) {
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
          tagtable.removeRow(tag);
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
        if (source == tagtable.getWidget(tag, 0)) {
          tagtable.removeRow(tag);
          eventsHandler.onClick(event);
          break;
        }
      }
    }
  };
}
