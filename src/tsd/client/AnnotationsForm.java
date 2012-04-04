package tsd.client;

import com.google.gwt.user.client.ui.SimplePanel;

public class AnnotationsForm extends SimplePanel {
  private final TagsPanel tagsPanel;

  public AnnotationsForm(final EventsHandler eventsHandler) {
    super();

    tagsPanel = new TagsPanel(eventsHandler);

    this.setWidget(tagsPanel);
  }

  public String buildQueryString() {
    final StringBuilder result = new StringBuilder();
    final String[][] tags = tagsPanel.getTags();

    if (tags.length > 0) {
      result.append("&a={");

      for (int i = 0; i < tags.length; i++) {
        final String name = tags[i][0];
        final String value = tags[i][1];

        if (!name.isEmpty() && !value.isEmpty()) {
          result.append(name).append('=').append(value).append(',');
        }
      }

      final int last = result.length() - 1;

      if (result.charAt(last) == '{') {  // There was no tag.
        result.setLength(last);          // So remove the `{'.
      } else {  // Need to replace the last ',' with a `}'.
        result.setCharAt(last, '}');
      }
    }
    
    return result.toString();
  }
}
