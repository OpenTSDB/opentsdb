package tsd.client;

import com.google.gwt.user.client.ui.SimplePanel;

public class AnnotationsForm extends SimplePanel {
  private final TagsPanel tagsPanel;

  public AnnotationsForm(final EventsHandler eventsHandler) {
    super();

    tagsPanel = new TagsPanel(eventsHandler);

    this.setWidget(tagsPanel);
  }

  public void buildQueryString(final StringBuilder url) {
    final String[][] tags = tagsPanel.getTags();

    if (tags.length > 0) {
      url.append("&a={");

      for (int i = 0; i < tags.length; i++) {
        final String name = tags[i][0];
        final String value = tags[i][1];

        if (!name.isEmpty() && !value.isEmpty()) {
          url.append(name).append('=').append(value).append(',');
        }
      }

      final int last = url.length() - 1;

      if (url.charAt(last) == '{') {  // There was no tag.
        url.setLength(last);          // So remove the `{'.
      } else {  // Need to replace the last ',' with a `}'.
        url.setCharAt(last, '}');
      }
    }
  }
}
