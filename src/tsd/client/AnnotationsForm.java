package tsd.client;

import java.util.Map;

import com.google.gwt.user.client.ui.SimplePanel;

public class AnnotationsForm extends SimplePanel {
  private final TagsPanel tagsPanel;

  public AnnotationsForm(final EventsHandler eventsHandler) {
    super();

    tagsPanel = new TagsPanel(eventsHandler);

    this.setWidget(tagsPanel);
  }

  public void buildQueryString(final StringBuilder url) {
    url.append("&a={");
    Map<String, String> tags = tagsPanel.getTags();

    for (Map.Entry<String, String> tag : tags.entrySet()) {
      String name = tag.getKey();
      String value = tag.getValue();

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
