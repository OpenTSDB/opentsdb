package tsd.client;

import com.google.gwt.user.client.ui.SimplePanel;

public class AnnotationsForm extends SimplePanel {
  private final TagsPanel tagsPanel;

  public AnnotationsForm(EventsHandler eventsHandler) {
    super();
    
    tagsPanel = new TagsPanel(eventsHandler);

    this.setWidget(tagsPanel);
  }
}
