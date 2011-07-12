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
import java.util.HashMap;

import com.google.gwt.http.client.RequestBuilder;
import com.google.gwt.http.client.RequestCallback;
import com.google.gwt.http.client.RequestException;
import com.google.gwt.http.client.Response;
import com.google.gwt.json.client.JSONArray;
import com.google.gwt.json.client.JSONParser;
import com.google.gwt.json.client.JSONValue;
import com.google.gwt.user.client.ui.HasText;
import com.google.gwt.user.client.ui.MultiWordSuggestOracle;
import com.google.gwt.user.client.ui.SuggestBox;
import com.google.gwt.user.client.ui.SuggestOracle;
import com.google.gwt.user.client.ui.TextBoxBase;

/**
 * An oracle that gets suggestions through an AJAX call and provides caching.
 *
 * The oracle builds up a local cache of known suggestions and tries to avoid
 * unnecessary requests when the cache can be used (which is fairly frequent
 * given the typing pattern) or when we know for sure there won't be any
 * results.
 *
 * The oracle is given a type.  Every instance that share the same type also
 * share the same caches under the hood.  This is convenient when you want to
 * have multiple text boxes with the same type of suggestions.
 */
final class RemoteOracle extends SuggestOracle {

  private static final int MAX_SUGGESTIONS = 25;  // = UniqueId.MAX_SUGGESTIONS

  private static final String SUGGEST_URL = "/suggest?type=";  // + type&q=foo

  /**
   * Maps an oracle type to its suggestion cache.
   * The cache is in fact a {@link MultiWordSuggestOracle}, which we re-use as
   * its implementation is good (it uses a trie, handles HTML formatting etc.).
   */
  private static final HashMap<String, MultiWordSuggestOracle> caches =
    new HashMap<String, MultiWordSuggestOracle>();

  /** Maps an oracle type to the queries recently seen for this type. */
  private static final HashMap<String, QueriesSeen> all_queries_seen =
    new HashMap<String, QueriesSeen>();

  private final String type;
  private final MultiWordSuggestOracle cache;
  private final QueriesSeen queries_seen;
  private final ArrayList<Suggestion> default_suggestions =
    new ArrayList<Suggestion>(MAX_SUGGESTIONS);

  /** Which widget are we wrapping to provide suggestions. */
  private HasText requester;
  /** Current ongoing request, or null. */
  private Callback current;

  /**
   * Pending request that arrived while we were still processing `current'.
   * If requests keep coming in while we're processing `current', the last
   * pending one will overwrite the previous pending one.
   */
  private Request pending_req;
  private Callback pending_cb;

  /** Used to guess whether we need to fetch more suggestions. */
  private String last_query;
  private String last_suggestion;

  /**
   * Factory method to use in order to get a {@link RemoteOracle} instance.
   * @param suggest_type The type of suggestion wanted.
   * @param textbox The text box to wrap to provide suggestions to.
   */
  public static SuggestBox newSuggestBox(final String suggest_type,
                                         final TextBoxBase textbox) {
    final RemoteOracle oracle = new RemoteOracle(suggest_type);
    final SuggestBox box = new SuggestBox(oracle, textbox);
    oracle.requester = box;
    return box;
  }

  /** Private constructor, use {@link #newSuggestBox} instead. */
  private RemoteOracle(final String suggest_type) {
    type = suggest_type;
    MultiWordSuggestOracle cache = caches.get(type);
    QueriesSeen queries_seen;
    if (cache == null) {
      cache = new MultiWordSuggestOracle(".");
      queries_seen = new QueriesSeen();
      caches.put(type, cache);
      all_queries_seen.put(type, queries_seen);
    } else {
      queries_seen = all_queries_seen.get(type);
    }
    this.cache = cache;
    this.queries_seen = queries_seen;
  }

  @Override
  public boolean isDisplayStringHTML() {
    return true;
  }

  @Override
  public void requestSuggestions(final Request request, final Callback callback) {
    if (current != null) {
      pending_req = request;
      pending_cb = callback;
      return;
    }
    current = callback;
    {
      final String this_query = request.getQuery();
      // Check if we can serve this from our local cache, without even talking
      // to the server.  This is possible if either of those is true:
      //   1. We've already seen this query recently.
      //   2. This new query precedes another one and the user basically just
      //      typed another letter, so if the new query is "less than" the last
      //      result we got from the server, we know we already cached the full
      //      range of results covering the new request.
      if ((last_query != null
           && last_query.compareTo(this_query) <= 0
           && this_query.compareTo(last_suggestion) < 0)
          || queries_seen.check(this_query)) {
        current = null;
        cache.requestSuggestions(request, callback);
        return;
      }
      last_query = this_query;
    }

    final RequestBuilder builder = new RequestBuilder(RequestBuilder.GET,
      SUGGEST_URL + type + "&q=" + last_query);
    try {
      builder.sendRequest(null, new RequestCallback() {
        public void onError(final com.google.gwt.http.client.Request r,
                            final Throwable e) {
          current = null;  // Something bad happened, drop the current request.
          if (pending_req != null) {  // But if we have another waiting...
            requestSuggestions(pending_req, pending_cb);  // ... try it now.
          }
        }

        // Need to use fully-qualified names as this class inherits already
        // from a pair of inner classes called Request / Response :-/
        public void onResponseReceived(final com.google.gwt.http.client.Request r,
                                       final com.google.gwt.http.client.Response response) {
          if (response.getStatusCode() == com.google.gwt.http.client.Response.SC_OK
              // Is this response still relevant to what the requester wants?
              && requester.getText().startsWith(last_query)) {
            final JSONValue json = JSONParser.parse(response.getText());
            // In case this request returned nothing, we pretend the last
            // suggestion ended with the largest character possible, so we
            // won't send more requests to the server if the user keeps
            // adding extra characters.
            last_suggestion = last_query + "\377";
            if (json != null && json.isArray() != null) {
              final JSONArray results = json.isArray();
              final int n = Math.min(request.getLimit(), results.size());
              for (int i = 0; i < n; i++) {
                final JSONValue suggestion = results.get(i);
                if (suggestion == null || suggestion.isString() == null) {
                  continue;
                }
                final String suggestionstr = suggestion.isString().stringValue();
                last_suggestion = suggestionstr;
                cache.add(suggestionstr);
              }
              cache.requestSuggestions(request, callback);
              pending_req = null;
              pending_cb = null;
            }
          }
          current = null;  // Regardless of what happened above, this is done.
          if (pending_req != null) {
            final Request req = pending_req;
            final Callback cb = pending_cb;
            pending_req = null;
            pending_cb = null;
            requestSuggestions(req, cb);
          }
        }
      });
    } catch (RequestException ignore) {
    }
  }

  /** Small circular buffer of queries already typed by the user. */
  private static final class QueriesSeen {

    /**
     * A circular buffer containing the last 32 requests already served.
     * It would be awesome if {@code gwt.user.client.ui.PrefixTree} wasn't
     * package-private, so we could use that instead.
     */
    private final String[] already_requested = new String[128];
    private int already_index;  // Index into already_index.

    /**
     * Checks whether or not we've already seen that query.
     */
    boolean check(final String query) {
      // Check most recent queries first, as they're the most likely to match
      // if the user goes back and forth by typing a few characters, removing
      // some, typing some more, etc.
      for (int i = already_index - 1; i >= 0; i--) {
        if (query.equals(already_requested[i])) {
          return true;
        }
      }
      for (int i = already_requested.length - 1; i >= already_index; i--) {
        if (query.equals(already_requested[i])) {
          return true;
        }
      }

      // First time we see this query, let's record it.
      already_requested[already_index++] = query;
      if (already_index == already_requested.length) {
        already_index = 0;
      }
      return false;
    }

  }

}
