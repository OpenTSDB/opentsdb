
package net.opentsdb.search;

import net.opentsdb.meta.Annotation;
import net.opentsdb.meta.LabelMeta;
import net.opentsdb.plugins.Plugin;
import net.opentsdb.uid.LabelId;
import net.opentsdb.uid.LabelType;

import com.google.common.util.concurrent.ListenableFuture;

import javax.annotation.Nonnull;

/**
 * Implementations of this plugin provides advanced search capabilities to clients. The index
 * manipulation methods will be called on changes to the domain objects.
 *
 * <p>The canonical information will always be stored in the database that is managed by the {@link
 * net.opentsdb.storage.TsdbStore} and the same information may be indexed more than once when the
 * user runs a full re-index for example.
 */
public abstract class SearchPlugin extends Plugin {
  /**
   * Called when we want to add or update the information of a LabelMeta object.
   *
   * @param meta A validated {@link LabelMeta} instance to index
   * @return A future that indicates the completion of the request.
   */
  @Nonnull
  public abstract ListenableFuture<Void> indexLabelMeta(final LabelMeta meta);

  /**
   * Called when we need to remove a LabelMeta from the store that backs this search plugin.
   *
   * @param id The identifier that together with the provided type identifies a {@link LabelMeta} to
   * remove
   * @param type The type that together with the provided id identifies a {@link LabelMeta} to
   * remove
   * @return A future that indicates the completion of the request.
   */
  @Nonnull
  public abstract ListenableFuture<Void> deleteLabelMeta(final LabelId id,
                                                         final LabelType type);

  /**
   * Index the annotation in the backing store.
   *
   * @param note The annotation to index
   * @return A future that indicates the completion of the request.
   */
  @Nonnull
  public abstract ListenableFuture<Void> indexAnnotation(final Annotation note);

  /**
   * Remove the annotation from the backing store.
   *
   * @param note The annotation to remove
   * @return A future that indicates the completion of the request.
   */
  @Nonnull
  public abstract ListenableFuture<Void> deleteAnnotation(final Annotation note);

  /**
   * Perform a query against this search plugins backing store and get the matching label meta
   * objects.
   *
   * <p>There are no expectations on what kind of filtering the backing store has to support. If the
   * search plugin does not support filtering then an empty {@link Iterable} should be returned.
   *
   * @param query A plugin specific query as received without modification
   * @return A future that on completion contains an {@link Iterable} of label meta objects
   */
  @Nonnull
  public abstract ListenableFuture<Iterable<LabelMeta>> findLabels(final String query);
}
