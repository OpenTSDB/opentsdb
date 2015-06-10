// This file is part of OpenTSDB.
// Copyright (C) 2013  The OpenTSDB Authors.
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

package net.opentsdb.search;

import net.opentsdb.meta.Annotation;
import net.opentsdb.meta.LabelMeta;
import net.opentsdb.plugins.Plugin;
import net.opentsdb.uid.IdQuery;
import net.opentsdb.uid.IdentifierDecorator;
import net.opentsdb.uid.LabelId;
import net.opentsdb.uid.UniqueIdType;

import com.stumbleupon.async.Deferred;

import java.util.List;
import javax.annotation.Nonnull;

/**
 * Search plugins allow data from OpenTSDB to be published to a search indexer. Many great products
 * already exist for searching so it doesn't make sense to re-implement an engine within OpenTSDB.
 * Likewise, going directly to the storage system for searching isn't efficient.
 *
 * <p><b>Note:</b> Since canonical information is stored in the underlying OpenTSDB database, the
 * same document may be re-indexed more than once. This may happen if someone runs a full
 * re-indexing thread to make sure the search engine is up to date, particularly after a TSD crash
 * where some data may not have been sent. Be sure to account for that when indexing. Each object
 * has a way to uniquely identify it, see the method notes below.
 *
 * <p><b>Warning:</b> All indexing methods should be performed asynchronously. You may want to
 * create a queue in the implementation to store data until you can ship it off to the service.
 * Every indexing method should return as quickly as possible.
 *
 * @since 2.0
 */
public abstract class SearchPlugin extends Plugin {
  /**
   * Called when we want to add or update the information of a LabelMeta object.
   *
   * @param meta A validated {@link LabelMeta} instance to index
   * @return A deferred object that indicates the completion of the request.
   */
  public abstract Deferred<Void> indexLabelMeta(final LabelMeta meta);

  /**
   * Called when we need to remove a LabelMeta from the store that backs this search plugin.
   *
   * @param id The identifier that together with the provided type identifies a {@link LabelMeta} to
   * remove
   * @param type The type that together with the provided id identifies a {@link LabelMeta} to
   * remove
   * @return A deferred object that indicates the completion of the request.
   */
  @Nonnull
  public abstract Deferred<Void> deleteLabelMeta(@Nonnull final LabelId id,
                                                 @Nonnull final UniqueIdType type);

  /**
   * Indexes an annotation object <b>Note:</b> Unique Document ID = TSUID and Start Time
   *
   * @param note The annotation to index
   * @return A deferred object that indicates the completion of the request. The {@link Object} has
   * not special meaning and can be {@code null} (think of it as {@code Deferred<Void>}).
   */
  public abstract Deferred<Object> indexAnnotation(final Annotation note);

  /**
   * Called to remove an annotation object from the index <b>Note:</b> Unique Document ID = TSUID
   * and Start Time
   *
   * @param note The annotation to remove
   * @return A deferred object that indicates the completion of the request. The {@link Object} has
   * not special meaning and can be {@code null} (think of it as {@code Deferred<Void>}).
   */
  public abstract Deferred<Object> deleteAnnotation(final Annotation note);

  /**
   * Executes a very basic search query, returning the results in the SearchQuery object passed in.
   *
   * @param query The query to execute against the search engine
   * @return The query results
   */
  public abstract Deferred<SearchQuery> executeQuery(final SearchQuery query);

  /**
   * Should look up IDs that match the parameters described in the provided {@link
   * net.opentsdb.uid.IdQuery}.
   *
   * @param query The parameters for the query
   * @return A deferred with a list of matching IDs.
   */
  public abstract Deferred<List<IdentifierDecorator>> executeIdQuery(final IdQuery query);
}
