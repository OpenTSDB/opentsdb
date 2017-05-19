// under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 2.1 of the License, or (at your
// option) any later version.  This program is distributed in the hope that it
// will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty
// of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser
// General Public License for more details.  You should have received a copy
// of the GNU Lesser General Public License along with this program.  If not,
// see <http://www.gnu.org/licenses/>.package net.opentsdb.data;
package net.opentsdb.query.execution.serdes;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Map.Entry;

import com.google.common.reflect.TypeToken;

import net.opentsdb.common.Const;
import net.opentsdb.data.SimpleStringGroupId;
import net.opentsdb.data.TimeSeriesGroupId;
import net.opentsdb.data.iterators.DefaultIteratorGroups;
import net.opentsdb.data.iterators.IteratorGroup;
import net.opentsdb.data.iterators.IteratorGroups;
import net.opentsdb.data.iterators.TimeSeriesIterators;
import net.opentsdb.data.iterators.TimeSeriesIterator;
import net.opentsdb.data.types.numeric.NumericMillisecondShard;
import net.opentsdb.data.types.numeric.NumericType;
import net.opentsdb.data.types.numeric.UglyByteNumericSerdes;
import net.opentsdb.query.pojo.TimeSeriesQuery;
import net.opentsdb.utils.Bytes;

/**
 * Just a super ugly and uncompressed serialization method for dumping
 * an iterator groups set into a byte encoded output stream and reading back
 * in.
 * <p>
 * TODO - Right now it's only supporting the {@link NumericMillisecondShard}
 * since it *does* provide a modicum of compression.
 * <p>
 * TODO - Registry of types and put in the registry.
 * 
 * @since 3.0
 */
public class UglyByteIteratorGroupsSerdes implements
    TimeSeriesSerdes<IteratorGroups> {

  // TODO - registry :)
  /** The numeric type serdes. */
  private final UglyByteNumericSerdes nums = new UglyByteNumericSerdes();
  
  @SuppressWarnings("unchecked")
  @Override
  public void serialize(final TimeSeriesQuery query, 
                        final SerdesOptions options,
                        final OutputStream stream, 
                        final IteratorGroups data) {
    if (stream == null) {
      throw new IllegalArgumentException("Output stream may not be null.");
    }
    if (data == null) {
      throw new IllegalArgumentException("Data may not be null.");
    }
    try {
      stream.write(Bytes.fromInt(data.groups().size()));
      for (final Entry<TimeSeriesGroupId, IteratorGroup> entry : data) {
        final TimeSeriesGroupId group_id = entry.getKey();
        final byte[] group = group_id.id().getBytes(Const.UTF8_CHARSET);
        stream.write(Bytes.fromInt(group.length));
        stream.write(group);
        
        stream.write(Bytes.fromInt(entry.getValue().iterators().size()));
        for (final TimeSeriesIterators iterators : entry.getValue()) {
          
          stream.write(Bytes.fromInt(iterators.iterators().size()));
          for (final TimeSeriesIterator<?> it : iterators.iterators()) {
            final byte[] type = it.type().toString().getBytes(Const.ASCII_CHARSET);
            stream.write(Bytes.fromInt(type.length));
            stream.write(type);
            
            if (it.type().equals(NumericType.TYPE)) {
              nums.serialize(query, options, stream, 
                  (TimeSeriesIterator<NumericType>) it);
            }
          }
        }
      }
    } catch (IOException e) {
      throw new RuntimeException("Unexpected exception during "
          + "serialization of: " + data, e);
    }
  }

  @Override
  public IteratorGroups deserialize(final SerdesOptions options,
                                    final InputStream stream) {
    if (stream == null) {
      throw new IllegalArgumentException("Stream cannot be null.");
    }
    final IteratorGroups results = new DefaultIteratorGroups();
    
    try {
      byte[] buf = new byte[4];
      stream.read(buf);
      int groups = Bytes.getInt(buf);
      for (int g = 0; g < groups; g++) {
        // group first
        buf = new byte[4];
        stream.read(buf);
        int len = Bytes.getInt(buf);
        
        buf = new byte[len];
        stream.read(buf);
        final TimeSeriesGroupId group_id = new SimpleStringGroupId(
            new String(buf, Const.UTF8_CHARSET));
        
        buf = new byte[4];
        stream.read(buf);
        int timeseries = Bytes.getInt(buf);
        
        for (int t = 0; t < timeseries; t++) {
          buf = new byte[4];
          stream.read(buf);          
          int iterators = Bytes.getInt(buf);
          
          for (int i = 0; i < iterators; i++) {
            buf = new byte[4];
            stream.read(buf);    
            len = Bytes.getInt(buf);
            
            buf = new byte[len];
            stream.read(buf);
            
            // TODO - need a util here
            Class<?> clazz = Class.forName(new String(buf));
            TypeToken<?> type = TypeToken.of(clazz);
            if (type.equals(NumericType.TYPE)) {
              results.addIterator(group_id, nums.deserialize(options, stream));
            }
          }
        }
      }
    } catch (Exception e) {
      throw new RuntimeException("Unexpected exception deserializing stream: " 
          + stream, e);
    }
    return results;
  }

}
