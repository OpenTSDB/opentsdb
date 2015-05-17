package net.opentsdb.utils;

import com.google.common.primitives.Longs;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;

import java.util.concurrent.TimeUnit;

@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Benchmark)
public class MoreLongsBenchmark {
  @Param({"4", "123123123", "9223372036854775807"})
  public String stringLong;

  @Benchmark
  public long moreLongsParseLong() {
    return MoreLongs.parseLong(stringLong);
  }

  @Benchmark
  public long longParseLong() {
    return Long.parseLong(stringLong);
  }

  @Benchmark
  public long longsTryParse() {
    return Longs.tryParse(stringLong);
  }
}
