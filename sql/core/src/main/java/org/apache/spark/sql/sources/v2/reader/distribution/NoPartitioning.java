package org.apache.spark.sql.sources.v2.reader.distribution;

/**
 * Represents a partitioning that guarantees nothing, which means it can not satisfy any
 * distributions.
 */
public class NoPartitioning implements Partitioning {
  @Override
  public boolean satisfies(Distribution distribution) {
    return false;
  }

  @Override
  public int numPartitions() {
    return 0;
  }
}
