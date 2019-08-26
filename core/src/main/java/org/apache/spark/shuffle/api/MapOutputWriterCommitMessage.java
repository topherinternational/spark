package org.apache.spark.shuffle.api;

import java.util.Optional;

import org.apache.spark.annotation.Experimental;
import org.apache.spark.storage.BlockManagerId;

@Experimental
public final class MapOutputWriterCommitMessage {

  private final long[] partitionLengths;
  private final Optional<BlockManagerId> location;

  private MapOutputWriterCommitMessage(long[] partitionLengths, Optional<BlockManagerId> location) {
    this.partitionLengths = partitionLengths;
    this.location = location;
  }

  public static MapOutputWriterCommitMessage of(long[] partitionLengths) {
    return new MapOutputWriterCommitMessage(partitionLengths, Optional.empty());
  }

  public static MapOutputWriterCommitMessage of(
      long[] partitionLengths, java.util.Optional<BlockManagerId> location) {
    return new MapOutputWriterCommitMessage(partitionLengths, location);
  }

  public long[] getPartitionLengths() {
    return partitionLengths;
  }

  public Optional<BlockManagerId> getLocation() {
    return location;
  }
}
