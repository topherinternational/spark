package org.apache.spark.shuffle.api;

import java.util.Optional;

import org.apache.spark.annotation.Private;
import org.apache.spark.storage.BlockManagerId;

@Private
public final class MapOutputWriterCommitMessage {

  private final long[] partitionLengths;
  private final Optional<BlockManagerId> location;

  private MapOutputWriterCommitMessage(
      long[] partitionLengths, Optional<BlockManagerId> location) {
    this.partitionLengths = partitionLengths;
    this.location = location;
  }

  public static MapOutputWriterCommitMessage of(long[] partitionLengths) {
    return new MapOutputWriterCommitMessage(partitionLengths, Optional.empty());
  }

  public static MapOutputWriterCommitMessage of(
      long[] partitionLengths, BlockManagerId location) {
    return new MapOutputWriterCommitMessage(partitionLengths, Optional.of(location));
  }

  public long[] getPartitionLengths() {
    return partitionLengths;
  }

  public Optional<BlockManagerId> getLocation() {
    return location;
  }
}
