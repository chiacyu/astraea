/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.astraea.common.partitioner;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;
import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.PartitionInfo;

public class YourPartitioner implements Partitioner {
  private Map<Integer, Long> partitionLoading = new HashMap<>();

  // get your magic configs
  @Override
  public void configure(Map<String, ?> configs) {}

  /**
   * Compute the partition for the given record.
   *
   * @param topic The topic name
   * @param key The key to partition on (or null if no key)
   * @param keyBytes The serialized key to partition on( or null if no key)
   * @param value The value to partition on or null
   * @param valueBytes The serialized value to partition on or null
   * @param cluster The current cluster metadata
   */
  @Override
  public int partition(
      String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
    List<PartitionInfo> partitions = cluster.availablePartitionsForTopic(topic);
    int partitionNum = partitions.size();
    if (partitionLoading.isEmpty()) {
      IntStream.range(0, partitionNum).forEach(i -> partitionLoading.put(i, Long.valueOf(0)));
    }
    if (!partitions.isEmpty()) {
      int partitionIndex = 0;
      Long currentSum = partitionLoading.get(0).longValue();
      for (int i = 0; i < partitions.size(); i++) {
        if (partitionLoading.get(i).longValue() < currentSum) {
          currentSum = partitionLoading.get(i).longValue();
          partitionIndex = i;
        }
      }
      partitionLoading.put(
          partitionIndex, currentSum + (valueBytes == null ? 0 : valueBytes.length));
      return partitions.get(partitionIndex).partition();
    } else {
      return -1;
    }
  }

  @Override
  public void close() {}
}
