---
layout: global
title: Push-based shuffle
license: |
  Licensed to the Apache Software Foundation (ASF) under one or more
  contributor license agreements.  See the NOTICE file distributed with
  this work for additional information regarding copyright ownership.
  The ASF licenses this file to You under the Apache License, Version 2.0
  (the "License"); you may not use this file except in compliance with
  the License.  You may obtain a copy of the License at
 
     http://www.apache.org/licenses/LICENSE-2.0
 
  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
---
* This will become a table of contents (this text will be scraped).
{:toc}

# Push-based shuffle

Push based shuffle is a best-effort approach working with the existing shuffle architecture to convert small random reads into large sequential reads during shuffle effectively improving the overall I/O performance. Each individual partition is assigned with a shuffle merger location (currently external shuffle services in the case of YARN) for merging the blocks of the corresponding reduce partition that gets pushed from the map tasks. Shuffle merger services receive the blocks from different map tasks and merge them into one single file. With this approach, we are converting the small random reads happening on the external shuffle service side into large sequential reads. Currently it is supported only for the YARN cluster manager.

#### Server side properties

<table class="table">
<tr><th>Property Name</th><th>Default</th><th>Meaning</th><th>Since Version</th></tr>
<tr>
  <td><code>spark.shuffle.server.mergedShuffleFileManagerImpl</code></td>
  <td><code>org.apache.spark.network.shuffle.ExternalBlockHandler$NoOpMergedShuffleFileManager</code></td>
  <td>
    Class name of the implementation of MergedShuffleFileManager that merges the blocks pushed to it when push-based shuffle is enabled. By default, push-based shuffle is disabled at a cluster level because this configuration is set to 'org.apache.spark.network.shuffle.ExternalBlockHandler$NoOpMergedShuffleFileManager'. To turn on push-based shuffle at a cluster level, set the configuration to 'org.apache.spark.network.shuffle.RemoteBlockPushResolver'.
  </td>
  <td>3.2.0</td>
</tr>
<tr>
  <td><code>spark.shuffle.server.minChunkSizeInMergedShuffleFile</code></td>
  <td><code>2m</code></td>
  <td>
    The minimum size of a chunk when dividing a merged shuffle file into multiple chunks during push-based shuffle. A merged shuffle file consists of multiple small shuffle blocks. Fetching the complete merged shuffle file in a single response increases the memory requirements for the clients. Instead of serving the entire merged file, the shuffle service serves the merged file in `chunks`. A `chunk` constitutes few shuffle blocks in entirety and this configuration controls how big a chunk can get. A corresponding index file for each merged shuffle file will be generated indicating chunk boundaries.
  </td>
  <td>3.2.0</td>
</tr>
<tr>
  <td><code>spark.shuffle.server.mergedIndexCacheSize</code></td>
  <td><code>100m</code></td>
  <td>
    The size of cache in memory which is used in push-based shuffle for storing merged index files.
  </td>
  <td>3.2.0</td>
</tr>
<tr>
  <td><code>spark.shuffle.server.ioExceptionsThresholdDuringMerge</code></td>
  <td><code>4</code></td>
  <td>
    The threshold for number of IOExceptions while merging shuffle blocks to a shuffle partition. When the number of IOExceptions while writing to merged shuffle data/index/meta file exceed this threshold then the shuffle server will respond back to client to stop pushing shuffle blocks for this shuffle partition.
  </td>
  <td>3.2.0</td> 
</tr>
</table>

#### Client side properties
<table class="table">
<tr><th>Property Name</th><th>Default</th><th>Meaning</th><th>Since Version</th></tr>
<tr>
  <td><code>spark.shuffle.push.enabled</code></td>
  <td><code>false</code></td>
  <td>
    Set to 'true' to enable push-based shuffle on the client side and this works in conjunction with the server side flag spark.shuffle.server.mergedShuffleFileManagerImpl which needs to be set with the appropriate org.apache.spark.network.shuffle.MergedShuffleFileManager implementation for push-based shuffle to be enabled
  </td>
  <td>3.2.0</td>
</tr>
<tr>
  <td><code>spark.shuffle.push.merge.results.timeout</code></td>
  <td><code>10s</code></td>
  <td>
    Specify the max amount of time DAGScheduler waits for the merge results from all remote shuffle services for a given shuffle. DAGScheduler will start to submit following stages if not all results are received within the timeout.
  </td>
  <td>3.2.0</td>
</tr>
<tr>
  <td><code>spark.shuffle.push.merge.finalize.timeout</code></td>
  <td><code>10s</code></td>
  <td>
    Specify the amount of time DAGScheduler waits after all mappers finish for a given shuffle map stage before it starts sending merge finalize requests to remote shuffle services. This allows the shuffle services some extra time to merge as many blocks as possible.
  </td>
 <td>3.2.0</td> 
</tr>
<tr>
  <td><code>spark.shuffle.push.maxRetainedMergerLocations</code></td>
  <td><code>500</code></td>
  <td>
    Maximum number of shuffle push merger locations cached for push based shuffle. Currently, shuffle push merger locations are nothing but external shuffle services which are responsible for handling pushed blocks and merging them and serving merged blocks for later shuffle fetch.
  </td>
  <td>3.2.0</td>
</tr>
<tr>
  <td><code>spark.shuffle.push.mergersMinThresholdRatio</code></td>
  <td><code>0.05</code></td>
  <td>
    The minimum number of shuffle merger locations required to enable push based shuffle for a stage. This is specified as a ratio of the number of partitions in the child stage. For example, a reduce stage which has 100 partitions and uses the default value 0.05 requires at least 5 unique merger locations to enable push based shuffle. Merger locations are currently defined as external shuffle services.
  </td>
 <td>3.2.0</td> 
</tr>
<tr>
  <td><code>spark.shuffle.push.mergersMinStaticThreshold</code></td>
  <td><code>5</code></td>
  <td>
    The static threshold for number of shuffle push merger locations should be available in order to enable push based shuffle for a stage. Note this config works in conjunction with spark.shuffle.push.mergersMinThresholdRatio. Maximum of spark.shuffle.push.mergersMinStaticThreshold and spark.shuffle.push.mergersMinThresholdRatio ratio number of mergers needed to enable push based shuffle for a stage. For eg: with 1000 partitions for the child stage with spark.shuffle.push.mergersMinStaticThreshold as 5 and spark.shuffle.push.mergersMinThresholdRatio set to 0.05, we would need at least 50 mergers to enable push based shuffle for that stage
  <td>3.2.0</td> 
</tr>
<tr>
  <td><code>spark.shuffle.push.numPushThreads</code></td>
  <td><code>none</code></td>
  <td>
    Specify the number of threads in the block pusher pool. These threads assist in creating connections and pushing blocks to remote shuffle services. By default, the threadpool size is equal to the number of spark executor cores.
  </td>
  <td>3.2.0</td>
</tr>
<tr>
  <td><code>spark.shuffle.push.maxBlockSizeToPush</code></td>
  <td><code>1m</code></td>
  <td>
    The max size of an individual block to push to the remote shuffle services. Blocks larger than this threshold are not pushed to be merged remotely. These shuffle blocks will be fetched by the executors in the original manner.
  </td>
  <td>3.2.0</td>
</tr>
<tr>
  <td><code>spark.shuffle.push.maxBlockBatchSize</code></td>
  <td><code>3m</code></td>
  <td>
    The max size of a batch of shuffle blocks to be grouped into a single push request.
  </td>
  <td>3.2.0</td>
</tr>
</table>