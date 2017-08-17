/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.sources.v2.writer;

/**
 * A writer that is responsible for writing data to a writable data source.
 * The writing procedure is:
 *   1. create the write task, serialize and send it to all the partitions of the input data(RDD).
 *   2. for each partition, write the data with the writer.
 *   3. for each partition, if all data are written successfully, call writer.commit.
 *   4. for each partition, if exception happens during the writing, call writer.abort.
 *        TODO: shall we introduce a retry mechanism instead of calling `abort` immediately when
 *        failure happens?
 *   5. wait until all the writers are finished, i.e., either commit or abort.
 *   6. if all partitions are written successfully, call WritableDataSourceV2.commit.
 *   7. if some partitions failed and aborted, call WritableDataSourceV2.abort.
 *
 * Note that, the implementations are responsible to correctly implement transaction by overwriting
 * the `commit` and `abort` methods of the writer and write task.
 */
public interface DataSourceV2Writer {
  WriteTask createWriteTask();

  void commit(WriterCommitMessage[] messages);

  void abort();
}
