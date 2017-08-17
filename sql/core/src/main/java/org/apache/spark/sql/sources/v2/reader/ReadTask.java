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

package org.apache.spark.sql.sources.v2.reader;

import java.io.Serializable;

/**
 * A read task returned by a data source reader and is responsible for outputting data for an RDD
 * partition.
 */
public interface ReadTask<T> extends Serializable {
  /**
   * The preferred locations for this read task to run faster, but Spark can't guarantee that this
   * task will always run on these locations. Implementations should make sure that it can
   * be run on any location.
   */
  default String[] preferredLocations() {
    return new String[0];
  }

  /**
   * This method will be called before running this read task, users can overwrite this method
   * and put initialization logic here.
   */
  default void open() {}

  /**
   * Proceed to next record, returns false if there is no more records.
   */
  boolean next();

  /**
   * Return the current record. This method should return same value until `next` is called.
   */
  T get();

  /**
   * This method will be called after finishing this read task, users can overwrite this method
   * and put clean up logic here.
   */
  default void close() {}
}
