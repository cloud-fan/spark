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

package org.apache.spark.sql.catalyst.expressions

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.plans.SQLHelper
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.IntegerType

class ExpressionProjectionSuite extends SparkFunSuite with SQLHelper {
  test("Candidate expressions list is prevented from growing exponentially") {
    withSQLConf(SQLConf.MAX_ALIAS_REPLACEMENT_PROJECTED_EXPRESSIONS_LIST_SIZE.key -> "100") {
      val expressions = (1 to 20).map(i =>
        AttributeReference(name = s"expr-$i", dataType = IntegerType)())
      val struct = CreateNamedStruct(expressions)
      val getStructFieldExpressions = (1 to 20).map(GetStructField(struct, _))
      val projectList = expressions ++
        getStructFieldExpressions.map(s => Alias(s, s"alias_${s.name}")())
      val expressionProjection = ExpressionProjection(projectList)
      val expression = CreateNamedStruct(getStructFieldExpressions)
      // Without the threshold, candidate expression list can grow up to 2**20, which will likely
      // cause a test timeout
      assert(expressionProjection.replaceWithAlias(expression).size < 100 * 2)
    }
  }
}
