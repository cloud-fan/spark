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

package org.apache.spark.sql.execution.adaptive

import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.exchange.{BroadcastExchangeExec, Exchange, ReusedExchangeExec, ShuffleExchangeExec}

/**
 * Divide the spark plan into multiple QueryStages. For each Exchange in the plan, it adds a
 * QueryStage and a QueryStageInput. If reusing Exchange is enabled, it finds duplicated exchanges
 * and uses the same QueryStage for all the references. Note this rule must be run after
 * EnsureRequirements rule. The rule divides the plan into multiple sub-trees as QueryStageInput
 * is a leaf node. Transforming the plan after applying this rule will only transform node in a
 * sub-tree.
 */
object PlanQueryStage extends Rule[SparkPlan] {

  def apply(plan: SparkPlan): SparkPlan = {
    val exchangeToQueryStage = new java.util.IdentityHashMap[Exchange, QueryStage]
    val newPlan = plan.transformUp {
      case e: ShuffleExchangeExec =>
        val queryStage = ShuffleQueryStage(e)
        exchangeToQueryStage.put(e, queryStage)
        ShuffleQueryStageInput(queryStage, e.output)
      case e: BroadcastExchangeExec =>
        val queryStage = BroadcastQueryStage(e)
        exchangeToQueryStage.put(e, queryStage)
        BroadcastQueryStageInput(queryStage, e.output)
      // The `ReusedExchangeExec` was added in the rule `ReuseExchange`, via transforming up the
      // query plan. This rule also transform up the query plan, so when we hit `ReusedExchangeExec`
      // here, the exchange being reused must already be hit before and there should be an entry
      // for it in `exchangeToQueryStage`.
      case e: ReusedExchangeExec =>
        exchangeToQueryStage.get(e.child) match {
          case q: ShuffleQueryStage => ShuffleQueryStageInput(q, e.output)
          case q: BroadcastQueryStage => BroadcastQueryStageInput(q, e.output)
        }
    }
    ResultQueryStage(newPlan)
  }
}
