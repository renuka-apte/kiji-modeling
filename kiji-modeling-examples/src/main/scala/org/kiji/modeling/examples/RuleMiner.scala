/**
 * (c) Copyright 2013 WibiData, Inc.
 *
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.kiji.modeling.examples

import org.kiji.express.flow._
import org.kiji.modeling.framework.KijiModelingJob
import com.twitter.scalding.{Args, FieldConversions}
import scala.Some

/**
 * To use this class as part of the foodmart demo, run the following command once your tables are
 * loaded:
 *
 *     express job target/FoodmartRecommendations-1.0-SNAPSHOT.jar org.kiji.foodmart.RuleMiner \
 *         --input-table kiji://.env/macys/sales --input-column purchased_items \
 *         --output-table kiji://.env/macys/product --output-column frequent_itemset_recos
 *
 * To inspect the output:
 *
 *     kiji scan kiji://.env/macys/product/frequent_itemset_recos --max-rows=3
 */
class RuleMiner(args: Args) extends KijiModelingJob(args) with FieldConversions {
  val inputTableUri: String = args("input-table")
  val inputColumn: String = args("input-column")
  val outputTableUri: String = args("output-table")
  val outputColumn: String = args("output-column")
  val minBagSize: Int = args.getOrElse("min-bag-size", "2").toInt
  val maxBagSize: Int = args.getOrElse("max-bag-size", "2").toInt
  val supportThreshold: Double = args.getOrElse("support", "0.0001").toDouble
  

  val totalOrders = KijiInput(inputTableUri, inputColumn -> 'slice)
      .groupAll { _.size('norm)}

  KijiInput(inputTableUri, inputColumn -> 'order)
      // convert the input data in the Kiji table into a form that is required by prepareItemSets
      .map('order -> 'order) {
        order: Seq[FlowCell[String]] => order.map { _.qualifier }.toList
      }
      // generic frequent itemset mining steps
      .prepareItemSets[String]('order -> 'itemset, minBagSize, maxBagSize)
      .support('itemset -> 'support, Some(totalOrders), None, 'norm)
      .filter('support) { support: Double => support >= supportThreshold }
      // convert support to the datatype required by the table
      .map('support -> 'support) { support: Double => support.toString }
      // pivot the frequent itemsets into per product recommendations
      // some form of this step will be required any time you would like to convert frequent
      // itemsets into per-product recommendations.
      .flatMap('itemset -> ('entityId, 'recommendation)) {
        // for every frequent itemset {a, b} pivot it into rules of the form "recommend a when you
        // see b" and "recommend b when you see a"
        itemset: String => {
          val products = itemset.split(",")
          List((EntityId(products(0)), products(1)), (EntityId(products(1)), products(0)))
        }
      }
      // write it to the appropriate column in the product table
      .write(KijiOutput(outputTableUri, Map('support ->
          ColumnFamilyOutputSpec(outputColumn, 'recommendation))))
}
