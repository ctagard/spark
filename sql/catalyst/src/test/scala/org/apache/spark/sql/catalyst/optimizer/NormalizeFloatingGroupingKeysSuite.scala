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

package org.apache.spark.sql.catalyst.optimizer

import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.dsl.plans._
import org.apache.spark.sql.catalyst.expressions.{Alias, Cast, Expression, InSet, IsNaN}
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.RuleExecutor
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}

class NormalizeFloatingGroupingKeysSuite extends PlanTest {

  object Normalize extends RuleExecutor[LogicalPlan] {
    val batches = Batch("Normalize", Once, NormalizeFloatingGroupingKeys) :: Nil
  }

  object Simplify extends RuleExecutor[LogicalPlan] {
    val batches = Batch("Simplify", Once, SimplifyNormalizedComparisons) :: Nil
  }

  private val x = $"x".double
  private val f = $"f".float
  private val i = $"i".int
  private val relation = LocalRelation(x, f, i)

  private def normalizedKey(key: Expression): Alias =
    Alias(NormalizeFloatingNumbers.normalize(key), "_groupingnormalized")()

  test("compute a float/double grouping key normalized below the aggregate") {
    val key = normalizedKey(x)
    // Outside aggregate functions the key reads the normalized value, and the output keeps its
    // name. Aggregate functions keep reading the raw value.
    comparePlans(
      Normalize.execute(
        relation.groupBy(x, i)(x, i, x.cast(StringType).as("s"), max(x).as("m")).analyze),
      relation.select(x, f, i, key)
        .groupBy(key.toAttribute, i)(
          key.toAttribute.withName("x"), i, key.toAttribute.cast(StringType).as("s"),
          max(x).as("m"))
        .analyze)
  }

  test("rewrite references to the key above the aggregate") {
    val key = normalizedKey(x)
    comparePlans(
      Normalize.execute(relation.groupBy(x)(x).select(x).where(x > 1.0).analyze),
      relation.select(x, f, i, key)
        .groupBy(key.toAttribute)(key.toAttribute.withName("x"))
        .select(key.toAttribute.withName("x"))
        .where(key.toAttribute.withName("x") > 1.0)
        .analyze)
  }

  test("normalize struct keys and leave other keys, streaming plans and repeated runs as is") {
    val s = $"s".struct(StructType(Seq(StructField("d", DoubleType))))
    val structs = LocalRelation(s)
    val key = normalizedKey(s)
    comparePlans(
      Normalize.execute(structs.groupBy(s)(s).analyze),
      structs.select(s, key).groupBy(key.toAttribute)(key.toAttribute.withName("s")).analyze)

    val ints = relation.groupBy(i)(i, max(x).as("m")).analyze
    comparePlans(Normalize.execute(ints), ints)

    val streaming = LocalRelation(Seq(x), isStreaming = true).groupBy(x)(x).analyze
    comparePlans(Normalize.execute(streaming), streaming)

    val once = Normalize.execute(relation.groupBy(x)(x).analyze)
    comparePlans(Normalize.execute(once), once)
  }

  test("don't normalize a key again that a lower aggregate already normalized") {
    val key = normalizedKey(x)
    val upperKey = key.toAttribute.withName("x")
    comparePlans(
      Normalize.execute(relation.groupBy(x, i)(x, i).groupBy(x)(x, max(i).as("m")).analyze),
      relation.select(x, f, i, key)
        .groupBy(key.toAttribute, i)(upperKey, i)
        .groupBy(upperKey)(upperKey, max(i).as("m"))
        .analyze)
  }

  test("remove normalization where comparisons cannot observe it") {
    val normX = NormalizeFloatingNumbers.normalize(x)
    val normF = NormalizeFloatingNumbers.normalize(f)
    comparePlans(
      Simplify.execute(relation.where(
        normX > 1.0 && normX.in(2.0, 3.0) && InSet(normX, Set(4.0)) && IsNaN(normX) &&
          normX.isNotNull && Cast(normF, DoubleType) > 1.5).analyze),
      relation.where(
        x > 1.0 && x.in(2.0, 3.0) && InSet(x, Set(4.0)) && IsNaN(x) && x.isNotNull &&
          Cast(f, DoubleType) > 1.5).analyze)

    // A cast to STRING can tell -0.0 from 0.0, so the normalization stays.
    val castToString = relation.where(normX.cast(StringType) === "-0.0").analyze
    comparePlans(Simplify.execute(castToString), castToString)
  }
}
