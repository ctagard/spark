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

import scala.collection.mutable

import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, Cast, EqualNullSafe, EqualTo, Expression, GreaterThan, GreaterThanOrEqual, In, InSet, InSubquery, IsNaN, IsNotNull, IsNull, KnownFloatingPointNormalized, LessThan, LessThanOrEqual, NamedExpression}
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateExpression
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, Filter, GlobalLimit, LocalLimit, LogicalPlan, Project, Sort}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.types.{DoubleType, FloatType}

/**
 * Makes the normalization of float/double grouping keys explicit in the plan.
 *
 * An Aggregate puts -0.0 and 0.0, and all NaNs, in one group and outputs the normalized key (see
 * [[NormalizeFloatingNumbers]]). If that only happens when the Aggregate is planned, the optimizer
 * sees an Aggregate that outputs its grouping keys unchanged, and rules that rely on this (pushing
 * a filter below the Aggregate, removing a redundant Aggregate, replacing it with a Project) can
 * change the result. So this rule computes each normalized key in a Project below the Aggregate,
 * as [[InsertMapSortInAggregate]] does for map keys, and groups by that. Every rule then sees the
 * values the Aggregate outputs. [[SimplifyNormalizedComparisons]] removes the normalization again
 * where it cannot change a result, so that plain comparisons on the keys still reach data sources.
 *
 * Streaming Aggregates are left as they are, so that their state stays compatible with existing
 * checkpoints. Their keys are still normalized when they are planned.
 */
object NormalizeFloatingGroupingKeys extends Rule[LogicalPlan] {

  override def apply(plan: LogicalPlan): LogicalPlan = {
    if (!plan.exists(hasKeysToNormalize)) {
      return plan
    }
    plan.transformUpWithNewOutput {
      case agg: Aggregate if hasKeysToNormalize(agg) =>
        val aliases = mutable.LinkedHashMap.empty[Expression, Alias]
        agg.groupingExpressions.filter(needsNormalization(agg)).foreach { key =>
          aliases.getOrElseUpdate(
            key.canonicalized,
            Alias(NormalizeFloatingNumbers.normalize(key), "_groupingnormalized")())
        }
        def normalized(e: Expression): Option[Attribute] =
          aliases.get(e.canonicalized).map(_.toAttribute)
        // Outside aggregate functions, which keep reading the raw values, a key is replaced with
        // its normalized form.
        def replaceKeys(e: Expression): Expression = e match {
          case _: AggregateExpression => e
          case _ => normalized(e).getOrElse(e.mapChildren(replaceKeys))
        }

        val newAgg = Aggregate(
          agg.groupingExpressions.map(replaceKeys),
          agg.aggregateExpressions.map {
            case a: Attribute => normalized(a).map(_.withName(a.name)).getOrElse(a)
            case other => replaceKeys(other).asInstanceOf[NamedExpression]
          },
          Project(agg.child.output ++ aliases.values, agg.child),
          agg.hint)
        newAgg -> agg.output.zip(newAgg.output)
    }
  }

  private def hasKeysToNormalize(plan: LogicalPlan): Boolean = plan match {
    case agg: Aggregate =>
      !agg.isStreaming && agg.groupingExpressions.exists(needsNormalization(agg))
    case _ => false
  }

  // A key needs normalization unless it is constant or already normalized below the Aggregate,
  // e.g. by this rule for a lower Aggregate on the same key, which also makes the rule idempotent.
  private def needsNormalization(agg: Aggregate)(key: Expression): Boolean = key match {
    case _ if key.foldable || !NormalizeFloatingNumbers.needNormalize(key.dataType) => false
    case a: Attribute => !isNormalized(a, agg.child)
    case _ => true
  }

  // Whether `plan` outputs `a` as a normalized value: computed normalized by a Project, and passed
  // through unchanged by the operators above it.
  private def isNormalized(a: Attribute, plan: LogicalPlan): Boolean = plan match {
    case Project(projectList, child) => projectList.exists {
      case alias @ Alias(_: KnownFloatingPointNormalized, _) => alias.exprId == a.exprId
      case attr: Attribute => attr.exprId == a.exprId && isNormalized(a, child)
      case _ => false
    }
    case agg: Aggregate => agg.aggregateExpressions.exists {
      case attr: Attribute => attr.exprId == a.exprId && isNormalized(a, agg.child)
      case _ => false
    }
    case _: Filter | _: Sort | _: GlobalLimit | _: LocalLimit => isNormalized(a, plan.children.head)
    case _ => false
  }
}

/**
 * Removes float/double normalization (see [[NormalizeFloatingNumbers]]) from the operands of
 * expressions that cannot tell normalized values apart: comparisons, which already treat -0.0 and
 * 0.0 as equal and all NaNs as equal, IN (an IN subquery becomes a join whose keys are normalized),
 * and null and NaN checks. This keeps a comparison on a normalized grouping key in a form that
 * data sources and partition pruning can use once it is pushed below the Aggregate, including a
 * FLOAT key widened to DOUBLE, which keeps -0.0 and NaN.
 */
object SimplifyNormalizedComparisons extends Rule[LogicalPlan] {

  override def apply(plan: LogicalPlan): LogicalPlan = plan.transformAllExpressions {
    case e @ (_: EqualTo | _: EqualNullSafe | _: LessThan | _: LessThanOrEqual | _: GreaterThan |
        _: GreaterThanOrEqual | _: In | _: InSet | _: InSubquery | _: IsNull | _: IsNotNull |
        _: IsNaN) if e.children.exists(isNormalized) =>
      e.mapChildren(removeNormalization)
  }

  private def isNormalized(e: Expression): Boolean = removeNormalization(e) ne e

  private def removeNormalization(e: Expression): Expression = e match {
    case KnownFloatingPointNormalized(NormalizeNaNAndZero(child)) => child
    case cast @ Cast(KnownFloatingPointNormalized(NormalizeNaNAndZero(child)), DoubleType, _, _)
        if child.dataType == FloatType =>
      cast.copy(child = child)
    case _ => e
  }
}
