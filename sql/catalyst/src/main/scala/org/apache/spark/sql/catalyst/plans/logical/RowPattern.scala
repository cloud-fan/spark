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

package org.apache.spark.sql.catalyst.plans.logical

// ============================================================================
// Row Pattern AST for MATCH_RECOGNIZE PATTERN clause
// ============================================================================

/**
 * Base trait for row patterns in MATCH_RECOGNIZE.
 *
 * The pattern AST can represent patterns like:
 * - Simple: A B C
 * - With quantifiers: A+ B* C?
 * - With bounded repetition: A{2,5}
 * - With alternation: (A | B) C
 * - With grouping: (A B)+ C
 */
sealed trait RowPattern extends Serializable {
  /**
   * Returns all pattern variable names referenced in this row pattern.
   */
  def variableNames: Set[String]

  /**
   * Returns true if this pattern can match zero rows (i.e., is optional).
   */
  def canBeEmpty: Boolean

  /**
   * Returns a SQL-like string representation of this pattern.
   */
  def patternString: String
}

/**
 * A reference to a pattern variable (e.g., "A", "DOWN", "UP").
 * This is the leaf node in the pattern AST.
 *
 * @param name The name of the pattern variable
 */
case class PatternVariable(name: String) extends RowPattern {
  override def variableNames: Set[String] = Set(name)
  override def canBeEmpty: Boolean = false
  override def patternString: String = name
}

// ============================================================================
// Pattern Quantifiers
// ============================================================================

/**
 * Quantifiers for pattern repetition.
 */
sealed trait PatternQuantifier extends Serializable {
  def minOccurrences: Int
  def maxOccurrences: Option[Int] // None means unbounded

  def quantifierString: String
}

/** Matches one or more occurrences ('+') */
case object OneOrMore extends PatternQuantifier {
  override def minOccurrences: Int = 1
  override def maxOccurrences: Option[Int] = None
  override def quantifierString: String = "+"
}

/** Matches zero or more occurrences ('*') */
case object ZeroOrMore extends PatternQuantifier {
  override def minOccurrences: Int = 0
  override def maxOccurrences: Option[Int] = None
  override def quantifierString: String = "*"
}

/** Matches zero or one occurrence ('?') */
case object ZeroOrOne extends PatternQuantifier {
  override def minOccurrences: Int = 0
  override def maxOccurrences: Option[Int] = Some(1)
  override def quantifierString: String = "?"
}

/** Matches exactly n occurrences ('{n}') */
case class Exactly(n: Int) extends PatternQuantifier {
  require(n >= 0, "Repetition count must be non-negative")
  override def minOccurrences: Int = n
  override def maxOccurrences: Option[Int] = Some(n)
  override def quantifierString: String = s"{$n}"
}

/** Matches between min and max occurrences ('{min,max}') */
case class Between(min: Int, max: Int) extends PatternQuantifier {
  require(min >= 0, "Minimum repetition must be non-negative")
  require(max >= min, "Maximum must be >= minimum")
  override def minOccurrences: Int = min
  override def maxOccurrences: Option[Int] = Some(max)
  override def quantifierString: String = s"{$min,$max}"
}

/** Matches at least min occurrences ('{min,}') */
case class AtLeast(min: Int) extends PatternQuantifier {
  require(min >= 0, "Minimum repetition must be non-negative")
  override def minOccurrences: Int = min
  override def maxOccurrences: Option[Int] = None
  override def quantifierString: String = s"{$min,}"
}

// ============================================================================
// Composite Row Patterns
// ============================================================================

/**
 * A quantified pattern (e.g., A+, B*, C?, D{2,5}).
 *
 * @param pattern The pattern to quantify
 * @param quantifier The quantifier specifying repetition
 * @param greedy Whether the quantifier is greedy (true) or reluctant (false)
 */
case class QuantifiedPattern(
    pattern: RowPattern,
    quantifier: PatternQuantifier,
    greedy: Boolean = true) extends RowPattern {

  override def variableNames: Set[String] = pattern.variableNames
  override def canBeEmpty: Boolean = quantifier.minOccurrences == 0
  override def patternString: String = {
    val reluctantSuffix = if (greedy) "" else "?"
    // Wrap composite patterns (sequences, alternations) in parentheses
    val patternStr = pattern match {
      case _: PatternSequence | _: PatternAlternation => s"(${pattern.patternString})"
      case _ => pattern.patternString
    }
    s"$patternStr${quantifier.quantifierString}$reluctantSuffix"
  }
}

/**
 * A sequence of patterns that must match consecutively (e.g., A B C).
 *
 * @param patterns The sequence of patterns
 */
case class PatternSequence(patterns: Seq[RowPattern]) extends RowPattern {
  require(patterns.nonEmpty, "Pattern sequence cannot be empty")

  override def variableNames: Set[String] = patterns.flatMap(_.variableNames).toSet
  override def canBeEmpty: Boolean = patterns.forall(_.canBeEmpty)
  override def patternString: String = patterns.map(_.patternString).mkString(" ")
}

/**
 * An alternation of patterns (e.g., A | B). Matches if any of the alternatives match.
 *
 * @param alternatives The alternative patterns
 */
case class PatternAlternation(alternatives: Seq[RowPattern]) extends RowPattern {
  require(alternatives.size >= 2, "Alternation requires at least 2 alternatives")

  override def variableNames: Set[String] = alternatives.flatMap(_.variableNames).toSet
  override def canBeEmpty: Boolean = alternatives.exists(_.canBeEmpty)
  override def patternString: String = alternatives.map(_.patternString).mkString("(", " | ", ")")
}

