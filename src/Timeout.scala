/*
 * Copyright (C) 2011 Mikhail Vorozhtsov
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

package com.github.mvv.sawoko

import java.util.concurrent.TimeUnit

sealed trait Timeout extends Ordered[Timeout] {
  val timeout: Long
  val unit: TimeUnit
  @inline
  final def isInf = timeout < 0
  final def base =
    if (timeout <= 0)
      new RelativeTimeout(0, this)
    else
      new RelativeTimeout(System.nanoTime, this)
  def -(that: Timeout): Timeout
  def to(unit: TimeUnit): Timeout
  def toNanos: Long
  final def compare(that: Timeout) =
    if (isInf)
      if (that.isInf) 0 else -1
    else if (that.isInf)
      1
    else {
      val m = min(unit, that.unit)
      m.convert(timeout, unit) compare m.convert(that.timeout, that.unit)
    }
  final override def equals(that: Any) = that match {
    case that: Timeout => compare(that) == 0
    case _ => false
  }
}
final class NonNegativeTimeout(val timeout: Long, val unit: TimeUnit)
            extends Timeout {
  require(timeout >= 0)
  def -(that: Timeout) =
    if (that.isInf)
      Timeout.Now
    else {
      val m = min(unit, that.unit)
      val t1 = m.convert(timeout, unit)
      val t2 = m.convert(that.timeout, that.unit)
      if (t2 >= t1) Timeout.Now
      else Timeout(t1 - t2, m)
    }
  def to(unit: TimeUnit) = Timeout(unit.convert(timeout, this.unit), unit)
  def toNanos = unit.toNanos(timeout)
  override def hashCode = unit.toNanos(timeout).intValue
  override def toString = "Timeout(" + timeout + ", " + unit + ")"
}
object Timeout {
  private final class InfTimeout(val unit: TimeUnit) extends Timeout {
    val timeout = -1L
    def -(that: Timeout) = if (that.isInf) Now else this
    def to(unit: TimeUnit) = new InfTimeout(unit)
    def toNanos = -1L
    override def hashCode = -1
    override def toString = "Timeout(Inf)"
  }
  val Inf: Timeout = new InfTimeout(TimeUnit.MILLISECONDS)
  val Now = Timeout(0, TimeUnit.NANOSECONDS)

  def apply(timeout: Long, unit: TimeUnit) =
    new NonNegativeTimeout(timeout, unit)

  final class LongTimeouts(x: Long) {
    def ns = Timeout(x, TimeUnit.NANOSECONDS)
    def us = Timeout(x, TimeUnit.MICROSECONDS)
    def ms = Timeout(x, TimeUnit.MILLISECONDS)
    def s = Timeout(x, TimeUnit.SECONDS)
    def m = Timeout(x, TimeUnit.MINUTES)
    def h = Timeout(x, TimeUnit.HOURS)
    def d = Timeout(x, TimeUnit.DAYS)
  }
}

final class RelativeTimeout(private val base: Long, val timeout: Timeout) {
  def adjust =
    if (timeout.timeout <= 0)
      this
    else {
      val now = System.nanoTime
      val newTimeout = timeout - Timeout(now - base, TimeUnit.NANOSECONDS)
      new RelativeTimeout(now, newTimeout)
    }
}
