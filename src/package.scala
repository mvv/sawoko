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

object `package` {
  val R1 = WaitOpResultList.First
  val R2 = R1.next
  val R3 = R2.next
  val R4 = R3.next
  val R5 = R4.next
  val R6 = R5.next
  val R7 = R6.next
  val R8 = R7.next
  val R9 = R8.next
  val R10 = R9.next

  type >::>[+A, -B] = B <::< A

  implicit def intTimeouts(x: Int) = new Timeout.LongTimeouts(x)
  implicit def longTimeouts(x: Long) = new Timeout.LongTimeouts(x)

  implicit object timeUnitOrdering extends Ordering[TimeUnit] {
    def compare(u1: TimeUnit, u2: TimeUnit) =
      if (u1 eq u2) 0
      else  (u1, u2) match {
        case (TimeUnit.NANOSECONDS, _) => -1
        case (_, TimeUnit.NANOSECONDS) => 1
        case (TimeUnit.MICROSECONDS, _) => -1
        case (_, TimeUnit.MICROSECONDS) => 1
        case (TimeUnit.MILLISECONDS, _) => -1
        case (_, TimeUnit.MILLISECONDS) => 1
        case (TimeUnit.SECONDS, _) => -1
        case (_, TimeUnit.SECONDS) => 1
        case (TimeUnit.MINUTES, _) => -1
        case (_, TimeUnit.MINUTES) => 1
        case (TimeUnit.HOURS, _) => -1
        case (_, TimeUnit.HOURS) => 1
        case (TimeUnit.DAYS, _) => -1
        case (_, TimeUnit.DAYS) => 1
      }
  }

  @inline
  def min[A](x: A, y: A)(implicit ord: Ordering[A]) =
    if (ord.compare(x, y) < 0) x else y
}
