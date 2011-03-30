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

sealed trait <::<[-A, +B] extends (A => B) with NotNull { self =>
  final def antisymm[A1 <: A, B1 >: B](rel: B1 <::< A1) = new (A1 =::= B1) {
    def apply(a: A1) = self(a)
    def unapply(b: B1) = rel(b)
  }
  final def trans[C](rel: B <::< C) = new (A <::< C) {
    def apply(a: A) = rel(self(a))
  }
}
object <::< {
  implicit def subtype[A, B >: A] = new (A <::< B) {
    def apply(a: A) = a
  }
  implicit def inject[A, B](std: A <:< B) = new (A <::< B) {
    def apply(a: A) = std(a)
  }
}

sealed trait =::=[A, B] extends (A => B) with NotNull { self =>
  def unapply(b: B): A
  final def symm = new (B =::= A) {
    def apply(b: B) = self.unapply(b)
    def unapply(a: A) = self(a)
  }
  final def trans[C](rel: B =::= C) = new (A =::= C) {
    def apply(a: A) = rel(self(a))
    def unapply(c: C) = self.unapply(rel.unapply(c))
  }
  final def sub = new (A <::< B) {
    def apply(a: A) = self(a)
  }
  final def sup = new (B <::< A) {
    def apply(b: B) = self.unapply(b)
  }
}
object =::= {
  implicit def refl[A] = new (A =::= A) {
    def apply(a: A) = a
    def unapply(a: A) = a
  }
  implicit def inject[A, B](std: A =:= B) = new (A =::= B) {
    def apply(a: A) = std(a)
    def unapply(b: B) = b.asInstanceOf[A]
  }
}
