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

sealed trait SignalGuard[S[_]] extends NotNull

final class InSignal[S[_], A](private val sig: S[A]) {
  override def hashCode = sig.hashCode
  override def equals(that: Any) = that match {
    case that: AnyRef if that.getClass == getClass =>
      sig == that.asInstanceOf[InSignal[S, _]].sig
    case _ =>
      false
  }
  def signal(implicit guard: SignalGuard[S]) = sig
}
object InSignal {
  def apply[S[_], A](signal: S[A]) = new InSignal[S, A](signal)
  implicit def signalToInSignal[S[_], A](signal: S[A]) =
    new InSignal[S, A](signal)
}
final class OutSignal[S[_], A](private val sig: S[A]) {
  override def hashCode = sig.hashCode
  override def equals(that: Any) = that match {
    case that: AnyRef if that.getClass == getClass =>
      sig == that.asInstanceOf[OutSignal[S, _]].sig
    case _ =>
      false
  }
  def signal(implicit guard: SignalGuard[S]) = sig
}
object OutSignal {
  def apply[S[_], A](signal: S[A]) = new OutSignal[S, A](signal)
  implicit def signalToOutSignal[S[_], A](signal: S[A]) =
    new OutSignal[S, A](signal)
}

trait CreateSignalAsyncExecutor[S[_]] extends AsyncExecutor {
  def registerCreateSignal[A](
        pid: Pid, callback: SimpleCallback[S[A]]): Option[SimpleResult[S[A]]]
}

final class CreateSignalOp[S[_], A]
            extends AsyncOp[CreateSignalAsyncExecutor[S], S[A]] {
  def register(ep: EP, callback: Callback) =
    ep.executor.registerCreateSignal(ep.pid, callback)
}

trait AwaitSignalWaitCap[S[_]] extends WaitCap

final case class AwaitSignal[S[_], A](in: InSignal[S, A]) extends WaitOp {
  type Cap = AwaitSignalWaitCap[S]
  type Result = A
}

trait EmitSignalAsyncExecutor[S[_]] extends AsyncExecutor {
  protected implicit val signalGuard = new SignalGuard[S] {}
  def registerEmitSignal[A](
        pid: Pid, out: S[A], value: A,
        callback: SimpleCallback[Boolean]): Option[SimpleResult[Boolean]]
}

final case class EmitSignalOp[S[_], A](out: OutSignal[S, A], value: A)
                 extends AsyncOp[EmitSignalAsyncExecutor[S], Boolean] {
  private implicit val signalGuard = new SignalGuard[S] {}
  def register(ep: EP, callback: Callback) =
    ep.executor.registerEmitSignal(ep.pid, out.signal, value, callback)
}

trait SignalOps[S[_]] {
  import AsyncOps._
  import WaitOps._

  @inline
  def createSignal[A] =
    exec(new CreateSignalOp[S, A])
  @inline
  def awaitSignal[S[_], A](in: InSignal[S, A]) =
    waitOne(AwaitSignal(in))
  @inline
  def awaitSignal[S[_], A](in: InSignal[S, A], timeout: Timeout) =
    waitOne(AwaitSignal(in), timeout)
  @inline
  def emitSignal[S[_], A](out: OutSignal[S, A], value: A) =
    exec(EmitSignalOp(out, value))
  @inline
  def emitSignal[S[_]](out: OutSignal[S, Unit]) =
    exec(EmitSignalOp(out, ()))
}
