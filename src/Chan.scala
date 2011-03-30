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

sealed trait ChanGuard[C[_]] extends NotNull

final class InChan[C[_], A](private val ch: C[A]) {
  override def hashCode = ch.hashCode
  override def equals(that: Any) = that match {
    case that: AnyRef if that.getClass == getClass =>
      ch == that.asInstanceOf[InChan[C, _]].ch
    case _ =>
      false
  }
  def chan(implicit guard: ChanGuard[C]): C[A] = ch
}
object InChan {
  def apply[C[_], A](chan: C[A]) = new InChan[C, A](chan)
  implicit def chanToInChan[C[_], A](chan: C[A]) = new InChan[C, A](chan)
}
final class OutChan[C[_], A](private val ch: C[A]) {
  override def hashCode = ch.hashCode
  override def equals(that: Any) = (that: @unchecked) match {
    case that: AnyRef if that.getClass == getClass =>
      ch == that.asInstanceOf[OutChan[C, _]].ch
    case _ =>
      false
  }
  def chan(implicit guard: ChanGuard[C]): C[A] = ch
}
object OutChan {
  def apply[C[_], A](chan: C[A]) = new OutChan[C, A](chan)
  implicit def chanToOutChan[C[_], A](chan: C[A]) = new OutChan[C, A](chan)
}

trait CreateChanAsyncExecutor[C[_]] extends AsyncExecutor {
  def registerCreateChan[A](
        pid: Pid, callback: SimpleCallback[C[A]]): Option[SimpleResult[C[A]]]
}

final class CreateChanOp[C[_], A]
            extends AsyncOp[CreateChanAsyncExecutor[C], C[A]] {
  def register(ep: EP, callback: Callback) =
    ep.executor.registerCreateChan(ep.pid, callback)
}

trait ReadChanWaitCap[C[_]] extends WaitCap

final case class ReadChan[C[_], A](in: InChan[C, A]) extends WaitOp {
  type Cap = ReadChanWaitCap[C]
  type Result = A
}

trait WriteChanSyncWaitCap[C[_]] extends WaitCap

final case class WriteChanSync[C[_], A](out: OutChan[C, A], value: A)
                 extends WaitOp {
  type Cap = WriteChanSyncWaitCap[C]
  type Result = Unit
}

trait WriteChanAsyncExecutor[C[_]] extends AsyncExecutor {
  protected implicit val chanGuard = new ChanGuard[C] {}
  def registerWriteChan[A](
        pid: Pid, out: C[A], value: A,
        callback: SimpleCallback[Unit]): Option[SimpleResult[Unit]]
}

final case class WriteChanOp[C[_], A](out: OutChan[C, A], value: A)
                 extends AsyncOp[WriteChanAsyncExecutor[C], Unit] {
  private implicit val chanGuard = new ChanGuard[C] {}
  def register(ep: EP, callback: Callback) =
    ep.executor.registerWriteChan(ep.pid, out.chan, value, callback)
}

trait ChanOps[C[_]] {
  import AsyncOps._
  import WaitOps._

  @inline
  def createChan[A] =
    exec(new CreateChanOp[C, A])
  @inline
  def readChan[C[_], A](in: InChan[C, A]) =
    waitOne(ReadChan(in))
  @inline
  def readChan[C[_], A](in: InChan[C, A], timeout: Timeout) =
    waitOne(ReadChan(in), timeout)
  @inline
  def writeChan[C[_], A](out: OutChan[C, A], value: A) =
    exec(WriteChanOp(out, value))
  @inline
  def writeChanSync[C[_], A](out: OutChan[C, A], value: A) =
    waitOne(WriteChanSync(out, value))
  @inline
  def writeChanSync[C[_], A](out: OutChan[C, A], value: A, timeout: Timeout) =
    waitOne(WriteChanSync(out, value), timeout)
}
