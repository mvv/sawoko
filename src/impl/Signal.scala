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

package com.github.mvv.sawoko.impl

import java.util.concurrent.atomic.AtomicReference
import com.github.mvv.sawoko._

sealed trait Signal[A]
object MultiSignalOps extends SignalOps[Signal]

trait SignalMultiAsyncExecutor extends MultiAsyncExecutor
                                  with CreateSignalAsyncExecutor[Signal]
                                  with EmitSignalAsyncExecutor[Signal] {
    self: WaitManyTimeoutMultiAsyncExecutor[AwaitSignalWaitCap[Signal]] =>

  private class SignalImpl[A] extends Signal[A] {
    private[SignalMultiAsyncExecutor]
    val emitted = new AtomicReference[Option[A]](None)
    private[SignalMultiAsyncExecutor]
    val lock = new AnyRef
    private[SignalMultiAsyncExecutor]
    val waitPoints = new AltQueue[WaitOpResumePoint[Any]]
    private[SignalMultiAsyncExecutor]
    var waitersCancelled = false
  }

  def registerCreateSignal[A](
        pid: Pid, callback: SimpleCallback[Signal[A]]) =
    Some(Success(new SignalImpl[A]: Signal[A]))

  def registerEmitSignal[A](
        pid: Pid, out: Signal[A], value: A,
        callback: SimpleCallback[Boolean]) = forceFork(pid, callback) {
    val signal = out.asInstanceOf[SignalImpl[A]]
    if (signal.emitted.compareAndSet(None, Some(value))) {
      val points = signal.lock.synchronized {
        signal.waitersCancelled = true
        signal.waitPoints.clear
      }
      points.foreach(_.resume(value))
      Some(Success(true))
    } else
      Some(Success(false))
  }

  private final class SignalOpCanceller(
                        signal: SignalImpl[Any],
                        node: AltQueue.Node[WaitOpResumePoint[Any]])
                      extends WaitOpCanceller {
    val handlesTimeout = false
    def cancel {
      if (signal.emitted.get.isDefined)
        return
      signal.lock.synchronized {
        if (!signal.waitersCancelled)
          signal.waitPoints.remove(node)
      }
    }
  }

  registerWaitOpHandler {
    new WaitOpHandler[AwaitSignal[Signal, Any]] {
      val hasCap = implicitly[HasCap]
      def check(pid: Pid, op: AwaitSignal[Signal, Any]): Option[Any] = {
        val signal = op.in.signal.asInstanceOf[SignalImpl[Any]]
        signal.emitted.get
      }
      def register(point: WaitOpResumePoint[Any],
                   op: AwaitSignal[Signal, Any]): WaitOpCanceller = {
        val signal = op.in.signal.asInstanceOf[SignalImpl[Any]]
        signal.emitted.get match {
          case Some(value) =>
            point.resume(value)
            WaitOpCanceller.Noop
          case _ =>
            val node = signal.lock.synchronized {
              if (signal.waitersCancelled)
                None
              else
                Some(signal.waitPoints.push(point))
            }
            node match {
              case Some(node) =>
                new SignalOpCanceller(signal, node)
              case None =>
                point.resume(signal.emitted.get.get)
                WaitOpCanceller.Noop
            }
        }
      }
    }
  }
}
