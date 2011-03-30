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

import com.github.mvv.sawoko._

sealed class Tid[+A]
object MultiForkOps extends ForkOps[Tid]

trait ForkMultiAsyncExecutor extends MultiAsyncExecutor
                                with ForkAsyncExecutor[Tid] {
    self: WaitManyTimeoutMultiAsyncExecutor[JoinThreadWaitCap[Tid]]
          with ForkMultiAsyncExecutor =>

  protected class TidImpl(val pid: Pid) extends Tid[Any] {
    override def hashCode = pid.hashCode
    override def equals(that: Any) = that match {
      case that: TidImpl => that.pid == pid
      case _ => false
    }
  }

  trait ForkPid extends WaitPid { pid: Pid =>
    private[ForkMultiAsyncExecutor]
    val tid = new TidImpl(pid)
    private[ForkMultiAsyncExecutor]
    val joinLock = new AnyRef
    private[ForkMultiAsyncExecutor]
    val joinPoints = new AltQueue[WaitOpResumePoint[AsyncResult[Nothing, Any]]]
  }
  type Pid <: ForkPid

  protected override def doFinished(pid: Pid, result: Result[this.type, Any]) {
    val points = pid.joinLock.synchronized(pid.joinPoints.clear)
    points.foreach(_.resume(result))
    super.doFinished(pid, result)
  }

  final def registerCurrentTid(pid: Pid): SimpleResult[Tid[Any]] =
    Success(pid.tid)

  protected def newProcess(parent: Pid): Pid

  final def registerFork[A](
              pid: Pid, body: Async[this.type, A],
              callback: SimpleCallback[Tid[A]]) = {
    val newPid = newProcess(pid)
    forkNewProcess(newPid, body)
    Some(Success(newPid.tid.asInstanceOf[Tid[A]]))
  }

  registerWaitOpHandler {
    new WaitOpHandler[JoinThread[Tid, Any]] {
      val hasCap = implicitly[HasCap]
      def check(pid: Pid, op: JoinThread[Tid, Any]) = {
        val joinPid = op.tid.asInstanceOf[TidImpl].pid
        joinPid.resultOption
      }
      def register(point: WaitOpResumePoint[Result[Nothing, Any]],
                   op: JoinThread[Tid, Any]) = {
        val joinPid = op.tid.asInstanceOf[TidImpl].pid
        if (joinPid == point.pid)
          throw new JoinThreadDeadlockException
        val resultOrNode = joinPid.joinLock.synchronized {
          joinPid.resultOption match {
            case Some(result) =>
              Right(Success(result))
            case None =>
              Left(joinPid.joinPoints.push(point))
          }
        }
        resultOrNode match {
          case Right(result) =>
            point.resume(result)
            WaitOpCanceller.Noop
          case Left(node) =>
            new WaitOpCanceller {
              val handlesTimeout = false
              def cancel {
                joinPid.joinLock.synchronized {
                  joinPid.joinPoints.remove(node)
                }
              }
            }
        }
      }
    }
  }
}
