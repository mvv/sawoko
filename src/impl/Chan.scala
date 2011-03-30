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

sealed trait Chan[A]
object MultiChanOps extends ChanOps[Chan]

trait ChanMultiAsyncExecutor
      extends MultiAsyncExecutor
         with CreateChanAsyncExecutor[Chan]
         with WriteChanAsyncExecutor[Chan] {
    self: WaitManyTimeoutMultiAsyncExecutor[
            ReadChanWaitCap[Chan] with WriteChanSyncWaitCap[Chan]] =>

  private class ChanImpl[A] extends Chan[A] {
    private[ChanMultiAsyncExecutor]
    val lock = new AnyRef
    private[ChanMultiAsyncExecutor]
    val readQueue = new AltQueue[WaitOpResumePoint[A]]
    private[ChanMultiAsyncExecutor]
    val writeQueue = new AltQueue[(A, Option[WaitOpResumePoint[Unit]])]
  }

  def registerCreateChan[A](
        pid: Pid, callback: SimpleCallback[Chan[A]]) =
    Some(Success(new ChanImpl[A]: Chan[A]))

  def registerWriteChan[A](
        pid: Pid, out: Chan[A], value: A,
        callback: SimpleCallback[Unit]): Option[SimpleResult[Unit]] =
    forceFork(pid, callback) {
      val chan = out.asInstanceOf[ChanImpl[A]]
      chan.lock.synchronized {
        if (chan.writeQueue.isEmpty)
          chan.readQueue.take.foreach { point =>
            if (point.resume(value))
              return Some(Success(()))
          }
        chan.writeQueue.push(value -> None)
      }
      Some(Success(()))
    }

  private class ChanOpCanceller[A](
                  chan: ChanImpl[Any],
                  node: Either[
                          AltQueue.Node[WaitOpResumePoint[Any]],
                          AltQueue.Node[
                            (Any, Option[WaitOpResumePoint[Unit]])]])
                extends WaitOpCanceller {
    val handlesTimeout = false
    def cancel {
      chan.lock.synchronized {
        node match {
          case Left(node) => chan.readQueue.remove(node)
          case Right(node) => chan.writeQueue.remove(node)
        }
        chan.writeQueue.peek.foreach {
          case (value, None) =>
            chan.readQueue.take.foreach { readPoint =>
              if (readPoint.resume(value))
                chan.writeQueue.take
            }
          case (value, Some(writePoint)) =>
            chan.readQueue.peek.foreach { readPoint =>
              readPoint.resume(value, writePoint, ()) match {
                case (false, true) =>
                  chan.readQueue.take
                case (true, false) =>
                  chan.writeQueue.take
                case _ =>
                  chan.readQueue.take
                  chan.writeQueue.take
              }
            }
        }
      }
    }
  }

  registerWaitOpHandler {
    new WaitOpHandler[WriteChanSync[Chan, Any]] {
      val hasCap = implicitly[HasCap]
      def check(pid: Pid, op: WriteChanSync[Chan, Any]): Option[Unit] = {
        val chan = op.out.chan.asInstanceOf[ChanImpl[Any]]
        chan.lock.synchronized {
          if (chan.writeQueue.isEmpty)
            chan.readQueue.take.foreach { readPoint =>
              if (readPoint.resume(op.value))
                return Some(())
            }
        }
        None
      }
      def register(point: WaitOpResumePoint[Unit],
                   op: WriteChanSync[Chan, Any]): WaitOpCanceller = {
        val chan = op.out.chan.asInstanceOf[ChanImpl[Any]]
        val node = chan.lock.synchronized {
          if (chan.writeQueue.isEmpty)
            chan.readQueue.peek match {
              case Some(readPoint) =>
                readPoint.resume(op.value, point, ()) match {
                  case (false, true) =>
                    chan.readQueue.take
                    Some(chan.writeQueue.push(op.value -> Some(point)))
                  case (readIsOk, writeIsOk) =>
                    if (readIsOk == writeIsOk)
                      chan.readQueue.take
                    None
                }
              case None =>
                Some(chan.writeQueue.push(op.value -> Some(point)))
            }
          else
            Some(chan.writeQueue.push(op.value -> Some(point)))
        }
        node match {
          case Some(node) => new ChanOpCanceller(chan, Right(node))
          case None => WaitOpCanceller.Noop
        }
      }
    }
  }
  registerWaitOpHandler {
    new WaitOpHandler[ReadChan[Chan, Any]] {
      val hasCap = implicitly[HasCap]
      def check(pid: Pid, op: ReadChan[Chan, Any]): Option[Any] = {
        val chan = op.in.chan.asInstanceOf[ChanImpl[Any]]
        chan.lock.synchronized {
          if (chan.readQueue.isEmpty)
            chan.writeQueue.take match {
              case Some((value, None)) =>
                return Some(value)
              case Some((value, Some(writePoint))) =>
                if (writePoint.resume(()))
                  return Some(value)
              case None =>
            }
        }
        None
      }
      def register(point: WaitOpResumePoint[Any],
                   op: ReadChan[Chan, Any]): WaitOpCanceller = {
        val chan = op.in.chan.asInstanceOf[ChanImpl[Any]]
        val node = chan.lock.synchronized {
          if (chan.readQueue.isEmpty)
            chan.writeQueue.peek match {
              case Some((value, None)) =>
                if (point.resume(value))
                  chan.writeQueue.take
                None
              case Some((value, Some(writePoint))) =>
                point.resume(value, writePoint, ()) match {
                  case (false, writeIsOk) =>
                    if (!writeIsOk)
                      chan.writeQueue.take
                    None
                  case (true, false) =>
                    chan.writeQueue.take
                    Some(chan.readQueue.push(point))
                  case (true, true) =>
                    chan.writeQueue.take
                    None
                }
              case None =>
                Some(chan.readQueue.push(point))
            }
          else
            Some(chan.readQueue.push(point))
        }
        node match {
          case Some(node) => new ChanOpCanceller(chan, Left(node))
          case None => WaitOpCanceller.Noop
        }
      }
    }
  }
}
