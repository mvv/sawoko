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

import scala.annotation.unchecked.uncheckedVariance
import scala.collection.mutable.{Buffer, ListBuffer}
import java.util.concurrent.atomic.{AtomicInteger, AtomicReference}
import com.github.mvv.sawoko._

object WaitManyTimeoutMultiAsyncExecutor {
  sealed trait ImmediateResult[+A]
  object ImmediateResult {
    object Invalid extends ImmediateResult[Nothing]
    object Undefined extends ImmediateResult[Nothing]
    sealed trait Defined[+A] extends ImmediateResult[A]
    object TimedOut extends Defined[Nothing]
    sealed trait Indexed[+A] extends Defined[A] {
      val index: Int
      def either: Either[Throwable, A]
    }
    object Indexed {
      def unapply[A](indexed: Indexed[A]) = Some(indexed.index)
    }
    final case class Success[+A](index: Int, value: A) extends Indexed[A] {
      def either = Right(value)
    }
    final case class Failure(index: Int, cause: Throwable)
                     extends Indexed[Nothing] {
      def either = Left(cause)
    }
  }
}

trait WaitManyTimeoutMultiAsyncExecutor[+P <: WaitCap]
      extends TimeoutMultiAsyncExecutor
         with WaitManyTimeoutAsyncExecutor[P] {
  import WaitManyTimeoutMultiAsyncExecutor._

  private val waitProcessSerial = new AtomicInteger(0)

  trait WaitPid extends MultiPid { pid: Pid =>
    final val serial = waitProcessSerial.incrementAndGet
    @volatile
    private[WaitManyTimeoutMultiAsyncExecutor]
    var waitCounter = 0
    private[WaitManyTimeoutMultiAsyncExecutor]
    val waitLock = new AnyRef
    private[WaitManyTimeoutMultiAsyncExecutor]
    val waitImmediateResult: AtomicReference[ImmediateResult[Any]] =
      new AtomicReference(ImmediateResult.Invalid)
  }
  type Pid <: WaitPid

  protected final class WaitOpResumePoint[-A] private[WaitManyTimeoutMultiAsyncExecutor] (
                          val pid: Pid,
                          private val serial: Int,
                          private val resultIndex: Int,
                          private val conv: A => Any,
                          val expiresAt: Long,
                          private val cancellers: Buffer[WaitOpCanceller],
                          private val timeoutHandler: TimeoutHandler) {
    def resumeWith(result: Either[Throwable, A]): Boolean = {
      if (pid.waitCounter != serial)
        return false
      pid.waitLock.synchronized {
        if (pid.waitCounter == serial)
          pid.waitCounter = serial + 1
        else
          return false
      }
      val expired = expiresAt >= 0 && System.nanoTime > expiresAt
      val ir = if (expired) ImmediateResult.TimedOut
               else result match {
                 case Right(result) =>
                   ImmediateResult.Success(resultIndex, result)
                 case Left(cause) =>
                   ImmediateResult.Failure(resultIndex, cause)
               }
      if (!pid.waitImmediateResult.compareAndSet(
             ImmediateResult.Undefined, ir))
        resumeProcess(pid) {
          timeoutHandler.cancel
          if (expired)
            cancellers.view.zipWithIndex.foreach { case (canceller, i) =>
              if (i != resultIndex && !canceller.handlesTimeout)
                try { canceller.cancel } catch { case _ => }
            }
          else
            cancellers.view.zipWithIndex.foreach { case (canceller, i) =>
              if (i != resultIndex)
                try { canceller.cancel } catch { case _ => }
            }
          cancellers.clear
          if (expired)
            Success(None)
          else result match {
            case Right(result) =>
              Success(Some(conv(result)))
            case Left(cause) =>
              Threw(WaitOpException(resultIndex, cause))
          }
        }
      !expired
    }
    def resumeWith[B](result: Either[Throwable, A],
                      point1: WaitOpResumePoint[B],
                      result1: Either[Throwable, B]): (Boolean, Boolean) = {
      val pid1 = point1.pid
      val serial1 = point1.serial
      if (expiresAt >= 0 || point1.expiresAt >= 0) {
        val now = System.nanoTime
        val expired = expiresAt >= 0 && now > expiresAt
        val expired1 = point1.expiresAt >= 0 && now > point1.expiresAt
        if (expired) {
          val success = pid.waitLock.synchronized {
            if (pid.waitCounter == serial) {
              pid.waitCounter = serial + 1
              true
            } else
              false
          }
          if (success &&
              !pid.waitImmediateResult.compareAndSet(
                 ImmediateResult.Undefined, ImmediateResult.TimedOut))
            resumeProcess(pid) {
              timeoutHandler.cancel
              cancellers.view.zipWithIndex.foreach { case (canceller, i) =>
                if (i != resultIndex && !canceller.handlesTimeout)
                  try { canceller.cancel } catch { case _ => }
              }
              cancellers.clear
              Success(None)
            }
        }
        if (expired1) {
          val success = pid1.waitLock.synchronized {
            if (pid1.waitCounter == serial1) {
              pid1.waitCounter = serial1 + 1
              true
            } else
              false
          }
          if (success &&
              !pid1.waitImmediateResult.compareAndSet(
                 ImmediateResult.Undefined, ImmediateResult.TimedOut))
            resumeProcess(pid1) {
              point1.timeoutHandler.cancel
              point1.cancellers.view.zipWithIndex.foreach {
                case (canceller, i) =>
                  if (i != point1.resultIndex && !canceller.handlesTimeout)
                    try { canceller.cancel } catch { case _ => }
              }
              point1.cancellers.clear
              Success(None)
            }
        }
        if (expired || expired1)
          return (!expired, !expired1)
      }
      if (pid.serial < pid1.serial) {
        pid.waitLock.synchronized {
          if (pid.waitCounter != serial)
            return (false, true)
          pid1.waitLock.synchronized {
            if (pid1.waitCounter != serial1)
              return (true, false)
            pid1.waitCounter = serial1 + 1
          }
          pid.waitCounter = serial + 1
        }
      } else {
        pid1.waitLock.synchronized {
          if (pid1.waitCounter != serial1)
            return (true, false)
          pid.waitLock.synchronized {
            if (pid.waitCounter != serial)
              return (false, true)
            pid.waitCounter = serial + 1
          }
          pid1.waitCounter = serial1 + 1
        }
      }
      if (!pid.waitImmediateResult.compareAndSet(
             ImmediateResult.Undefined, result match {
               case Right(result) =>
                 ImmediateResult.Success(resultIndex, result)
               case Left(cause) =>
                 ImmediateResult.Failure(resultIndex, cause)
             }))
        resumeProcess(pid) {
          timeoutHandler.cancel
          cancellers.view.zipWithIndex.foreach { case (canceller, i) =>
            if (i != resultIndex)
              try { canceller.cancel } catch { case _ => }
          }
          cancellers.clear
          result match {
            case Right(result) => Success(Some(conv(result)))
            case Left(cause) => Threw(WaitOpException(resultIndex, cause))
          }
        }
      if (!pid1.waitImmediateResult.compareAndSet(
             ImmediateResult.Undefined, result1 match {
               case Right(result) =>
                 ImmediateResult.Success(point1.resultIndex, result)
               case Left(cause) =>
                 ImmediateResult.Failure(point1.resultIndex, cause)
             }))
        resumeProcess(pid1) {
          point1.timeoutHandler.cancel
          point1.cancellers.view.zipWithIndex.foreach { case (canceller, i) =>
            if (i != point1.resultIndex)
              try { canceller.cancel } catch { case _ => }
          }
          point1.cancellers.clear
          result1 match {
            case Right(result) =>
              Success(Some(point1.conv(result)))
            case Left(cause) =>
              Threw(WaitOpException(point1.resultIndex, cause))
          }
        }
      (true, true)
    }
    def resume(result: A) =
      resumeWith(Right(result))
    def resume[B](result: A, point1: WaitOpResumePoint[B], result1: B) =
      resumeWith(Right(result), point1, Right(result1))
    def failed(cause: Throwable) =
      resumeWith(Left(cause))
  }

  protected trait WaitOpCanceller {
    val handlesTimeout: Boolean
    def cancel: Unit
  }
  protected object WaitOpCanceller {
    object Noop extends WaitOpCanceller {
      val handlesTimeout = true
      def cancel {}
    }
  }

  protected trait WaitOpHandler[W <: WaitOp] {
    final type HasCap = P <::< W#Cap
    val hasCap: (P @uncheckedVariance) <::< W#Cap
    def check(pid: Pid, op: W): Option[W#Result]
    def register(point: WaitOpResumePoint[W#Result], op: W): WaitOpCanceller
  }

  private var waitOpHandlers = Map.empty[Class[_], WaitOpHandler[_]]

  protected final def registerWaitOpHandler[W <: WaitOp : ClassManifest](
                        handler: WaitOpHandler[W]) {
    val clazz = classManifest[W].erasure
    require(!waitOpHandlers.contains(clazz))
    waitOpHandlers += (clazz -> handler)
  }

  def registerWaitManyTimeout[L <: WaitOpList](
        pid: Pid, ops: L, timeout: Timeout,
        callback: SimpleCallback[Option[WaitOpsResult[L#Results]]])(
        implicit w: P <::< L#Cap) = forceFork(pid, callback) {
    val handlers = waitOpHandlers
    if (timeout.timeout == 0) {
      val it = ops.items.iterator.map { item =>
        handlers.get(item.op.getClass) match {
          case Some(handler) =>
            try {
              handler.asInstanceOf[WaitOpHandler[item.Op]].
                  check(pid, item.op).map { result =>
                Success(Some(item.toResult(result)))
              }
            } catch {
              case e: Throwable =>
                Some(Threw(WaitOpException(item.index, e)))
            }
          case None =>
            Some(Threw(WaitOpException(item.index,
                                       new ImplContractViolationException)))
        }
      } .dropWhile(_.isEmpty).take(1)
      if (it.hasNext) it.next
      else Some(Success(None))
    } else {
      val expiresAt = if (timeout.isInf) -1
                      else System.nanoTime + timeout.toNanos
      val serial = pid.waitCounter
      val cancellers = new ListBuffer[WaitOpCanceller]
      val timeoutHandler = createTimeoutHandler(expiresAt) {
        if (pid.waitCounter != serial &&
            pid.waitLock.synchronized {
              if (pid.waitCounter == serial) {
                pid.waitCounter = serial + 1
                true
              } else
                false
            }) {
          cancellers.foreach { canceller =>
            if (!canceller.handlesTimeout)
              try { canceller.cancel } catch { case _ => }
          }
          cancellers.clear
          resumeProcess(pid, Success(None))
        }
      }
      var registerTimeout = false
      pid.waitImmediateResult.set(ImmediateResult.Undefined)
      val items = ops.items
      items.iterator.map { item =>
        if (item.index > 0 &&
            pid.waitImmediateResult.get != ImmediateResult.Undefined)
          false
        else {
          val point = new WaitOpResumePoint[item.Result](
                            pid, serial, item.index, item.toResult(_),
                            expiresAt, cancellers, timeoutHandler)
          handlers.get(item.op.getClass) match {
            case Some(handler) =>
              try {
                val canceller = handler.asInstanceOf[WaitOpHandler[item.Op]].
                                  register(point, item.op)
                cancellers += canceller
                if (!canceller.handlesTimeout)
                  registerTimeout = true
                true
              } catch {
                case e: Throwable =>
                  point.failed(e)
                  false
              }
            case None =>
              point.failed(new ImplContractViolationException)
              false
          }
        }
      } .dropWhile(identity[Boolean]).hasNext
      pid.waitImmediateResult.getAndSet(ImmediateResult.Invalid) match {
        case result @ ImmediateResult.Indexed(index) =>
          cancellers.view.zipWithIndex.foreach { case (canceller, i) =>
            if (i != index)
              try { canceller.cancel } catch { case _ => }
          }
          cancellers.clear
          result.either match {
            case Right(result) =>
              val item = items(index)
              val r = item.toResult(result.asInstanceOf[item.Result])
              Some(Success(Some(r)))
            case Left(cause) =>
              Some(Threw(WaitOpException(index, cause)))
          }
        case ImmediateResult.TimedOut =>
          cancellers.foreach { canceller =>
            try { canceller.cancel } catch { case _ => }
          }
          cancellers.clear
          Some(Success(None))
        case _ =>
          if (expiresAt >= 0 && registerTimeout)
            timeoutHandler.register
          None
      }
    }
  }
}
