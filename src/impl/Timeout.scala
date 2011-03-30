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

import scala.collection.immutable.TreeMap
import java.util.concurrent.{LinkedBlockingQueue, TimeUnit}
import java.util.concurrent.atomic.AtomicInteger

object TimeoutMultiAsyncExecutor {
  private sealed trait TimeoutRequest
  private final case class AddTimeoutRequest(ts: Long, handler: () => _)
                           extends TimeoutRequest
  private final case class CancelTimeoutRequest(ts: Long, handler: () => _)
                           extends TimeoutRequest
  private object QuitTimeoutRequest extends TimeoutRequest
}

trait TimeoutMultiAsyncExecutor extends MultiAsyncExecutor {
  import TimeoutMultiAsyncExecutor._

  private val timeoutQueue = new LinkedBlockingQueue[TimeoutRequest]
  private def newTimeoutThread = new Thread {
    private var timeoutCancelled = Set.empty[() => _]
    private var timeoutHandlers = TreeMap.empty[Long, Set[() => _]]
    override def run {
      var timeout = 0L
      while (true) {
        val request = if (timeout == 0L) timeoutQueue.take
                      else timeoutQueue.poll(timeout, TimeUnit.NANOSECONDS)
        request match {
          case null =>
          case AddTimeoutRequest(ts, handler) =>
            if (timeoutCancelled.contains(handler))
              timeoutCancelled -= handler
            else
              timeoutHandlers += ts -> (timeoutHandlers.get(ts) match {
                case Some(handlers) => handlers + handler
                case None => Set(handler)
              })
          case CancelTimeoutRequest(ts, handler) =>
            timeoutHandlers = timeoutHandlers.get(ts) match {
              case Some(handlers) =>
                val hs = handlers - handler
                if (hs.isEmpty)
                  timeoutHandlers - ts
                else
                  timeoutHandlers + (ts -> hs)
              case None =>
                timeoutCancelled += handler
                timeoutHandlers
            }
          case QuitTimeoutRequest =>
            timeoutHandlers = TreeMap.empty
            timeoutCancelled = Set.empty
            timeoutQueue.clear
            return
        }
        var now = 0L
        timeoutHandlers = timeoutHandlers.dropWhile { case (ts, handlers) =>
          val expired = if (ts > now) {
                          now = System.nanoTime
                          ts <= now
                        } else
                          true
          if (expired) {
            handlers.foreach { handler =>
              try { handler() } catch { case _ => }
            }
            true
          } else {
            timeout = ts - now
            false
          }
        }
        if (timeoutHandlers.isEmpty)
          timeout = 0L
      }
    }
  }

  protected override def doStart(rest: => Unit) = super.doStart {
    val timeoutThread = newTimeoutThread
    timeoutThread.setName("Sawoko [" + this + "] :: Timeouts handler")
    timeoutThread.start
    try {
      rest
    } catch {
      case e: Throwable =>
        timeoutQueue.put(QuitTimeoutRequest)
        timeoutThread.join
        throw e
    }
  }

  protected override def doStop {
    timeoutQueue.put(QuitTimeoutRequest)
    super.doStop
  }

  protected final class TimeoutHandler private[TimeoutMultiAsyncExecutor] (
                          val ts: Long,
                          private val body: () => Unit) {
    private val condition = new AtomicInteger(0)
    def register = {
      if (condition.compareAndSet(0, 1))
        timeoutQueue.put(AddTimeoutRequest(ts, body))
      this
    }
    def cancel = {
      if (!condition.compareAndSet(0, 2))
        timeoutQueue.put(CancelTimeoutRequest(ts, body))
      this
    }
  }
  protected final def createTimeoutHandler[T](ts: Long)(body: => T) =
    new TimeoutHandler(ts, () => body)
  protected final def registerTimeoutHandler[T](ts: Long)(body: => T) =
    new TimeoutHandler(ts, () => body).register
}
