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

import scala.concurrent.forkjoin._
import java.util.concurrent.atomic.{AtomicInteger, AtomicReference}
import java.util.concurrent.{Future, TimeUnit, CountDownLatch, TimeoutException}
import com.github.mvv.sawoko._

object MultiAsyncExecutor {
  final class UnitForkJoinTask(runnable: Runnable) extends ForkJoinTask[Unit] {
    def getRawResult = ()
    def setRawResult(x: Unit) {}
    def exec = {
      runnable.run
      true
    }
  }

  sealed trait MultiState
  object NotStarted extends MultiState
  object Starting extends MultiState
  object Started extends MultiState
  object Stopping extends MultiState
  object CleaningUp extends MultiState
  object Stopped extends MultiState
}

trait MultiAsyncExecutor extends AsyncExecutorService { self =>
  import MultiAsyncExecutor._

  trait MultiPid { pid: Pid =>
    private[MultiAsyncExecutor]
    var toplevel: Boolean = false
    @volatile
    private[MultiAsyncExecutor]
    var result: Option[Result[self.type, Any]] = None
    private[MultiAsyncExecutor]
    val resultLatch = new CountDownLatch(1)
    private[MultiAsyncExecutor]
    var callback: Callback[X forSome {
                               type X >: self.type <: AsyncExecutor
                             }, _] = null

    final def resultOption = result
    final def isToplevel = toplevel
  }
  type Pid <: MultiPid

  protected final def suspendProcess[X >: self.type <: AsyncExecutor](
                        pid: Pid, callback: Callback[X, _]) {
    pid.callback = callback
  }
  protected final def unsuspendProcess(pid: Pid) {
    pid.callback = null
  }
  protected final def resumeProcess[X >: self.type <: AsyncExecutor, A](
                        pid: Pid)(result: => Result[X, A]) {
    val callback = pid.callback.asInstanceOf[Callback[X, A]]
    pid.callback = null
    val runnable = new Runnable {
      def run = callback(result)
    }
    Thread.currentThread match {
      case thread: ForkJoinWorkerThread =>
        new UnitForkJoinTask(runnable).fork
      case thread =>
        pool.execute(runnable)
    }
  }
  @inline
  protected final def resumeProcess[X >: self.type <: AsyncExecutor, A](
                        pid: Pid, result: Result[X, A]): Unit =
    resumeProcess[X, A](pid)(result)
  @inline
  protected final def scheduleProcess[
                          X >: self.type <: AsyncExecutor, A](
                        pid: Pid, result: Result[X, A])(
                        callback: Callback[X, A]) {
    suspendProcess(pid, callback)
    resumeProcess(pid, result)
  }

  private val multiState = new AtomicReference[MultiState](NotStarted)
  private val multiCounter = new AtomicInteger(0)
  private val multiLatch = new CountDownLatch(1)
  private var pool: ForkJoinPool = null

  protected def onPoolThreadTermination {}

  final def start(parallelism: Int): this.type = {
    if (!multiState.compareAndSet(NotStarted, Starting))
      throw new IllegalStateException
    val threadFactory = new ForkJoinPool.ForkJoinWorkerThreadFactory {
      def newThread(pool: ForkJoinPool) = {
        new ForkJoinWorkerThread(pool) {
          protected override def onStart {
            super.onStart
            setName("Sawoko [" + self + "] :: Worker #" + getPoolIndex)
          }
          protected override def onTermination(cause: Throwable) = {
            try {
              onPoolThreadTermination
            } catch {
              case e: Throwable =>
            }
            super.onTermination(cause)
          }
        }
      }
    }
    try {
      pool = new ForkJoinPool(parallelism, threadFactory)
      pool.setAsyncMode(true)
    } catch {
      case e: Throwable =>
        multiState.set(NotStarted)
        throw e
    }
    try {
      doStart {}
    } catch {
      case e: Throwable =>
        pool.shutdownNow
        pool = null
        multiState.set(NotStarted)
        throw e
    }
    multiCounter.incrementAndGet
    multiState.set(Started)
    this
  }
  final def start = start(Runtime.getRuntime.availableProcessors)

  protected def doStart(rest: => Unit) { rest }
  protected def doStop { }

  private def doShutdown {
    if (!multiState.compareAndSet(Stopping, CleaningUp))
      return
    try {
      doStop
    } catch {
      case e: Throwable =>
    }
    pool.shutdownNow
    pool = null
    multiState.set(Stopped)
    multiLatch.countDown
  }

  final def awaitTermination(timeout: Long, unit: TimeUnit) = {
    multiState.get match {
      case NotStarted | Starting =>
        throw new IllegalArgumentException
      case _ =>
    }
    multiLatch.await(timeout, unit)
  }

  final def initiateShutdown: Future[Unit] = {
    if (multiState.compareAndSet(Started, Stopping)) {
      if (multiCounter.decrementAndGet == 0)
        doShutdown
    } else {
      multiState.get match {
        case NotStarted | Starting =>
          throw new IllegalStateException
        case Started =>
          if (multiState.compareAndSet(Started, Stopping)) {
            if (multiCounter.decrementAndGet == 0)
              doShutdown
          }
        case _ =>
      }
    }
    new Future[Unit] {
      def get: Unit = {
        if (!multiLatch.await(1, TimeUnit.SECONDS)) {
          println("NOPE: " + multiCounter.get)
          get
        }
      }
      def get(timeout: Long, unit: TimeUnit) {
        if (!multiLatch.await(timeout, unit))
          throw new TimeoutException
      }
      def isCancelled = false
      def isDone = multiLatch.getCount == 0
      def cancel(mayInterruptIfRunning: Boolean) = false
    }
  }

  protected def newProcess(in: InPid): Pid

  final def submit[X >: this.type <: AsyncExecutor, A](
              in: InPid, async: Async[X, A]) = {
    if (multiState.get != Started)
      throw new IllegalStateException
    val i = multiCounter.incrementAndGet
    if (i == 1) {
      if (multiCounter.decrementAndGet == 0)
        doShutdown
      throw new IllegalStateException
    }
    val pid = try {
      val pid = newProcess(in)
      pid.toplevel = true
      pool.execute(new Runnable {
        def run = runWithPid(pid, async)
      })
      pid
    } catch {
      case e: Throwable =>
        if (multiCounter.decrementAndGet == 0)
          doShutdown
        throw e
    }
    new Future[Result[X, A]] {
      def get = {
        pid.resultLatch.await
        pid.result.get.asInstanceOf[Result[X, A]]
      }
      def get(timeout: Long, unit: TimeUnit) = {
        if (!multiLatch.await(timeout, unit))
          throw new TimeoutException
        pid.result.get.asInstanceOf[Result[X, A]]
      }
      def isCancelled = false
      def isDone = pid.resultLatch.getCount == 0
      def cancel(mayInterruptIfRunning: Boolean) = false
    }
  }

  protected final def forkNewProcess(newPid: Pid, body: Async[this.type, _]) {
    val count = multiCounter.incrementAndGet
    val task = new UnitForkJoinTask(new Runnable {
      def run = runWithPid(newPid, body)
    })
    task.fork
  }

  protected def doFinished(pid: Pid, result: Result[this.type, Any]) { }
  protected final def finished(pid: Pid, result: Result[this.type, Any]) {
    pid.result = Some(result)
    val count = multiCounter.decrementAndGet
    pid.resultLatch.countDown
    try {
      doFinished(pid, result)
    } catch {
      case e: Throwable =>
    }
    if (count == 0)
      doShutdown
  }

  @inline
  protected final def forceFork[X >: this.type <: AsyncExecutor, A](
                        pid: Pid, callback: Callback[X, A])(
                        body: => Option[Result[X, A]]) = {
    suspendProcess(pid, callback)
    try {
      body.foreach { result =>
        resumeProcess(pid, result)
      }
    } catch {
      case e: Throwable =>
        resumeProcess(pid, Threw(e))
    }
    None
  }
}
