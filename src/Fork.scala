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

class JoinThreadDeadlockException extends RuntimeException {
  override def getMessage = "Thread tried to join itself"
}

trait ForkAsyncExecutor[T[+_]] extends AsyncExecutor {
  def registerFork[A](
        pid: Pid, body: Async[this.type, A],
        callback: SimpleCallback[T[A]]): Option[SimpleResult[T[A]]]
  def registerCurrentTid(pid: Pid): SimpleResult[T[Any]]
}

final class ForkAsyncOp[-X <: AsyncExecutor, T[+_], A](
              body: Async[X, A])
            extends AsyncOp[ForkAsyncExecutor[T] with X, T[A]] {
  def register(ep: EP, callback: Callback) =
    ep.executor.registerFork(ep.pid, body, callback)
}

final class CurrentTidOp[T[+_]]
            extends AsyncOp[ForkAsyncExecutor[T], T[Any]] {
  def register(ep: EP, callback: Callback) =
    Some(ep.executor.registerCurrentTid(ep.pid))
}

trait JoinThreadWaitCap[T[+_]] extends WaitCap

final case class JoinThread[T[+_], A](tid: T[A]) extends WaitOp {
  type Cap = JoinThreadWaitCap[T]
  type Result = AsyncResult[Nothing, A]
}

trait ForkOps[T[+_]] {
  import AsyncOps._
  import WaitOps._

  @inline
  def fork[X <: AsyncExecutor, A](body: => Async[X, A]) =
    exec(new ForkAsyncOp[X, T, A](guard(body)))
  @inline
  def currentThread =
    exec(new CurrentTidOp[T])
  @inline
  def joinThread[T[+_], A](tid: T[A]) =
    waitOne(JoinThread(tid))
  @inline
  def joinThread[T[+_], A](tid: T[A], timeout: Timeout) =
    waitOne(JoinThread(tid), timeout)
}
