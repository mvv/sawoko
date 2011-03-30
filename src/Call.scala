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

trait CallAsyncExecutor[-K] extends AsyncExecutor {
  def registerCall[K1 <: K, A](
        pid: Pid, key: K1, body: => A,
        callback: Callback[CallAsyncExecutor[K1], A]): Unit
}

final class CallOp[K, A](key: K, body: => A)
            extends AsyncOp[CallAsyncExecutor[K], A] {
  def register(ep: EP, callback: Callback): Option[Result] = {
    ep.executor.registerCall(ep.pid, key, body, callback)
    None
  }
}

trait CallOps {
  import AsyncOps._

  @inline
  def call[K, A](key: K)(body: => A) =
    exec(new CallOp(key, body))
  @inline
  def call[A](body: => A) =
    exec(new CallOp((), body))
}

object CallOps extends CallOps
