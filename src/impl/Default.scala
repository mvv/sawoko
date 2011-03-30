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

object DefaultMultiAsyncExecutor {
  type Caps = JoinThreadWaitCap[Tid] with
              AwaitSignalWaitCap[Signal] with
              ReadChanWaitCap[Chan] with
              WriteChanSyncWaitCap[Chan]
}
class DefaultMultiAsyncExecutor
         extends WaitManyTimeoutMultiAsyncExecutor[DefaultMultiAsyncExecutor.Caps]
            with ForkMultiAsyncExecutor
            with YieldMultiAsyncExecutor
            with SleepMultiAsyncExecutor
            with SignalMultiAsyncExecutor
            with ChanMultiAsyncExecutor {
  type InPid = Unit

  class Pid protected[DefaultMultiAsyncExecutor] () extends ForkPid

  protected def newProcess(in: Unit) = new Pid
  protected def newProcess(parent: Pid) = new Pid
}

object DefaultNioMultiAsyncExecutor {
  type Caps = DefaultMultiAsyncExecutor.Caps with NioPollWaitCap
}
class DefaultNioMultiAsyncExecutor
      extends DefaultMultiAsyncExecutor
         with WaitManyTimeoutMultiAsyncExecutor[DefaultNioMultiAsyncExecutor.Caps]
         with NioMultiAsyncExecutor
