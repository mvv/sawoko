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

import java.util.Date
import com.github.mvv.sawoko._

trait SleepMultiAsyncExecutor extends TimeoutMultiAsyncExecutor
                                 with SleepAsyncExecutor {
  final def registerSleep(pid: Pid, timeout: Timeout,
                          callback: Long => Unit) = {
    if (timeout.timeout == 0)
      scheduleProcess(pid, Success(0L)) {
        case Success(result) => callback(result)
        case _ =>
      }
    else {
      suspendProcess(pid, (_: SimpleResult[Long]) match {
          case Success(result) => callback(result)
          case _ =>
        })
      val ts = System.nanoTime
      registerTimeoutHandler(ts + timeout.toNanos) {
        val passed = (System.nanoTime - ts).ns.to(timeout.unit).timeout
        resumeProcess(pid, Success(passed))
      }
    }
    None
  }
  final def registerSleepUntil(pid: Pid, date: Date,
                               callback: Boolean => Unit) = {
    val expiresAt = date.getTime.ms.toNanos
    val ts = System.nanoTime
    val time = expiresAt - ts
    if (time <= 0)
      scheduleProcess(pid, Success(false))(_ => callback(false))
    else {
      suspendProcess(pid, (_: SimpleResult[Boolean]) => callback(true))
      registerTimeoutHandler(expiresAt) {
        resumeProcess(pid, Success(true))
      }
    }
    None
  }
}
