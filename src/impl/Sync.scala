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

class SyncExecutor extends AsyncExecutor {
  final type Pid = Unit

  private var stateOpt: Option[Either[Throwable, Any]] = None

  def finished(pid: Unit, result: Result[this.type, Any]) {
    stateOpt = Some {
      (result: @unchecked) match {
        case Success(result) => Right(result)
        case Threw(cause) => Left(cause)
      }
    }
  }

  def isFinished = stateOpt.isDefined
  def resultOption = stateOpt
  def result = stateOpt.get

  def run[A](async: => Async[AsyncExecutor, A]) = {
    runWithPid((), AsyncOps.guard(async))
    result.asInstanceOf[Either[Throwable, A]]
  }
}
