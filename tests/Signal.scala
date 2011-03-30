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

package com.github.mvv.sawoko.tests

import org.specs2._
import com.github.mvv.sawoko._
import com.github.mvv.sawoko.impl._

object SignalSpec extends mutable.Specification {
  import MultiSignalOps._

  var e: DefaultMultiAsyncExecutor = null

  step {
    e = new DefaultMultiAsyncExecutor
    e.start
  }

  "Creating a signal" in {
    e.invoke(createSignal[Int]).isSuccess must_== true
  }

  "Emitting and then waiting for a signal" in {
    e.invoke {
      for {
        s <- createSignal[Int]
        _ <- emitSignal(s, 123)
        v <- awaitSignal(s)
      } yield v
    } must_== Success(123)
  }

  step {
    e.shutdown
  }
}
