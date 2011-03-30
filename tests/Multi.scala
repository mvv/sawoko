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

object MultiSpec extends mutable.Specification {
  import AsyncOps._

  var e: DefaultMultiAsyncExecutor = null

  step {
    e = new DefaultMultiAsyncExecutor
    e.start
  }

  "Running pure returns the original value" in {
    e.invoke(pure(1)) must_== Success(1)
  }

  "Throwing exception causes failure" in {
    class E extends RuntimeException
    val result = e.invoke(pure(throw new E) >> pure(1)) match {
      case Threw(e: E) => true
      case _ => false
    }
    result must_== true
  }

  "Calling 'fail' causes failure" in {
    class E extends RuntimeException
    val result = e.invoke(fail(new E) >> pure(1)) match {
      case Threw(e: E) => true
      case _ => false
    }
    result must_== true
  }

  "Yielding returns control eventually" in {
    import YieldOps._
    e.invoke(yieldM >> pure(1)) must_== Success(1)
  }

  "Sleeping returns control eventually" in {
    import SleepOps._
    e.invoke(sleep(1.ms) >> pure(2)) must_== Success(2)
  }

  step {
    e.shutdown
  }
}