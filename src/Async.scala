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

import java.util.concurrent.Future

class ImplContractViolationException
      extends RuntimeException("Implementation violated the contract")

sealed trait AsyncResult[-X <: AsyncExecutor, +A] {
  def isSuccess = false
  def isFailure = false
  def isAbortion = false
}
final case class Success[A](result: A) extends AsyncResult[AsyncExecutor, A] {
  override def isSuccess = true
}
final case class Failure[-X <: AsyncExecutor, C](
                   fail: AsyncFail[X, C], cause: C)
                 extends AsyncResult[X, Nothing] {
  override def isFailure = true
}
object Failure {
  type Some = Failure[Nothing, _]
}
final case class Abortion[-X <: AsyncExecutor, C](
                   abort: AsyncAbort[X, C], cause: C)
                 extends AsyncResult[X, Nothing] {
  override def isAbortion = true
}
object Abortion {
  type Some = Abortion[Nothing, _]
}

trait AsyncExecutor {
  final type Result[-X <: AsyncExecutor, +A] = AsyncResult[X, A]
  final type Callback[+X <: AsyncExecutor, -A] = Result[X, A] => Unit
  final type SimpleResult[A] = Result[AsyncExecutor, A]
  final type SimpleCallback[A] = Callback[AsyncExecutor, A]
  type Pid

  protected def finished(pid: Pid, result: Result[this.type, Any]): Unit
  private[sawoko] def _finished(pid: Pid, result: Result[this.type, Any]) =
    finished(pid, result)

  protected final def runWithPid(pid: Pid, async: Async[this.type, _]) =
    runWithGuards(pid, async, Nil)

  private[sawoko] def runWithGuards(pid: Pid, async: Async[this.type, _],
                                    guards: List[Async.Guard[this.type]]) {
    var _async: Async[this.type, _] = async
    var _guards: List[Async.Guard[this.type]] = guards
    val _executor: this.type = this
    val _pid = pid
    val ep = new ExecutorAndPid[this.type] {
      val executor = _executor
      val pid = _pid
    }
    while(true) {
      _async.runWithGuards[this.type](ep, _guards) match {
        case Some((nextAsync, nextGuards)) =>
          _async = nextAsync
          _guards = nextGuards
        case None =>
          return
      }
    }
  }
}

trait AsyncExecutorService extends AsyncExecutor {
  type InPid
  protected def newProcess(in: InPid): Pid

  def submit[X >: this.type <: AsyncExecutor, A](
        in: InPid, async: Async[X, A]): Future[Result[X, A]]
  final def submit[X >: this.type <: AsyncExecutor, A](
              async: => Async[X, A])(
              implicit w: InPid =::= Unit): Future[Result[X, A]] =
    submit(w.unapply(()), AsyncOps.guard(async))
  def execute[X >: this.type <: AsyncExecutor](
        in: InPid, async: Async[X, Any]): Unit =
    submit(in, async)
  final def execute[X >: this.type <: AsyncExecutor](
              async: => Async[X, Any])(
              implicit w: InPid =::= Unit): Unit =
    execute(w.unapply(()), AsyncOps.guard(async))
  def invoke[X >: this.type <: AsyncExecutor, A](
        in: InPid, async: Async[X, A]): Result[X, A] =
    submit(in, async).get
  final def invoke[X >: this.type <: AsyncExecutor, A](
              async: => Async[X, A])(
              implicit w: InPid =::= Unit): Result[X, A] =
    invoke(w.unapply(()), AsyncOps.guard(async))

  def start: this.type
  def initiateShutdown: Future[Unit]
  def shutdown: Unit = initiateShutdown.get
}

sealed trait ExecutorAndPid[+X <: AsyncExecutor] {
  val executor: X
  val pid: executor.Pid
}

trait AsyncFail[-X <: AsyncExecutor, A] {
  final def apply(cause: A) = Failure(this, cause)
  final def unapply(x: AnyRef): Option[A] = x match {
    case Failure(fail, cause) if fail eq this =>
      Some(cause.asInstanceOf[A])
    case _ =>
      None
  }
}
trait AsyncAbort[-X <: AsyncExecutor, A] {
  final def apply(cause: A) = Abortion(this, cause)
  final def unapply(x: AnyRef): Option[A] = x match {
    case Abortion(abort, cause) if abort eq this =>
      Some(cause.asInstanceOf[A])
    case _ =>
      None
  }
}
trait AsyncOp[-X <: AsyncExecutor, A] {
  final type Result = AsyncResult[X, A]
  final type Callback = Result => Unit
  final type EP = ExecutorAndPid[X]
  def register(ep: EP, callback: Callback): Option[Result]
}

object Threw extends AsyncFail[AsyncExecutor, Throwable]

sealed trait ContinueOrBreak[+A, +B]
final case class Continue[+A](acc: A) extends ContinueOrBreak[A, Nothing]
final case class Break[+A](result: A) extends ContinueOrBreak[Nothing, A]

object AsyncOps {
  def pure[A](value: A): Async[AsyncExecutor, A] =
    new Async.Pure(value)
  def guard[X <: AsyncExecutor, A](body: => Async[X, A]) =
    new Async.Guarded(body)
  @inline
  def lift[A](body: => A) = guard(pure(body))
  @inline
  def abort[X <: AsyncExecutor, A](
        abort: AsyncAbort[X, A], value: A): Async[X, Nothing] =
    new Async.Abort[X, A](abort, value)
  @inline
  def fail[X <: AsyncExecutor, A](
        fail: AsyncFail[X, A], value: A): Async[X, Nothing] =
    new Async.Fail[X, A](fail, value)
  @inline
  def fail(cause: Throwable): Async[AsyncExecutor, Nothing] =
    fail(Threw, cause)
  @inline
  def exec[X <: AsyncExecutor, A](op: AsyncOp[X, A]): Async[X, A] =
    new Async.Exec[X, A, A](op, pure(_))

  val RightUnit = Right(())
  val trueM = pure(true)
  val falseM = pure(false)
  val unitM = pure(())
  val NoneM = pure(None)
  @inline
  def SomeM[A](value: A) = pure(Some(value))
  @inline
  def LeftM[A](value: A) = pure(Left(value))
  @inline
  def RightM[A](value: A) = pure(Right(value))
  val RightUnitM = pure(RightUnit)
  @inline
  def continueM[A](acc: A) = pure(Continue(acc))
  val continueM = pure(Continue(()))
  @inline
  def breakM[A](result: A) = pure(Break(result))
  val breakM = pure(Break(()))
  @inline
  def ifM(test: Boolean) = if (test) continueM else breakM
  @inline
  def ifM[A, B](test: Boolean, c: => A, b: => B) =
    if (test) continueM(c) else breakM(b)
  @inline
  def ifM[A](test: Boolean, c: => A) =
    if (test) continueM(c) else breakM
  @inline
  def unlessM(test: Boolean) = if (test) breakM else continueM
  @inline
  def unlessM[A, B](test: Boolean, c: => A, b: => B) =
    if (test) breakM(b) else continueM(c)
  @inline
  def unlessM[A](test: Boolean, c: => A) =
    if (test) breakM else continueM(c)
  @inline
  def breakIfM[A](test: Boolean, b: => A) =
    if (test) breakM(b) else continueM
  @inline
  def breakUnlessM[A](test: Boolean, b: => A) =
    if (test) continueM else breakM(b)

  def forM[X <: AsyncExecutor, A, B](
        z: A)(body: A => Async[X, ContinueOrBreak[A, B]]): Async[X, B] =
    guard(body(z)).flatMap {
      case Continue(acc) => forM(acc)(body)
      case Break(result) => pure(result)
    }
  @inline
  def repeatM[X <: AsyncExecutor, A](
        body: => Async[X, ContinueOrBreak[Unit, A]]) =
    forM(())(_ => body)

  def foreachM[X <: AsyncExecutor, A](
        xs: Iterator[A])(
        body: A => Async[X, _]): Async[X, Unit] = guard {
    if (xs.isEmpty) pure(())
    else body(xs.next).flatMap(_ => foreachM(xs)(body))
  }
  @inline
  def foreachM[X <: AsyncExecutor, A](
        xs: TraversableOnce[A])(
        body: A => Async[X, _]): Async[X, Unit] =
    foreachM(xs.toIterator)(body)

  def foldM[X <: AsyncExecutor, A, B](
        xs: Iterator[A], z: B)(
        body: (B, A) => Async[X, B]): Async[X, B] = guard {
    if (xs.isEmpty) pure(z)
    else body(z, xs.next).flatMap(acc => foldM(xs, acc)(body))
  }
  @inline
  def foldM[X <: AsyncExecutor, A, B](
        xs: TraversableOnce[A], z: B)(
        body: (B, A) => Async[X, B]): Async[X, B] =
    foldM(xs.toIterator, z)(body)
  def foldWhileM[X <: AsyncExecutor, A, B, C](
        xs: Iterator[A], z: C)(
        body: (C, A) => Async[X, ContinueOrBreak[C, B]]):
          Async[X, Either[B, C]] = guard {
    if (xs.isEmpty) pure(Right(z))
    else body(z, xs.next).flatMap {
      case Continue(acc) => foldWhileM(xs, acc)(body)
      case Break(result) => pure(Left(result))
    }
  }
  @inline
  def foldWhileM[X <: AsyncExecutor, A, B, C](
        xs: TraversableOnce[A], z: C)(
        body: (C, A) => Async[X, ContinueOrBreak[C, B]]):
          Async[X, Either[B, C]] =
    foldWhileM(xs.toIterator, z)(body)
  def foldSomeM[X <: AsyncExecutor, A, B](
        xs: Iterator[A], z: B)(
        body: (B, A) => Async[X, ContinueOrBreak[B, B]]): Async[X, B] = guard {
    if (xs.isEmpty) pure(z)
    else body(z, xs.next).flatMap {
      case Continue(acc) => foldSomeM(xs, acc)(body)
      case Break(result) => pure(result)
    }
  }
  @inline
  def foldSomeM[X <: AsyncExecutor, A, B](
        xs: TraversableOnce[A], z: B)(
        body: (B, A) => Async[X, ContinueOrBreak[B, B]]): Async[X, B] =
    foldSomeM(xs.toIterator, z)(body)
}

sealed trait Async[-X <: AsyncExecutor, +A] {
  def flatMap[X1 <: AsyncExecutor, B](
        async: A => Async[X1, B]): Async[X with X1, B]
  def map[B](f: A => B): Async[X, B]
  @inline
  final def >>[X1 <: AsyncExecutor, B](async: => Async[X1, B]) =
    flatMap(_ => async)

  protected[sawoko] def runWithGuards[X1 <: X](
                          ep: ExecutorAndPid[X1],
                          guards: List[Async.Guard[X1]]):
                            Option[(Async[X1, _], List[Async.Guard[X1]])]

  final def handle[X1 <: AsyncExecutor, C >: A, B <: C](
              handler: PartialFunction[Failure.Some, Async[X1, B]]):
                Async[X with X1, C] =
    new Async.Catch[X with X1, C, C](
          this, handler, (x: C) => AsyncOps.pure(x))
  final def threw[X1 <: AsyncExecutor, C >: A, B <: C](
              handler: PartialFunction[Throwable, Async[X1, B]]):
                Async[X with X1, C] = handle[X1, C, B] {
    case Threw(e) if handler.isDefinedAt(e) => handler(e)
  }
  final def cleanup[X1 <: AsyncExecutor](
              body: => Async[X1, Any]): Async[X with X1, A] =
    new Async.Finally(this, AsyncOps.guard(body), (x: A) => AsyncOps.pure(x))
}
object Async {
  import AsyncOps._

  @inline
  private def liftErrors[X <: AsyncExecutor, A](
                body: => Async[X, A]): Async[X, A] =
    try { body } catch { case e: Throwable => fail(e) }

  sealed trait Guard[-X <: AsyncExecutor]
  private final class Finalizer[X <: AsyncExecutor, A, B](
                        val release: () => Async[X, Any],
                        val callback: A => Async[X, B])
                      extends Guard[X]
  private final class Catcher[X <: AsyncExecutor, A, B](
                        val handler: PartialFunction[Failure.Some, Async[X, A]],
                        val callback: A => Async[X, B])
                      extends Guard[X]

  final class Pure[A](value: A) extends Async[AsyncExecutor, A] {
    def flatMap[X <: AsyncExecutor, B](async: A => Async[X, B]) =
      new Guarded(async(value))
    def map[B](f: A => B) = new Guarded(pure(f(value)))
    protected[sawoko] def runWithGuards[X <: AsyncExecutor](
                            ep: ExecutorAndPid[X],
                            guards: List[Guard[X]]) = guards match {
      case (x: Finalizer[_, _, _]) :: guards  =>
        val f = x.asInstanceOf[Finalizer[X, A, _]]
        val next = liftErrors { f.release() }
        Some((next.flatMap(_ => f.callback(value)), guards))
      case (x: Catcher[_, _, _]) :: guards =>
        val c = x.asInstanceOf[Catcher[X, A, _]]
        Some((pure(value).flatMap(c.callback(_)), guards))
      case Nil =>
        ep.executor._finished(ep.pid, Success(value))
        None
    }
  }
  final class Guarded[X <: AsyncExecutor, A](
                body: => Async[X, A])
              extends Async[X, A] {
    def flatMap[E1 <: AsyncExecutor, B](async: A => Async[E1, B]) =
      new Guarded(body.flatMap(async))
    def map[B](f: A => B) = new Guarded(body.map(f))
    protected[sawoko] def runWithGuards[X1 <: X](
                            ep: ExecutorAndPid[X1], guards: List[Guard[X1]]) =
      Some((liftErrors(body), guards))
  }
  final class Abort[X <: AsyncExecutor, A](
                abort: AsyncAbort[X, A], cause: A)
              extends Async[X, Nothing] {
    def flatMap[X1 <: AsyncExecutor, B](async: Nothing => Async[X1, B]) = this
    def map[B](f: Nothing => B) = this
    protected[sawoko] def runWithGuards[X1 <: X](
                            ep: ExecutorAndPid[X1],
                            guards: List[Guard[X1]]) = guards match {
      case (guard: Finalizer[_, _, _]) :: guards =>
        val f = guard.asInstanceOf[Finalizer[X, _, _]]
        val next = liftErrors { f.release() }
        Some((next.flatMap(_ => this), guards))
      case _ :: guards =>
        Some((this, guards))
      case Nil =>
        ep.executor._finished(ep.pid, Abortion(abort, cause))
        None
    }
  }
  final class Fail[X <: AsyncExecutor, A](
                fail: AsyncFail[X, A], cause: A)
              extends Async[X, Nothing] {
    def flatMap[X1 <: AsyncExecutor, B](async: Nothing => Async[X1, B]) = this
    def map[B](f: Nothing => B) = this
    protected[sawoko] def runWithGuards[X1 <: X](
                            ep: ExecutorAndPid[X1],
                            guards: List[Guard[X1]]) = guards match {
      case (guard: Finalizer[_, _, _]) :: guards =>
        val f = guard.asInstanceOf[Finalizer[X, _, _]]
        val next = liftErrors { f.release() }
        Some((next.flatMap(_ => this), guards))
      case (guard: Catcher[_, _, _]) :: guards =>
        val c = guard.asInstanceOf[Catcher[X, A, _]]
        val failure = Failure[X, A](fail, cause)
        val next = liftErrors {
          pure(c.handler.isDefinedAt(failure))
        } .flatMap { defined =>
          if (defined) c.handler(failure)
          else this
        }
        Some((next, guards))
      case Nil =>
        ep.executor._finished(ep.pid, Failure(fail, cause))
        None
    }
  }
  final class Finally[X <: AsyncExecutor, A, B, C](
                body: Async[X, A],
                release: Async[X, B],
                callback: A => Async[X, C])
              extends Async[X, C] {
    def flatMap[X1 <: AsyncExecutor, D](
          async: C => Async[X1, D]): Async[X with X1, D] =
      new Finally(body, release,
                  (x: A) => liftErrors { callback(x).flatMap(async) })
    def map[D](f: C => D): Async[X, D] =
      new Finally(body, release, (x: A) => liftErrors { callback(x).map(f) })
    protected[sawoko] def runWithGuards[X1 <: X](
                            ep: ExecutorAndPid[X1],
                            guards: List[Guard[X1]]) =
      Some((body, new Finalizer(() => release, callback) :: guards))
  }
  final class Catch[X <: AsyncExecutor, A, B](
                body: Async[X, A],
                handle: PartialFunction[Failure.Some, Async[X, A]],
                callback: A => Async[X, B])
              extends Async[X, B] {
    def flatMap[X1 <: AsyncExecutor, C](
          async: B => Async[X1, C]): Async[X with X1, C] =
      new Catch(body, handle,
                (x: A) => liftErrors { callback(x).flatMap(async) })
    def map[C](f: B => C): Async[X, C] =
      new Catch(body, handle, (x: A) => liftErrors { callback(x).map(f) })
    protected[sawoko] def runWithGuards[X1 <: X](
                            ep: ExecutorAndPid[X1], guards: List[Guard[X1]]) =
      Some((body, new Catcher(handle, callback) :: guards))
  }
  final class Exec[X <: AsyncExecutor, A, B](
                op: AsyncOp[X, A],
                callback: A => Async[X, B])
              extends Async[X, B] {
    def flatMap[X1 <: AsyncExecutor, C](
          async: B => Async[X1, C]): Async[X with X1, C] =
      new Exec(op, (x: A) => liftErrors { callback(x).flatMap(async) })
    def map[C](f: B => C): Async[X, C] =
      new Exec(op, (x: A) => liftErrors { callback(x).map(f) })
    protected[sawoko] def runWithGuards[X1 <: X](
                            ep: ExecutorAndPid[X1],
                            guards: List[Guard[X1]]) = {
      def cont(result: op.Result) = result match {
        case Success(value) =>
          liftErrors { callback(value) }
        case Failure(fail, cause) =>
          new Fail(fail, cause)
        case Abortion(abort, cause) =>
          new Abort(abort, cause)
      }
      try {
        op.register(ep, result =>
            ep.executor.runWithGuards(ep.pid, cont(result), guards)) .map {
          result => (cont(result), guards)
        }
      } catch {
        case e: Throwable => Some((fail(e), guards))
      }
    }
  }
}
