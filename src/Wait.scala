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

import scala.collection.mutable.ArraySeq

case class WaitOpException(index: Int, cause: Throwable)
           extends RuntimeException(cause) {
  override def getMessage = "Wait operation #" + index + " failed with " + cause
}

trait WaitCap

object WaitOpList {
  type MutableItems[R <: WaitOpResultList, Cap <: WaitCap] =
    ArraySeq[WaitOpItem[Cap, W, R] forSome { type W <: WaitOp }]

  /*
   * Moving consing here improves the inferred type, i.e
   * ReadChan[C, A] ?: ReadChan[C, B] vs
   * ReadChan[C, A] ?: _1.type forSome { val _1: ReadChan[C, B] }
   */
  final class WaitOpCons[L <: WaitOpList](ops: L) {
    @inline
    def ?:[W <: WaitOp](op: W) = new ?:[W, L](op, ops)
  }
  implicit def waitOpCons[L <: WaitOpList](ops: L) = new WaitOpCons(ops)
}
sealed trait WaitOpList {
  import WaitOpList._

  type Cap <: WaitCap
  type Results <: WaitOpResultList

  val size: Int

  final type Items =
    IndexedSeq[WaitOpItem[Cap, W, Results] forSome { type W <: WaitOp }]
  final def items: Items = {
    val result: MutableItems[Results, Cap] = new ArraySeq(size)
    fillItemsFrom(result, R1)
    result
  }
  protected[sawoko]
  def fillItemsFrom[R <: WaitOpResultList, C <: Cap,
                    I <: WaitOpResultList.Index](
        items: MutableItems[R, C], i: I): Unit
}
trait WaitOp extends WaitOpList { self =>
  import WaitOpList._

  type Result

  final type Results = WaitOpResultSingle[Result]

  final val size = 1
  @inline
  protected[sawoko]
  final def fillItemsFrom[R <: WaitOpResultList, C <: Cap,
                          I <: WaitOpResultList.Index](
              items: MutableItems[R, C], i: I) {
    items(i.value) = new WaitOpItem[C, self.type, R] {
        val hasCap = implicitly[C <::< Cap]
        val op: self.type = self
        val index = i.value
        def toResult(result: op.Result) =
          WaitOpResult[R, I](i, result.asInstanceOf[R#Apply[I]])
      }
  }
}
final case class ?:[W <: WaitOp, T <: WaitOpList](head: W, tail: T)
                 extends WaitOpList { self =>
  import WaitOpList._

  type Cap = W#Cap with T#Cap
  type Results = WaitOpResultCons[W#Result, T#Results]

  val size = head.size + tail.size
  protected[sawoko]
  def fillItemsFrom[R <: WaitOpResultList, C <: Cap,
                    I <: WaitOpResultList.Index](
        items: MutableItems[R, C], i: I) {
    head.asInstanceOf[W { type Cap = W#Cap }].
      fillItemsFrom[R, C, I](items, i)
    tail.asInstanceOf[T { type Cap = T#Cap }].
      fillItemsFrom[R, C, I#Succ](items, i.next)
  }
}

object WaitOpResultList {
  sealed trait YesOrNo
  sealed trait Yes extends YesOrNo
  sealed trait No extends YesOrNo

  sealed trait Index {
    type This >: this.type <: Index
    final type Succ = Next[This]
    type IfFirst[U, T <: U, F <: U] <: U
    type Recur[U, Z <: U, N[I <: Index] <: U] <: U
    val value: Int
    final def next: Next[This] = Next[This](this)
    def unapply[L <: WaitOpResultList](
          result: WaitOpsResult[L])(
          implicit w: L#HasIndex[This] <::< Yes): Option[L#Apply[This]] =
      result match {
        case WaitOpResult(index, result) if index.value == value =>
          Some(result.asInstanceOf[L#Apply[This]])
        case _ =>
          None
      }
  }
  object First extends Index {
    type This = First.type
    type IfFirst[U, T <: U, F <: U] = T
    type Recur[U, Z <: U, N[I <: Index] <: U] = Z
    val value = 0
  }
  final case class Next[Pred <: Index](pred: Pred) extends Index {
    type This = Next[Pred]
    type IfFirst[U, T <: U, F <: U] = F
    type Recur[U, Z <: U, N[I <: Index] <: U] = N[Pred]
    val value = pred.value + 1
  }
}
sealed trait WaitOpResultList {
  import WaitOpResultList._
  type Head
  type HasIndex0[I <: Index] <: YesOrNo
  final type HasIndex[I <: Index] = I#IfFirst[YesOrNo, Yes, HasIndex0[I]]
  type Apply[I <: Index]
}
final class WaitOpResultSingle[A] extends WaitOpResultList {
  import WaitOpResultList._
  type Head = A
  type HasIndex0[I <: Index] = I#IfFirst[YesOrNo, Yes, No]
  type Apply[I <: Index] = I#IfFirst[Any, A, Nothing]
}
final class WaitOpResultCons[A, T <: WaitOpResultList]
            extends WaitOpResultList {
  import WaitOpResultList._
  type Head = A
  type Tail = T
  type HasIndex0[I <: Index] = I#Recur[YesOrNo, Yes, Tail#HasIndex0]
  type Apply[I <: Index] = I#Recur[Any, A, Tail#Apply]
}

sealed trait WaitOpsResult[+L <: WaitOpResultList]
final case class WaitOpResult[L <: WaitOpResultList,
                              I <: WaitOpResultList.Index](
                   index: I, value: L#Apply[I])
                 extends WaitOpsResult[L]

sealed trait WaitOpItem[P <: WaitCap, W <: WaitOp, L <: WaitOpResultList] {
  final type Op = op.type
  final type Result = op.Result
  val hasCap: P <::< W#Cap
  val op: W
  val index: Int
  def toResult(result: Result): WaitOpsResult[L]
}

trait WaitAsyncExecutor[+P <: WaitCap] extends AsyncExecutor {
  def registerWait[W <: WaitOp](
        pid: Pid, op: W,
        callback: SimpleCallback[W#Result])(
        implicit w: P <::< W#Cap): Option[SimpleResult[W#Result]]
}

final case class WaitAsyncOp[W <: WaitOp](op: W)
                 extends AsyncOp[WaitAsyncExecutor[W#Cap], W#Result] {
  def register(ep: EP, callback: Callback) =
    ep.executor.registerWait(ep.pid, op, callback)
}

trait WaitTimeoutAsyncExecutor[+P <: WaitCap] extends WaitAsyncExecutor[P] {
  def registerWaitTimeout[W <: WaitOp](
        pid: Pid, op: W, timeout: Timeout,
        callback: SimpleCallback[Option[W#Result]])(
        implicit w: P <::< W#Cap): Option[SimpleResult[Option[W#Result]]]
}

trait DefaultWaitForWaitTimeoutAsyncExecutor[+P <: WaitCap]
      extends WaitAsyncExecutor[P] {
    self: WaitTimeoutAsyncExecutor[P] =>
  def registerWait[W <: WaitOp](
        pid: Pid, op: W,
        callback: SimpleCallback[W#Result])(
        implicit w: P <::< W#Cap) =
    registerWaitTimeout[W](
        pid, op, Timeout.Inf,
        _ match {
          case Success(Some(result)) =>
            callback(Success(result))
          case failure @ Failure(_, _) =>
            callback(failure)
          case _ =>
            callback(Threw(new ImplContractViolationException))
        }) .map {
      _ match {
        case Success(Some(result)) =>
          Success(result)
        case failure @ Failure(_, _) =>
          failure
        case _ =>
          Threw(new ImplContractViolationException)
      }
    }
}

final case class WaitTimeoutAsyncOp[W <: WaitOp](op: W, timeout: Timeout)
                 extends AsyncOp[WaitTimeoutAsyncExecutor[W#Cap],
                                 Option[W#Result]] {
  def register(ep: EP, callback: Callback) =
    ep.executor.registerWaitTimeout(ep.pid, op, timeout, callback)
}

trait WaitManyAsyncExecutor[+P <: WaitCap] extends WaitAsyncExecutor[P] {
  def registerWaitMany[L <: WaitOpList](
        pid: Pid, ops: L,
        callback: SimpleCallback[WaitOpsResult[L#Results]])(
        implicit w: P <::< L#Cap):
          Option[SimpleResult[WaitOpsResult[L#Results]]]
}

trait DefaultWaitForWaitManyAsyncExecutor[+P <: WaitCap]
      extends WaitAsyncExecutor[P] {
    self: WaitManyAsyncExecutor[P] =>
  def registerWait[W <: WaitOp](
        pid: Pid, op: W,
        callback: SimpleCallback[W#Result])(
        implicit w: P <::< W#Cap) =
    registerWaitMany[W](
        pid, op,
        _ match {
          case Success(R1(result)) =>
            callback(Success(result))
          case Threw(WaitOpException(0, cause)) =>
            callback(Threw(cause))
          case Threw(WaitOpException(_, _)) =>
            callback(Threw(new ImplContractViolationException))
          case failure @ Failure(_, _) =>
            callback(failure)
          case _ =>
            callback(Threw(new ImplContractViolationException))
        }) .map {
      _ match {
        case Success(R1(result)) =>
          Success(result)
        case Threw(WaitOpException(0, cause)) =>
          Threw(cause)
        case Threw(WaitOpException(_, cause)) =>
          Threw(new ImplContractViolationException)
        case failure @ Failure(_, _) =>
          failure
        case _ =>
          Threw(new ImplContractViolationException)
      }
    }
}

final case class WaitManyAsyncOp[L <: WaitOpList](ops: L)
                 extends AsyncOp[WaitManyAsyncExecutor[L#Cap],
                                 WaitOpsResult[L#Results]] {
  def register(ep: EP, callback: Callback) =
    ep.executor.registerWaitMany(ep.pid, ops, callback)
}

trait WaitManyTimeoutAsyncExecutor[+P <: WaitCap]
      extends WaitManyAsyncExecutor[P]
         with WaitTimeoutAsyncExecutor[P] {
  def registerWaitManyTimeout[L <: WaitOpList](
        pid: Pid, ops: L, timeout: Timeout,
        callback: SimpleCallback[Option[WaitOpsResult[L#Results]]])(
        implicit w: P <::< L#Cap):
          Option[SimpleResult[Option[WaitOpsResult[L#Results]]]]
  def registerWaitMany[L <: WaitOpList](
        pid: Pid, ops: L,
        callback: SimpleCallback[WaitOpsResult[L#Results]])(
        implicit w: P <::< L#Cap) =
    registerWaitManyTimeout[L](
        pid, ops, Timeout.Inf,
        _ match {
          case Success(Some(result)) =>
            callback(Success(result))
          case failure @ Failure(_, _) =>
            callback(failure)
          case _ =>
            callback(Threw(new ImplContractViolationException))
        }) .map {
      _ match {
        case Success(Some(result)) =>
          Success(result)
        case failure @ Failure(_, _) =>
          failure
        case _ =>
          Threw(new ImplContractViolationException)
      }
    }
  def registerWaitTimeout[W <: WaitOp](
        pid: Pid, op: W, timeout: Timeout,
        callback: SimpleCallback[Option[W#Result]])(
        implicit w: P <::< W#Cap) =
    registerWaitManyTimeout[W](
        pid, op, timeout,
        _ match {
          case Success(Some(R1(result))) =>
            callback(Success(Some(result)))
          case Success(None) =>
            callback(Success(None))
          case Threw(WaitOpException(0, cause)) =>
            callback(Threw(cause))
          case Threw(WaitOpException(_, _)) =>
            callback(Threw(new ImplContractViolationException))
          case failure @ Failure(_, _) =>
            callback(failure)
          case _ =>
            callback(Threw(new ImplContractViolationException))
        }) .map {
      case Success(Some(R1(result))) =>
        Success(Some(result))
      case Success(None) =>
        Success(None)
      case Threw(WaitOpException(0, cause)) =>
        Threw(cause)
      case Threw(WaitOpException(_, _)) =>
        Threw(new ImplContractViolationException)
      case failure @ Failure(_, _) =>
        failure
      case _ =>
        Threw(new ImplContractViolationException)
    }
  def registerWait[W <: WaitOp](
        pid: Pid, op: W,
        callback: SimpleCallback[W#Result])(
        implicit w: P <::< W#Cap) =
    registerWaitManyTimeout[W](
        pid, op, Timeout.Inf,
        _ match {
          case Success(Some(R1(result))) =>
            callback(Success(result))
          case Threw(WaitOpException(0, cause)) =>
            callback(Threw(cause))
          case Threw(WaitOpException(_, _)) =>
            callback(Threw(new ImplContractViolationException))
          case failure @ Failure(_, _) =>
            callback(failure)
          case _ =>
            callback(Threw(new ImplContractViolationException))
        }) .map {
      _ match {
        case Success(Some(R1(result))) =>
          Success(result)
        case Threw(WaitOpException(0, cause)) =>
          Threw(cause)
        case Threw(WaitOpException(_, _)) =>
          Threw(new ImplContractViolationException)
        case failure @ Failure(_, _) =>
          failure
        case _ =>
          Threw(new ImplContractViolationException)
      }
    }
}

final case class WaitManyTimeoutAsyncOp[L <: WaitOpList](
                   ops: L, timeout: Timeout)
                 extends AsyncOp[WaitManyTimeoutAsyncExecutor[L#Cap],
                                 Option[WaitOpsResult[L#Results]]] {
  def register(ep: EP, callback: Callback) =
    ep.executor.registerWaitManyTimeout(ep.pid, ops, timeout, callback)
}

trait WaitOps {
  import AsyncOps._

  @inline
  def waitOne[W <: WaitOp](op: W) =
    exec(WaitAsyncOp(op))
  @inline
  def waitOne[W <: WaitOp](op: W, timeout: Timeout) =
    exec(WaitTimeoutAsyncOp(op, timeout))
  @inline
  def waitMany[L <: WaitOpList](ops: L) =
    exec(WaitManyAsyncOp(ops))
  @inline
  def waitMany[L <: WaitOpList](ops: L, timeout: Timeout) =
    exec(WaitManyTimeoutAsyncOp(ops, timeout))
}

object WaitOps extends WaitOps
