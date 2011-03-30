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

import java.nio.ByteBuffer
import java.nio.channels.{
         SelectableChannel, SelectionKey, SocketChannel, ServerSocketChannel,
         ReadableByteChannel, WritableByteChannel}
import java.net.SocketAddress

trait NioPollWaitCap extends WaitCap

final case class NioPoll(channel: SelectableChannel, ops: Int) extends WaitOp {
  require(ops != 0 && (ops & channel.validOps) == ops)
  type Cap = NioPollWaitCap
  type Result = Int
}

trait NioAsyncExecutor extends AsyncExecutor {
  def registerNioClose(pid: Pid, channel: SelectableChannel,
                       callback: Callback[NioAsyncExecutor, Unit]): Boolean
}

final case class NioCloseOp(channel: SelectableChannel)
                 extends AsyncOp[NioAsyncExecutor, Unit] {
  def register(ep: EP, callback: Callback) = {
    val suspended = ep.executor.registerNioClose(ep.pid, channel, callback)
    if (suspended) None
    else Some(Success(()))
  }
}

trait NioOps {
  import AsyncOps._
  import WaitOps._
  import YieldOps._

  @inline
  def poll(channel: SelectableChannel, ops: Int) =
    waitOne(NioPoll(channel, ops))
  @inline
  def poll(channel: SelectableChannel, ops: Int, timeout: Timeout) =
    waitOne(NioPoll(channel, ops), timeout)
  @inline
  def close(channel: SelectableChannel) =
    exec(NioCloseOp(channel))

  def connect(channel: SocketChannel, address: SocketAddress) = yieldM >> {
    if (channel.connect(address))
      unitM
    else repeatM {
      poll(channel, SelectionKey.OP_CONNECT) >>
      unlessM(channel.finishConnect)
    }
  }
  def connect(channel: SocketChannel, address: SocketAddress,
              timeout: Timeout) = yieldM >> {
    if (channel.connect(address))
      trueM
    else forM(timeout.base) { case base =>
      poll(channel, SelectionKey.OP_CONNECT, base.timeout).flatMap {
        case Some(_) =>
          unlessM(channel.finishConnect, base.adjust, true)
        case None =>
          breakM(false)
      }
    }
  }
  def connect[W <: WaitOp](
        channel: SocketChannel, address: SocketAddress, op: W) =
    waitOne(op, Timeout.Now).flatMap {
      case Some(result) =>
        LeftM(result)
      case None =>
        if (channel.connect(address))
          RightUnitM
        else repeatM {
          waitMany(op ?: NioPoll(channel, SelectionKey.OP_CONNECT)).flatMap {
            case R1(result) =>
              breakM(Left(result))
            case _ =>
              breakIfM(channel.finishConnect, RightUnit)
          }
        }
    }
  def connect[W <: WaitOp](
        channel: SocketChannel, address: SocketAddress, op: W,
        timeout: Timeout) =
    waitOne(op, Timeout.Now).flatMap {
      case Some(result) =>
        LeftM(result)
      case None =>
        if (channel.connect(address))
          RightM(true)
        else forM(timeout.base) { base =>
          waitMany(op ?: NioPoll(channel, SelectionKey.OP_CONNECT),
                   base.timeout).flatMap {
            case Some(R1(result)) =>
              breakM(Left(result))
            case Some(_) =>
              unlessM(channel.finishConnect, base.adjust, Right(true))
            case None =>
              breakM(Right(false))
          }
        }
    }

  def accept(channel: ServerSocketChannel) = yieldM >> repeatM {
    channel.accept match {
      case null =>
        poll(channel, SelectionKey.OP_ACCEPT) >>
        continueM
      case s =>
        breakM(s)
    }
  }
  def accept(channel: ServerSocketChannel, timeout: Timeout) = yieldM >> {
    channel.accept match {
      case null => forM(timeout.base) { base =>
        poll(channel, SelectionKey.OP_ACCEPT, base.timeout).flatMap {
          case Some(_) => channel.accept match {
            case null => continueM(base.adjust)
            case s => breakM(Some(s))
          }
          case None => breakM(None)
        }
      }
      case s =>
        SomeM(s)
    }
  }
  def accept[W <: WaitOp](channel: ServerSocketChannel, op: W) =
    waitOne(op, Timeout.Now).flatMap {
      case Some(value) => LeftM(value)
      case None => channel.accept match {
        case null => repeatM {
          waitMany(op ?: NioPoll(channel, SelectionKey.OP_ACCEPT)).flatMap {
            case R1(result) => breakM(Left(result))
            case _ => channel.accept match {
              case null => continueM
              case s => breakM(Right(s))
            }
          }
        }
        case s => RightM(s)
      }
    }
  def accept[W <: WaitOp](
        channel: ServerSocketChannel, op: W, timeout: Timeout) =
    waitOne(op, Timeout.Now).flatMap {
      case Some(value) => SomeM(Left(value))
      case None => channel.accept match {
        case null => forM(timeout.base) { base =>
          waitMany(op ?: NioPoll(channel, SelectionKey.OP_ACCEPT),
                   base.timeout).flatMap {
            case Some(R1(result)) => breakM(Some(Left(result)))
            case Some(_) => channel.accept match {
              case null => continueM(base.adjust)
              case s => breakM(Some(Right(s)))
            }
            case None => breakM(None)
          }
        }
        case s => SomeM(Right(s))
      }
    }

  def read(channel: SelectableChannel with ReadableByteChannel,
           dst: ByteBuffer) = yieldM >> forM(0) { alreadyRead =>
    val n = channel.read(dst)
    if (n < 0)
      breakM(if (alreadyRead == 0) -1 else alreadyRead)
    else if (dst.remaining == 0)
      breakM(alreadyRead + n)
    else
      poll(channel, SelectionKey.OP_READ) >>
      continueM(alreadyRead + n)
  }
  def readSome(channel: SelectableChannel with ReadableByteChannel,
               dst: ByteBuffer) = yieldM >> repeatM {
    val n = channel.read(dst)
    if (n < 0)
      breakM(-1)
    else if (n > 0 || dst.remaining == 0)
      breakM(n)
    else
      poll(channel, SelectionKey.OP_READ) >>
      continueM
  }
  def read(channel: SelectableChannel with ReadableByteChannel,
           dst: ByteBuffer, timeout: Timeout) =
    yieldM >>
    forM(0) { alreadyRead =>
      val n = channel.read(dst)
      if (n < 0)
        breakM(if (alreadyRead == 0) -1 else alreadyRead)
      else if (dst.remaining == 0)
        breakM(alreadyRead + n)
      else
        poll(channel, SelectionKey.OP_READ, timeout).flatMap {
          case Some(_) => continueM(alreadyRead + n)
          case None => breakM(0)
        }
    }
  def readSome(channel: SelectableChannel with ReadableByteChannel,
               dst: ByteBuffer, timeout: Timeout) = yieldM >> {
    val n = channel.read(dst)
    if (n < 0)
      pure(-1)
    else if (n > 0 || dst.remaining == 0)
      pure(n)
    else forM(timeout.base) { base =>
      poll(channel, SelectionKey.OP_READ, base.timeout).flatMap {
        case Some(_) =>
          val n = channel.read(dst)
          if (n < 0)
            breakM(-1)
          else if (n > 0 || dst.remaining == 0)
            breakM(n)
          else
            continueM(base.adjust)
        case None =>
          breakM(0)
      }
    }
  }
  def read[W <: WaitOp](
        channel: SelectableChannel with ReadableByteChannel,
        dst: ByteBuffer, op: W) =
    waitOne(op, Timeout.Now).flatMap {
      case Some(result) => LeftM(result)
      case None => forM(0) { alreadyRead =>
        val n = channel.read(dst)
        if (n < 0)
          breakM(Right(if (alreadyRead == 0) -1 else alreadyRead))
        else if (dst.remaining == 0)
          breakM(Right(alreadyRead + n))
        else
          waitMany(op ?: NioPoll(channel, SelectionKey.OP_READ)).flatMap {
            case R1(value) => breakM(Left(value))
            case _ => continueM(alreadyRead + n)
          }
      }
    }
  def readSome[W <: WaitOp](
        channel: SelectableChannel with ReadableByteChannel,
        dst: ByteBuffer, op: W) =
    waitOne(op, Timeout.Now).flatMap {
      case Some(result) => LeftM(result)
      case None => repeatM {
        val n = channel.read(dst)
        if (n < 0)
          breakM(Right(-1))
        else if (n > 0 || dst.remaining == 0)
          breakM(Right(n))
        else
          waitMany(op ?: NioPoll(channel, SelectionKey.OP_READ)).flatMap {
            case R1(value) => breakM(Left(value))
            case _ => continueM
          }
      }
    }
  def read[W <: WaitOp](
        channel: SelectableChannel with ReadableByteChannel,
        dst: ByteBuffer, op: W, timeout: Timeout) =
    waitOne(op, Timeout.Now).flatMap {
      case Some(result) => LeftM(result)
      case None => forM((timeout.base, 0)) { case (base, alreadyRead) =>
        val n = channel.read(dst)
        if (n < 0)
          breakM(Right(if (alreadyRead == 0) -1 else alreadyRead))
        else if (dst.remaining == 0)
          breakM(Right(alreadyRead + n))
        else
          waitMany(op ?: NioPoll(channel, SelectionKey.OP_READ),
                   base.timeout).flatMap {
            case Some(R1(value)) => breakM(Left(value))
            case Some(_) => continueM((base.adjust, alreadyRead + n))
            case None => breakM(Right(alreadyRead + n))
          }
      }
    }
  def readSome[W <: WaitOp](
        channel: SelectableChannel with ReadableByteChannel,
        dst: ByteBuffer, op: W, timeout: Timeout) =
    waitOne(op, Timeout.Now).flatMap {
      case Some(result) => LeftM(result)
      case None => forM(timeout.base) { base =>
        val n = channel.read(dst)
        if (n < 0)
          breakM(Right(-1))
        else if (n > 0 || dst.remaining == 0)
          breakM(Right(n))
        else
          waitMany(op ?: NioPoll(channel, SelectionKey.OP_READ),
                   base.timeout).flatMap {
            case Some(R1(value)) => breakM(Left(value))
            case Some(_) => continueM(base.adjust)
            case None => breakM(Right(0))
          }
      }
    }

  def write(channel: SelectableChannel with WritableByteChannel,
            src: ByteBuffer) = yieldM >> forM(0) { alreadyWrote =>
    val n = channel.write(src)
    if (src.remaining == 0)
      breakM(alreadyWrote + n)
    else
      poll(channel, SelectionKey.OP_WRITE) >>
      continueM(alreadyWrote + n)
  }
  def write(channel: SelectableChannel with WritableByteChannel,
            src: ByteBuffer, timeout: Timeout) =
    yieldM >>
    forM((timeout.base, 0)) { case (base, alreadyWrote) =>
      val n = channel.write(src)
      if (src.remaining == 0)
        breakM(alreadyWrote + n)
      else
        poll(channel, SelectionKey.OP_WRITE, base.timeout).flatMap {
          case Some(_) => continueM((base.adjust, alreadyWrote + n))
          case None => breakM(alreadyWrote + n)
        }
    }
  def write[W <: WaitOp](
        channel: SelectableChannel with WritableByteChannel,
        src: ByteBuffer, op: W) =
    waitOne(op, Timeout.Now).flatMap {
      case Some(result) => LeftM(result)
      case None => forM(0) { alreadyWrote =>
        val n = channel.write(src)
        if (src.remaining == 0)
          breakM(alreadyWrote + n)
        else
          waitMany(op ?: NioPoll(channel, SelectionKey.OP_WRITE)).flatMap {
            case R1(result) => breakM(Left(result))
            case _ => continueM(alreadyWrote + n)
          }
      }
    }
  def write[W <: WaitOp](
        channel: SelectableChannel with WritableByteChannel,
        src: ByteBuffer, op: W, timeout: Timeout) =
    waitOne(op, Timeout.Now).flatMap {
      case Some(result) => LeftM(result)
      case None => forM((timeout.base, 0)) { case (base, alreadyWrote) =>
        val n = channel.write(src)
        if (src.remaining == 0)
          breakM(alreadyWrote + n)
        else
          waitMany(op ?: NioPoll(channel, SelectionKey.OP_WRITE),
                   base.timeout).flatMap {
            case Some(R1(result)) => breakM(Left(result))
            case Some(_) => continueM(base.adjust, alreadyWrote + n)
            case None => breakM(Right(alreadyWrote + n))
          }
      }
    }
}

object NioOps extends NioOps
