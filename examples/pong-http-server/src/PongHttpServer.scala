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

package com.github.mvv.sawoko.examples.pong_http_server

import java.net.{InetAddress, InetSocketAddress}
import java.nio.channels.{SocketChannel, ServerSocketChannel}
import java.nio.ByteBuffer
import org.clapper.argot._
import com.github.mvv.sawoko._
import com.github.mvv.sawoko.impl._

object PongHttpServer {
  import AsyncOps._
  import MultiForkOps._
  import MultiSignalOps._
  import NioOps._

  val resp = {
    "HTTP/1.1 200 OK\r\n" +
    "Content-Type: text/plain\r\n" +
    "Content-Length: 4\r\n" +
    "\r\n" +
    "PONG"
  } .getBytes

  def serve(s: SocketChannel, timeout: Timeout) = {
    val buf = ByteBuffer.allocate(4096)
    readSome(s, buf, timeout).flatMap {
      case n if n > 0 =>
        write(s, ByteBuffer.wrap(resp), timeout)
      case _ =>
        unitM
    }
  }

  def serverLoop[W <: WaitOp](ss: ServerSocketChannel, sh: W,
                              timeout: Timeout) = repeatM {
    accept(ss, sh).flatMap {
      case Right(s) =>
        s.configureBlocking(false)
        fork {
          guard {
            serve(s, timeout)
          } .cleanup {
            close(s)
          }
        } >>
        continueM
      case Left(_) =>
        breakM
    }
  }

  def main(args: Array[String]) {
    val parser = new ArgotParser("pong-http-server")
    val addressOpt = parser.option[InetAddress](
                       List("a", "address"), "ADDRESS",
                       "Address to bind to" +
                       " (default is to listen on all local addresses)") {
      (str, opt) =>
        try {
          InetAddress.getByName(str)
        } catch {
          case e: Throwable =>
            parser.usage("Invalid address `%s': %s" format (str, e.getMessage))
        }
    }
    val portOpt = parser.option[Int](
                    List("p", "port"), "PORT",
                    "Port to bind to (defaults to 8080)") {
      (str, opt) =>
        try {
          val i = Integer.parseInt(str)
          if (i < 0 || i > 0xFFFF)
            throw new IllegalArgumentException
          i
        } catch {
          case _ =>
            parser.usage("Invalid port number `%s'" format (str))
        }
    }
    val timeoutOpt = parser.option[Timeout](
                       List("t", "timeout"), "TIMEOUT",
                       "Timeout for network operations, in seconds" +
                       " (defaults to infinity)") {
      (str, opt) =>
        str match {
          case "inf" | "infinity" =>
            Timeout.Inf
          case _ =>
            try {
              val i = Integer.parseInt(str)
              if (i < 0)
                throw new IllegalArgumentException
              i.s
            } catch {
              case _ =>
                parser.usage("Invalid timeout value `%s'" format (str))
            }
        }
    }

    try {
      parser.parse(args)
    } catch {
      case e: ArgotUsageException =>
        println(e.message)
        System.exit(1)
    }

    val e = (new DefaultNioMultiAsyncExecutor).start
    val Success(sh) = e.invoke(createSignal[Unit])

    val ss = try {
      val ss = ServerSocketChannel.open
      val port = portOpt.value.getOrElse(8080)
      ss.configureBlocking(false)
      ss.socket.bind {
        addressOpt.value match {
          case Some(address) =>
            new InetSocketAddress(address, port)
          case None =>
            new InetSocketAddress(port)
        }
      }
      ss
    } catch {
      case t: Throwable =>
        e.shutdown
        throw t
    }

    println("Listening on " + ss.socket.getInetAddress.getHostAddress +
            ", port " + ss.socket.getLocalPort)

    Runtime.getRuntime.addShutdownHook {
      new Thread {
        override def run {
          e.execute(emitSignal(sh))
          e.shutdown
        }
      }
    }

    val timeout = timeoutOpt.value.getOrElse(Timeout.Inf)

    e.execute {
      guard {
        serverLoop(ss, AwaitSignal(sh), timeout)
      } .cleanup {
        close(ss) >>
        emitSignal(sh)
      }
    }
  }
}
