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

import scala.collection.mutable
import java.nio.channels.{SelectableChannel, Selector, SelectionKey,
                          ClosedChannelException}
import java.util.concurrent.{LinkedBlockingQueue}
import java.util.concurrent.atomic.AtomicBoolean
import com.github.mvv.sawoko._

trait NioMultiAsyncExecutor extends MultiAsyncExecutor with NioAsyncExecutor {
    self: WaitManyTimeoutMultiAsyncExecutor[NioPollWaitCap] =>

  private object NioChannels {
    private final class State(val channel: SelectableChannel) {
      var selectionKey: SelectionKey = null
      var acceptPoints = Set.empty[WaitOpResumePoint[Int]]
      var connectPoints = Set.empty[WaitOpResumePoint[Int]]
      var readPoints = Set.empty[WaitOpResumePoint[Int]]
      var writePoints = Set.empty[WaitOpResumePoint[Int]]
      var updatePoints = Set.empty[WaitOpResumePoint[Int]]
      var oldInterestOps = 0
      var newInterestOps = 0
      var detached = false
      var closeWaiters: List[Pid] = Nil
    }
  }
  private final class NioChannels {
    import NioChannels._
    import SelectionKey._

    private val states = new mutable.HashMap[SelectableChannel, State]
    private val enqueued = new LinkedBlockingQueue[State]

    def register(point: WaitOpResumePoint[Int],
                 channel: SelectableChannel, ops: Int) = {
      val (state, wakeUp) = channel.synchronized {
        if (!channel.isOpen)
          throw new ClosedChannelException
        val state = states.synchronized {
          states.getOrElse(channel, {
              val state = new State(channel)
              states += channel -> state
              state
            })
        }
        if ((ops & OP_ACCEPT) != 0)
          state.acceptPoints += point
        if ((ops & OP_CONNECT) != 0)
          state.connectPoints += point
        if ((ops & OP_READ) != 0)
          state.readPoints += point
        if ((ops & OP_WRITE) != 0)
          state.writePoints += point
        state.newInterestOps = state.oldInterestOps | ops
        val wakeUp = if (state.newInterestOps != state.oldInterestOps) {
          state.updatePoints += point
          if (state.closeWaiters.isEmpty) {
            enqueued.put(state)
            true
          } else
            false
        } else
          false
        (state, wakeUp)
      }
      if (wakeUp && nioWokenUp.compareAndSet(false, true))
        nioSelector.wakeup
      new WaitOpCanceller {
        val handlesTimeout = true
        def cancel {
          channel.synchronized {
            if (state.detached)
              return
            var interestOps = 0
            if ((ops & OP_ACCEPT) != 0) {
              state.acceptPoints -= point
              if (!state.acceptPoints.isEmpty)
                interestOps |= OP_ACCEPT
            }
            if ((ops & OP_CONNECT) != 0) {
              state.connectPoints -= point
              if (!state.connectPoints.isEmpty)
                interestOps |= OP_CONNECT
            }
            if ((ops & OP_READ) != 0) {
              state.readPoints -= point
              if (!state.readPoints.isEmpty)
                interestOps |= OP_READ
            }
            if ((ops & OP_WRITE) != 0) {
              state.writePoints -= point
              if (!state.writePoints.isEmpty)
                interestOps |= OP_WRITE
            }
            state.updatePoints -= point
            if (interestOps == 0) {
              if (state.closeWaiters.isEmpty) {
                states.synchronized {
                  states.remove(channel)
                }
                state.detached = true
              }
            } else {
              if (state.closeWaiters.isEmpty &&
                  state.updatePoints.isEmpty &&
                  state.newInterestOps != interestOps)
                enqueued.put(state)
            }
            state.newInterestOps = interestOps
          }
        }
      }
    }

    def close(pid: Pid, channel: SelectableChannel): Boolean = {
      channel.synchronized {
        states.synchronized(states.get(channel)) match {
          case Some(state) =>
            if (state.closeWaiters.isEmpty &&
                state.newInterestOps == state.oldInterestOps)
              enqueued.put(state)
            state.closeWaiters ::= pid
            true
          case None =>
            channel.close
            false
        }
      }
    }

    def preSync(selector: Selector) = {
      var state: State = enqueued.poll
      while (state != null) {
        var toClose: Option[(Seq[Pid], Result[AsyncExecutor, Unit])] = None
        var toCancel: List[(WaitOpResumePoint[Int], Throwable)] = Nil
        val channel = state.channel
        channel.synchronized {
          if (!state.detached && !state.closeWaiters.isEmpty) {
            val result = try {
              channel.close
              Success(())
            } catch {
              case e: Throwable => Threw(e)
            }
            toClose = Some(state.closeWaiters -> result)
            state.closeWaiters = Nil
            if (result.isSuccess) {
              val e = new ClosedChannelException
              if (state.selectionKey != null) {
                state.selectionKey.cancel
                state.selectionKey = null
              }
              state.acceptPoints.foreach(toCancel ::= _ -> e)
              state.connectPoints.foreach(toCancel ::= _ -> e)
              state.readPoints.foreach(toCancel ::= _ -> e)
              state.writePoints.foreach(toCancel ::= _ -> e)
              state.oldInterestOps = 0
              state.newInterestOps = 0
              states.synchronized { states.remove(channel) }
              state.detached = true
            } else if (state.newInterestOps == 0) {
              states.synchronized { states.remove(channel) }
              state.detached = true
            }
          }
          if (!state.detached &&
              state.newInterestOps != state.oldInterestOps) {
            try {
              state.selectionKey = channel.register(
                                     selector, state.newInterestOps, state)
              state.oldInterestOps = state.newInterestOps
            } catch {
              case e: Throwable =>
                if (state.selectionKey == null) {
                  state.updatePoints.foreach(toCancel ::= _ -> e)
                  states.synchronized { states.remove(channel) }
                  state.detached = true
                } else {
                  state.updatePoints.foreach { point =>
                    state.acceptPoints -= point
                    state.connectPoints -= point
                    state.readPoints -= point
                    state.writePoints -= point
                    toCancel ::= point -> e
                  }
                  state.newInterestOps = state.oldInterestOps
                }
            }
            state.updatePoints = Set.empty
          }
        }
        toClose.foreach { case (pids, result) =>
          pids.foreach { pid =>
            resumeProcess(pid, result)
          }
        }
        toClose = None
        toCancel.foreach { case (point, cause) =>
          point.failed(cause)
        }
        toCancel = Nil
        state = enqueued.poll
      }
      nioWokenUp.set(false)
    }

    def postSync(selector: Selector) = {
      import scala.collection.JavaConversions._
      var seenPoints = Set.empty[WaitOpResumePoint[Int]]
      var toResume: List[(WaitOpResumePoint[Int], Int)] = Nil
      def addToResume(point: WaitOpResumePoint[Int], op: Int) {
        if (!seenPoints(point)) {
          seenPoints += point
          toResume ::= point -> op
        }
      }
      selector.selectedKeys.foreach { key =>
        val state = key.attachment.asInstanceOf[State]
        val channel = state.channel
        channel.synchronized {
          if (!state.detached) {
            val readyOps = key.readyOps
            val alreadyEnqueued =
              state.oldInterestOps != state.newInterestOps ||
              !state.closeWaiters.isEmpty
            var interestOps = state.newInterestOps
            if ((readyOps & OP_ACCEPT) != 0) {
              state.acceptPoints.foreach(addToResume(_, OP_ACCEPT))
              state.acceptPoints = Set.empty
              interestOps &= ~OP_ACCEPT
            }
            if ((readyOps & OP_CONNECT) != 0) {
              state.connectPoints.foreach(addToResume(_, OP_CONNECT))
              state.connectPoints = Set.empty
              interestOps &= ~OP_CONNECT
            }
            if ((readyOps & OP_READ) != 0) {
              state.readPoints.foreach(addToResume(_, OP_READ))
              state.readPoints = Set.empty
              interestOps &= ~OP_READ
            }
            if ((readyOps & OP_WRITE) != 0) {
              state.writePoints.foreach(addToResume(_, OP_WRITE))
              state.writePoints = Set.empty
              interestOps &= ~OP_WRITE
            }
            state.newInterestOps = interestOps
            if (interestOps == 0) {
              key.cancel
              state.selectionKey = null
              if (state.closeWaiters.isEmpty) {
                states.synchronized { states.remove(channel) }
                state.detached = true
              }
            } else if (!alreadyEnqueued &&
                       state.newInterestOps != state.oldInterestOps)
              enqueued.put(state)
          }
        }
        toResume.foreach { case (point, op) =>
          point.resume(op)
        }
        toResume = Nil
      }
      try {
        selector.selectNow
      } catch {
        case _ =>
      }
    }

    def clear {
      states.synchronized(states.clear)
      enqueued.clear
    }
  }

  @volatile
  private var nioTerminating = false
  private val nioChannels = new NioChannels
  private val nioImmediateSelector = new ThreadLocal[Selector]
  private val nioImmediateChannel = new ThreadLocal[SelectableChannel]
  private var nioSelector: Selector = null
  private val nioWokenUp = new AtomicBoolean(false)
  private def newNioThread = new Thread {
    override def run {
      val selector = nioSelector
      while(!nioTerminating) {
        nioChannels.preSync(selector)
        try {
          selector.select(500)
        } catch {
          case e: Throwable =>
        }
        nioChannels.postSync(selector)
      }
      selector.close
      nioChannels.clear
    }
  }

  protected override def onPoolThreadTermination {
    nioImmediateSelector.get match {
      case null =>
      case selector =>
        try { selector.close } catch { case _ => }
        nioImmediateSelector.remove
    }
    super.onPoolThreadTermination
  }

  protected override def doStart(rest: => Unit) = super.doStart {
    nioTerminating = false
    nioSelector = Selector.open
    val nioThread = newNioThread
    nioThread.setName("Sawoko [" + this + "] :: NIO Dispatcher")
    try {
      nioThread.start
    } catch {
      case e: Throwable =>
        nioSelector.close
        nioSelector = null
        throw e
    }
    try {
      rest
    } catch {
      case e: Throwable =>
        nioTerminating = true
        nioSelector.wakeup
        nioSelector = null
        nioThread.join
        throw e
    }
  }

  protected override def doStop {
    nioTerminating = true
    nioSelector.wakeup
    nioSelector = null
    super.doStop
  }

  def registerNioClose(
        pid: Pid, channel: SelectableChannel,
        callback: Callback[NioAsyncExecutor, Unit]) =
    if (!channel.isOpen)
      false
    else {
      suspendProcess(pid, callback)
      val registered = try {
        nioChannels.close(pid, channel)
      } catch {
        case e: Throwable =>
          unsuspendProcess(pid)
          throw e
      }
      if (!registered)
        unsuspendProcess(pid)
      registered
    }

  registerWaitOpHandler {
    new WaitOpHandler[NioPoll] {
      val hasCap = implicitly[HasCap]
      def check(pid: Pid, op: NioPoll) = {
        val selector = nioImmediateSelector.get match {
          case null =>
            val selector = Selector.open
            nioImmediateSelector.set(selector)
            selector
          case selector =>
            selector
        }
        val lastChannel = nioImmediateChannel.get
        if (lastChannel != null && lastChannel != op.channel) {
          val lastKey = lastChannel.keyFor(selector)
          if (lastKey != null)
            lastKey.cancel
          nioImmediateChannel.set(null)
        }
        val key = op.channel.register(selector, op.ops)
        nioImmediateChannel.set(op.channel)
        if (selector.selectNow == 0) None
        else Some(key.readyOps & op.ops)
      }
      def register(point: WaitOpResumePoint[Int], op: NioPoll) = {
        check(point.pid, op) match {
          case Some(readyOps) =>
            point.resume(readyOps)
            WaitOpCanceller.Noop
          case None =>
            nioChannels.register(point, op.channel, op.ops)
        }
      }
    }
   }
}
