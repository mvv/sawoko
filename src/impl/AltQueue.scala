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

object AltQueue {
  final class Node[A](private[AltQueue] val value: A) {
    private[AltQueue] var prev: Node[A] = null
    private[AltQueue] var next: Node[A] = null
  }
}
final class AltQueue[A] {
  import AltQueue._
  private var first: Node[A] = null
  private var last: Node[A] = null

  def isEmpty = first == null
  def peek: Option[A] = if (first == null) None else Some(first.value)
  def take: Option[A] = {
    if (first == null)
      None
    else {
      val value = first.value
      val second = first.next
      first.next = null
      first = second
      if (second == null)
        last = null
      else
        second.prev = null
      Some(value)
    }
  }
  def push(value: A): Node[A] = {
    val node = new Node(value)
    if (last == null) {
      first = node
      last = node
    } else {
      node.prev = last
      last.next = node
      last = node
    }
    node
  }
  def remove(node: Node[A]): Boolean = {
    val prev = node.prev
    val next = node.next
    if (prev == null) {
      if (node != first)
        return false
      first = next
    } else
      prev.next = next
    if (next == null) {
      if (node != last)
        return false
      last = prev
    } else
      next.prev = prev
    node.prev = null
    node.next = null
    true
  }
  def clear: AltQueue[A] = {
    val result = new AltQueue[A]
    result.first = first
    result.last = last
    first = null
    last = null
    result
  }
  def foreach(f: A => Unit) {
    var node = first
    while (node != null) {
      f(node.value)
      node = node.next
    }
  }
}