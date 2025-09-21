/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.fs.aliyun.oss.v2.prefetchstream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Future;
import java.util.zip.CRC32;

public final class BufferData {

  private static final Logger LOG = LoggerFactory.getLogger(BufferData.class);

  public enum State {
    /**
     * Unknown / invalid state.
     */
    UNKNOWN,

    /**
     * Buffer has been acquired but has no data.
     */
    BLANK,

    /**
     * This block is being prefetched.
     */
    PREFETCHING,

    /**
     * This block is being added to the local cache.
     */
    CACHING,

    /**
     * This block has data and is ready to be read.
     */
    READY,

    /**
     * This block is no longer in-use and should not be used once in this state.
     */
    DONE
  }

  private final int blockNumber;
  private ByteBuffer buffer;
  private volatile State state;
  private Future<Void> action;
  private long checksum = 0;

  public BufferData(int blockNumber, ByteBuffer buffer) {
    Validate.checkNotNegative(blockNumber, "blockNumber");
    Validate.checkNotNull(buffer, "buffer");

    this.blockNumber = blockNumber;
    this.buffer = buffer;
    this.state = State.BLANK;
  }

  public int getBlockNumber() {
    return this.blockNumber;
  }

  public ByteBuffer getBuffer() {
    return this.buffer;
  }

  public State getState() {
    return this.state;
  }

  public long getChecksum() {
    return this.checksum;
  }

  public static long getChecksum(ByteBuffer buffer) {
    ByteBuffer tempBuffer = buffer.duplicate();
    tempBuffer.rewind();
    CRC32 crc32 = new CRC32();
    crc32.update(tempBuffer);
    return crc32.getValue();
  }

  public synchronized Future<Void> getActionFuture() {
    return this.action;
  }

  public synchronized void setPrefetch(Future<Void> actionFuture) {
    Validate.checkNotNull(actionFuture, "actionFuture");

    this.updateState(State.PREFETCHING, State.BLANK);
    this.action = actionFuture;
  }

  public synchronized void setCaching(Future<Void> actionFuture) {
    Validate.checkNotNull(actionFuture, "actionFuture");

    this.throwIfStateIncorrect(State.PREFETCHING, State.READY);
    this.state = State.CACHING;
    this.action = actionFuture;
  }

  public synchronized void setReady(State... expectedCurrentState) {
    if (this.checksum != 0) {
      throw new IllegalStateException("Checksum cannot be changed once set");
    }

    this.buffer = this.buffer.asReadOnlyBuffer();
    this.checksum = getChecksum(this.buffer);
    this.buffer.rewind();
    this.updateState(State.READY, expectedCurrentState);
  }

  public synchronized void setDone() {
    if (this.checksum != 0) {
      if (getChecksum(this.buffer) != this.checksum) {
        throw new IllegalStateException("checksum changed after setReady()");
      }
    }
    this.state = State.DONE;
    this.action = null;
  }

  public synchronized void updateState(State newState,
      State... expectedCurrentState) {
    Validate.checkNotNull(newState, "newState");
    Validate.checkNotNull(expectedCurrentState, "expectedCurrentState");

    this.throwIfStateIncorrect(expectedCurrentState);
    this.state = newState;
  }

  public void throwIfStateIncorrect(State... states) {
    Validate.checkNotNull(states, "states");

    if (this.stateEqualsOneOf(states)) {
      return;
    }

    List<String> statesStr = new ArrayList<String>();
    for (State s : states) {
      statesStr.add(s.toString());
    }

    String message = String.format(
        "Expected buffer state to be '%s' but found: %s",
        String.join(" or ", statesStr), this);
    throw new IllegalStateException(message);
  }

  public boolean stateEqualsOneOf(State... states) {
    State currentState = this.state;

    for (State s : states) {
      if (currentState == s) {
        return true;
      }
    }

    return false;
  }

  public String toString() {

    return String.format(
        "[%03d] id: %03d, %s: buf: %s, checksum: %d, future: %s",
        this.blockNumber,
        System.identityHashCode(this),
        this.state,
        this.getBufferStr(this.buffer),
        this.checksum,
        this.getFutureStr(this.action));
  }

  private String getFutureStr(Future<Void> f) {
    if (f == null) {
      return "--";
    } else {
      return this.action.isDone() ? "done" : "not done";
    }
  }

  private String getBufferStr(ByteBuffer buf) {
    if (buf == null) {
      return "--";
    } else {
      return String.format(
          "(id = %d, pos = %d, lim = %d)",
          System.identityHashCode(buf),
          buf.position(), buf.limit());
    }
  }
}
