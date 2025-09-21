/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hadoop.fs.aliyun.oss.v2.prefetchstream;

import org.apache.hadoop.fs.CanSetReadahead;
import org.apache.hadoop.fs.FSExceptionMessages;
import org.apache.hadoop.fs.StreamCapabilities;

import org.apache.hadoop.fs.aliyun.oss.v2.OssManager;
import org.apache.hadoop.fs.aliyun.oss.v2.statistics.remotelog.ReadLogContext;
import org.apache.hadoop.fs.aliyun.oss.v2.statistics.remotelog.StreamLogContext;
import org.apache.hadoop.fs.statistics.IOStatisticsSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicLong;

import static java.util.Objects.requireNonNull;

/**
 * Provides an {@link InputStream} that allows reading from an oss file.
 */
public abstract class RemoteInputStream
        extends InputStream
        implements CanSetReadahead, StreamCapabilities, IOStatisticsSource {

    private static final Logger LOG = LoggerFactory.getLogger(
            RemoteInputStream.class);

    /**
     * The oss file read by this instance.
     */
    private RemoteObject remoteObject;

    /**
     * Reading of oss file takes place through this reader.
     */
    private RemoteObjectReader reader;

    /**
     * Name of this stream. Used only for logging.
     */
    private final String name;

    /**
     * Indicates whether the stream has been closed.
     */
    private volatile boolean closed;

    /**
     * Internal position within the file. Updated lazily
     * after a seek before a read.
     */
    private FilePosition fpos;

    /**
     * This is the actual position within the file, used by
     * lazy seek to decide whether to seek on the next read or not.
     */
    private long nextReadPos;

    /**
     * Information about each block of the mapped oss file.
     */
    private PrefetchBlockData blockData;

    /**
     * Read-specific operation context.
     */
    protected ReadOpContext context;

    /**
     * Attributes of the oss object being read.
     */
    private ObjectAttributes objectAttributes;

    /**
     * Callbacks used for interacting with the underlying oss client.
     */
    private OssManager client;


    private InputPolicy inputPolicy;

    protected StreamLogContext logContext;


    public RemoteInputStream(
            ReadOpContext context,
            ObjectAttributes ObjectAttributes,
            OssManager client) {

        this.context = requireNonNull(context);
        this.objectAttributes = requireNonNull(ObjectAttributes);
        this.client = requireNonNull(client);
        this.name = RemoteObject.getPath(ObjectAttributes);

        setInputPolicy(context.getInputPolicy());
        setReadahead(context.getReadahead());

        long fileSize = ObjectAttributes.getLen();
        int bufferSize = context.getPrefetchBlockSize();

        this.blockData = new PrefetchBlockData(fileSize, bufferSize);
        this.fpos = new FilePosition(fileSize, bufferSize);
        this.remoteObject = getOssFile();
        this.reader = new RemoteObjectReader(remoteObject);

        this.nextReadPos = 0;
    }


    /**
     * Sets the number of bytes to read ahead each time.
     *
     * @param readahead the number of bytes to read ahead each time..
     */
    @Override
    public synchronized void setReadahead(Long readahead) {
        // We support read head by prefetching therefore we ignore the supplied value.
        if (readahead != null) {
            Validate.checkNotNegative(readahead, "readahead");
        }
    }

    /**
     * Indicates whether the given {@code capability} is supported by this stream.
     *
     * @param capability the capability to check.
     * @return true if the given {@code capability} is supported by this stream, false otherwise.
     */
    @Override
    public boolean hasCapability(String capability) {
        return capability.equalsIgnoreCase(StreamCapabilities.IOSTATISTICS)
                || capability.equalsIgnoreCase(StreamCapabilities.READAHEAD);
    }

    /**
     * Set/update the input policy of the stream.
     * This updates the stream statistics.
     *
     * @param inputPolicy new input policy.
     */
    private void setInputPolicy(InputPolicy inputPolicy) {
        this.inputPolicy = inputPolicy;
    }

    /**
     * Returns the number of bytes that can read from this stream without blocking.
     */
    @Override
    public int available() throws IOException {
        throwIfClosed();

        // Update the current position in the current buffer, if possible.
        if (!fpos.setAbsolute(nextReadPos)) {
            return 0;
        }

        return fpos.buffer().remaining();
    }

    /**
     * Gets the current position.
     *
     * @return the current position.
     * @throws IOException if there is an IO error during this operation.
     */
    public long getPos() throws IOException {
        throwIfClosed();

        return nextReadPos;
    }

    /**
     * Moves the current read position so that the next read will occur at {@code pos}.
     *
     * @param pos the absolute position to seek to.
     * @throws IOException              if there an error during this operation.
     * @throws IllegalArgumentException if pos is outside of the range [0, file size].
     */
    public void seek(long pos) throws IOException {
        throwIfClosed();
        throwIfInvalidSeek(pos);

        nextReadPos = pos;
    }

    /**
     * Ensures that a non-empty valid buffer is available for immediate reading.
     * It returns true when at least one such buffer is available for reading.
     * It returns false on reaching the end of the stream.
     *
     * @return true if at least one such buffer is available for reading, false otherwise.
     * @throws IOException if there is an IO error during this operation.
     */
    protected abstract boolean ensureCurrentBuffer(int len, long loop, ReadLogContext readLogContext) throws IOException;

    @Override
    public int read() throws IOException {
        throwIfClosed();

        if (remoteObject.size() == 0
                || nextReadPos >= remoteObject.size()) {
            return -1;
        }

        ReadLogContext readLogContext =  new ReadLogContext(logContext);
        readLogContext.setOffset(-1);
        readLogContext.setLen(1);
        if (!ensureCurrentBuffer(1,-1L, readLogContext)) {
            return -1;
        }

        nextReadPos++;
        incrementBytesRead(1);

        return fpos.buffer().get() & 0xff;
    }

    /**
     * Reads bytes from this stream and copies them into
     * the given {@code buffer} starting at the beginning (offset 0).
     * Returns the number of bytes actually copied in to the given buffer.
     *
     * @param buffer the buffer to copy data into.
     * @return the number of bytes actually copied in to the given buffer.
     * @throws IOException if there is an IO error during this operation.
     */
    @Override
    public int read(byte[] buffer) throws IOException {
        return read(buffer, 0, buffer.length);
    }

    /**
     * Reads up to {@code len} bytes from this stream and copies them into
     * the given {@code buffer} starting at the given {@code offset}.
     * Returns the number of bytes actually copied in to the given buffer.
     *
     * @param buffer the buffer to copy data into.
     * @param offset data is copied starting at this offset.
     * @param len    max number of bytes to copy.
     * @return the number of bytes actually copied in to the given buffer.
     * @throws IOException if there is an IO error during this operation.
     */
    @Override
    public int read(byte[] buffer, int offset, int len) throws IOException {
        throwIfClosed();

        if (len == 0) {
            return 0;
        }

        if (remoteObject.size() == 0
                || nextReadPos >= remoteObject.size()) {
            return -1;
        }

        AtomicLong loop = new AtomicLong(0);
        ReadLogContext readLogContext =  new ReadLogContext(logContext);
        readLogContext.setOffset(offset);
        readLogContext.setLen(len);
        if (!ensureCurrentBuffer(len,loop.incrementAndGet(),readLogContext)) {
            return -1;
        }

        int numBytesRead = 0;
        int numBytesRemaining = len;

        while (numBytesRemaining > 0) {
            if (!ensureCurrentBuffer(numBytesRemaining,loop.incrementAndGet(),readLogContext)) {
                break;
            }

            ByteBuffer buf = fpos.buffer();
            int bytesToRead = Math.min(numBytesRemaining, buf.remaining());
            buf.get(buffer, offset, bytesToRead);
            nextReadPos += bytesToRead;
            incrementBytesRead(bytesToRead);
            offset += bytesToRead;
            numBytesRemaining -= bytesToRead;
            numBytesRead += bytesToRead;
        }

        return numBytesRead;
    }

    protected RemoteObject getFile() {
        return remoteObject;
    }

    protected RemoteObjectReader getReader() {
        return reader;
    }

    protected ObjectAttributes getObjectAttributes() {
        return objectAttributes;
    }

    protected FilePosition getFilePosition() {
        return fpos;
    }

    protected String getName() {
        return name;
    }

    protected boolean isClosed() {
        return closed;
    }

    protected long getNextReadPos() {
        return nextReadPos;
    }

    protected PrefetchBlockData getBlockData() {
        return blockData;
    }

    protected ReadOpContext getContext() {
        return context;
    }


    protected RemoteObject getOssFile() {
        return new RemoteObject(
                context,
                objectAttributes,
                client);
    }

    protected String getOffsetStr(long offset) {
        int blockNumber = -1;

        if (blockData.isValidOffset(offset)) {
            blockNumber = blockData.getBlockNumber(offset);
        }

        return String.format("%d:%d", blockNumber, offset);
    }

    /**
     * Closes this stream and releases all acquired resources.
     *
     * @throws IOException if there is an IO error during this operation.
     */
    @Override
    public void close() throws IOException {
        LOG.debug("Closing RemoteInputStream");

        if (closed) {
            return;
        }
        closed = true;

        blockData = null;
        reader.close();
        reader = null;
        remoteObject = null;
        fpos.invalidate();
//    try {
//      client.close();
//    } finally {
//    }
//    client = null;
    }

    @Override
    public boolean markSupported() {
        return false;
    }

    @Override
    public String toString() {
        if (isClosed()) {
            return "closed";
        }

        StringBuilder sb = new StringBuilder();
        sb.append(String.format("nextReadPos = (%d)%n", nextReadPos));
        sb.append(String.format("fpos = (%s)", fpos));
        return sb.toString();
    }

    protected void throwIfClosed() throws IOException {
        if (closed) {
            throw new IOException(
                    name + ": " + FSExceptionMessages.STREAM_IS_CLOSED);
        }
    }

    protected void throwIfInvalidSeek(long pos) throws EOFException {
        if (pos < 0) {
            throw new EOFException(FSExceptionMessages.NEGATIVE_SEEK + " " + pos);
        } else if (pos > this.getBlockData().getFileSize()) {
            throw new EOFException(FSExceptionMessages.CANNOT_SEEK_PAST_EOF + " " + pos);
        }
    }

    // Unsupported functions.

    @Override
    public void mark(int readlimit) {
        throw new UnsupportedOperationException("mark not supported");
    }

    @Override
    public void reset() {
        throw new UnsupportedOperationException("reset not supported");
    }

    @Override
    public long skip(long n) {
        throw new UnsupportedOperationException("skip not supported");
    }

    private void incrementBytesRead(int bytesRead) {
        if (bytesRead > 0) {
            fpos.incrementBytesRead(bytesRead);
        }
    }
}
