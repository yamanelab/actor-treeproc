package csl.dataproc.tgls.util;

import java.io.Closeable;
import java.io.File;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.util.Arrays;

public class ByteBufferArray implements Closeable {
    protected ByteBuffer[] buffers;
    protected BufferIndexer indexer;
    protected BufferFactory factory;
    protected long limit = Long.MAX_VALUE;

    public static ByteBufferArray createForWrite(File file) {
        return new ByteBufferArray(EXP_INDEXER, new SingleMappedBufferFactory(file.toPath(), false));
    }
    public static ByteBufferArray createForRead(File file) {
        return new ByteBufferArray(EXP_INDEXER, new SingleMappedBufferFactory(file.toPath(), true));
    }
    public static ByteBufferArray createForWrite(Path file) {
        return new ByteBufferArray(EXP_INDEXER, new SingleMappedBufferFactory(file, false));
    }
    public static ByteBufferArray createForReadWrite(Path file) {
        return new ByteBufferArray(EXP_INDEXER, new SingleMappedBufferFactory(SingleMappedBufferFactory.openForReadWrite(file), false));
    }

    public static ByteBufferArray createForRead(Path file) {
        return new ByteBufferArray(EXP_INDEXER, new SingleMappedBufferFactory(file, true));
    }
    public static ByteBufferArray createForRead(FileChannel fileChannel) {
        return new ByteBufferArray(EXP_INDEXER, new SingleMappedBufferFactory(fileChannel, false));
    }

    public static ByteBufferArray createForWrite(FileChannel fileChannel) {
        return new ByteBufferArray(EXP_INDEXER, new SingleMappedBufferFactory(fileChannel, false));
    }

    public static ByteBufferArray createOnMemory(long size, File file) {
        return new ByteBufferArray(EXP_INDEXER, new OnMemoryBufferFactory(size, file.toPath()));
    }
    public static ByteBufferArray createOnMemory(long size, Path file) {
        return new ByteBufferArray(EXP_INDEXER, new OnMemoryBufferFactory(size, file));
    }

    public ByteBufferArray(BufferIndexer indexer, BufferFactory factory) {
        this.indexer = indexer;
        this.factory = factory;
        buffers = new ByteBuffer[3];
        limit(factory.getLimit());
    }

    public ByteBufferArray limit(long limit) {
        this.limit = limit;
        return this;
    }

    public long limit() {
        return limit;
    }

    public BufferIndexer getIndexer() {
        return indexer;
    }

    public BufferFactory getFactory() {
        return factory;
    }

    /** the position of the returned buffer is set to an index associated with dataIndex */
    public ByteBuffer getBuffer(long dataIndex) {
        int bufferIndex = indexer.getBufferIndexFromDataIndex(dataIndex);
        return getBuffer(dataIndex, bufferIndex);
    }

    protected ByteBuffer getBuffer(long dataIndex, int bufferIndex) {
        if (dataIndex < 0) {
            throw new IllegalArgumentException("invalid: " + dataIndex + " @" + bufferIndex);
        }
        expandBuffers(bufferIndex);
        long minDataIndex = indexer.getMinDataIndexForBufferIndex(bufferIndex);
        ByteBuffer buffer = getOrCreate(bufferIndex, minDataIndex);
        if (buffer != null) {
            try {
                buffer.position((int) (dataIndex - minDataIndex));
            } catch (Exception ex) {
                throw new RuntimeException("dataIndex: " + dataIndex+ " @" + bufferIndex + " min:" + minDataIndex, ex);
            }
        }
        return buffer;
    }
    protected void expandBuffers(int bufferIndex) {
        if (bufferIndex >= buffers.length) {
            int nextSize = (bufferIndex / 5 + 1) * 5;
            try {
                buffers = Arrays.copyOf(buffers, nextSize);
            } catch (OutOfMemoryError e) {
                System.err.println(bufferIndex);
                System.err.println(nextSize);
                throw e;
            }
        }
    }

    protected static final ByteBuffer EMPTY_BUFFER = ByteBuffer.wrap(new byte[0]);

    protected ByteBuffer getOrCreate(int bufferIndex, long minDataIndex) {
        ByteBuffer buffer = buffers[bufferIndex];
        if (buffer == null) {
            try {
                buffer = factory.createBuffer(bufferIndex,
                        Math.min(minDataIndex, limit),
                        Math.min(indexer.getMaxDataIndexForBufferIndex(bufferIndex), limit));
            } catch (Exception ex) { //it may be an out-of-memory error
                closeBuffers();
                //retry
                buffer = factory.createBuffer(bufferIndex,
                        Math.min(minDataIndex, limit),
                        Math.min(indexer.getMaxDataIndexForBufferIndex(bufferIndex), limit));
            }
            if (buffer == null) {
                buffer = EMPTY_BUFFER;
            }
            buffers[bufferIndex]= buffer;
        }
        if (buffer == EMPTY_BUFFER) {
            return null;
        } else {
            return buffer;
        }
    }

    public void close() {
        closeWithSize(-1L);
    }
    public void closeWithSize(long size) {
        closeBuffers();
        factory.closeWithSize(size);
    }
    protected void closeBuffers() {
        for (int i = 0, len = buffers.length; i < len; ++i) {
            ByteBuffer buffer = buffers[i];
            if (buffer != null) {
                factory.closeBuffer(i, buffer);
                buffers[i] = null;
            }
        }
    }


    public interface BufferIndexer {
        int getBufferIndexFromDataIndex(long dataIndex);
        /** inclusive */
        long getMinDataIndexForBufferIndex(int bufferIndex);
        /** exclusive */
        long getMaxDataIndexForBufferIndex(int bufferIndex);

    }

    public interface BufferFactory {
        ByteBuffer createBuffer(int bufferIndex, long minDataIndex, long maxDataIndex);
        void closeBuffer(int bufferIndex, ByteBuffer buffer);
        void closeWithSize(long totalSize);
        long getLimit();
    }

    //////////////////////////////////

    public static final ExponentialBufferIndexer EXP_INDEXER = new ExponentialBufferIndexer();

    /**
     * <pre>
     *   0: [0    ... +2^16]
     *   1: [2^16 ... +2^16]
     *   2: [2^17 ...        +2^17]
     *   3: [2^18 ...              +2^18]
     *  ...
     *  15: [2^30 ...                   +2^30]
     *  ...:[2^31 ...                   +2^30]
     *  ...
     *  ...:[     ...                   +2^30]
     * </pre>
     */
    public static class ExponentialBufferIndexer implements BufferIndexer {
        protected final int minShift;
        protected final int maxShift;
        protected final int diffShift;
        protected final long minBufferDataSize;
        protected final long maxBufferDataSize;
        protected final long maxBufferDataSizeX2;

        protected final int minZeros;

        public ExponentialBufferIndexer() {
            this(16, 30);
        }

        public ExponentialBufferIndexer(int minShift, int maxShift) {
            this.minShift = minShift;
            this.maxShift = maxShift;
            diffShift = maxShift - minShift;
            minBufferDataSize = 1L << minShift;
            maxBufferDataSize = 1L << maxShift;
            maxBufferDataSizeX2 = maxBufferDataSize * 2L;
            minZeros = Long.numberOfLeadingZeros(minBufferDataSize);
        }

        @Override
        public int getBufferIndexFromDataIndex(long dataIndex) {
            if (dataIndex < minBufferDataSize) {
                return 0;
            } else if (dataIndex >= maxBufferDataSizeX2) {
                //return (int) (dataIndex / maxBufferDataSize) + diffShift;
                return (int) (dataIndex >>> maxShift) + diffShift;
            } else {
                /*
                long mask = maxBufferDataSize;
                int i = diffShift + 1;
                while ((mask & dataIndex) != mask) {
                    mask >>= 1;
                    i--;
                }
                return i;
                */
                return minZeros - Long.numberOfLeadingZeros(dataIndex) + 1;
            }
        }

        @Override
        public long getMinDataIndexForBufferIndex(int bufferIndex) {
            if (bufferIndex == 0) {
                return 0;
            } else if (bufferIndex <= diffShift) {
                return ~(0xFFFF_FFFF_FFFF_FFFFL << (bufferIndex + minShift - 1)) + 1;
            } else {
                //return (long) (bufferIndex - (diffShift)) * maxBufferDataSize;
                return (long) (bufferIndex - diffShift) << maxShift;
            }
        }

        @Override
        public long getMaxDataIndexForBufferIndex(int bufferIndex) {
            if (bufferIndex == 0) {
                return minBufferDataSize;
            } else if (bufferIndex <= diffShift) {
                return ~(0xFFFF_FFFF_FFFF_FFFFL << (bufferIndex + minShift)) + 1;
            } else {
                //return (long) (bufferIndex - (diffShift) + 1) * maxBufferDataSize;
                return (long) (bufferIndex - diffShift + 1) << maxShift;
            }
        }

        @Override
        public String toString() {
            return String.format("Exp(min=%,d,max=%,d)", minBufferDataSize, maxBufferDataSize);
        }
    }

    public static final BlockBufferIndexer BLOCK_INDEXER = new BlockBufferIndexer(1000_000L);

    public static class BlockBufferIndexer implements BufferIndexer {
        protected long blockSize;
        protected long length;

        public BlockBufferIndexer(long blockSize) {
            this(blockSize, Long.MAX_VALUE);
        }

        public BlockBufferIndexer(long blockSize, long length) {
            this.blockSize = blockSize;
            this.length = length;
        }

        public long getBlockSize() {
            return blockSize;
        }
        public long getLength() {
            return length;
        }

        @Override
        public int getBufferIndexFromDataIndex(long dataIndex) {
            return (int) (dataIndex / blockSize);
        }

        @Override
        public long getMinDataIndexForBufferIndex(int bufferIndex) {
            return ((long) bufferIndex) * blockSize;
        }

        @Override
        public long getMaxDataIndexForBufferIndex(int bufferIndex) {
            return Math.min(length, ((long) (bufferIndex + 1)) * blockSize);
        }

        @Override
        public String toString() {
            return String.format("Block(blockSize=%,d, len=%,d)", blockSize, length);
        }
    }

    //////////////////////////////////

    public static class OnMemoryEntry {
        public long minDataIndex;
        public ByteBuffer buffer;

        public OnMemoryEntry(long minDataIndex, ByteBuffer buffer) {
            this.minDataIndex = minDataIndex;
            this.buffer = buffer;
        }
    }


    //////////////////////////////////

    /**
     * <pre>
     *     ByteBuffer b = ba.put(i, d1, null);
     *     b = ba.put(i+1, d2, b);
     *     b = ba.put(i+2, d3, b);
     *     ...
     * </pre>
     * @param buffer null or a buffer for the position dataIndex-1
     * @return the buffer for the position dataIndex
     */
    public ByteBuffer put(long dataIndex, byte data, ByteBuffer buffer) {
        if (buffer == null || buffer.remaining() < 1) {
            buffer = getBuffer(dataIndex);
        }
        buffer.put(data);
        return buffer;
    }

    public ByteBuffer put(long dataIndex, byte data) {
        return getBuffer(dataIndex).put(data);
    }

    /**
     * <pre>
     *     long rem = data.remaining();
     *     ByteBuffer b = ba.put(i, data);
     *     b = ba.put(i + rem, v, b);
     *     ...
     * </pre>
     */
    public ByteBuffer put(long dataIndex, ByteBuffer data) {
        int bufferIndex = indexer.getBufferIndexFromDataIndex(dataIndex);
        long i = dataIndex;
        int dataRemain = data.remaining();
        int dataPosition = data.position();
        ByteBuffer result = null;
        while (dataRemain > 0) {
            long bufferRemain = indexer.getMaxDataIndexForBufferIndex(bufferIndex) - i;
            int dataLen = (int) Math.min(bufferRemain, dataRemain);

            data.limit(dataPosition + dataLen); //write to the buffer end
            result = getBuffer(i, bufferIndex);
            result.put(data);

            i += dataLen;
            dataRemain -= dataLen;
            dataPosition += dataLen;
            ++bufferIndex;
        }
        return result;
    }

    public ByteBuffer put(long dataIndex, ByteBuffer data, ByteBuffer buffer) {
        if (buffer == null) {
            return put(dataIndex, data);
        } else if (buffer.remaining() < data.remaining()) { //buffer overflow
            int realLimit = data.limit();
            int advance = buffer.remaining();
            data.limit(data.position() + advance);
            buffer.put(data);

            //write data rest
            data.limit(realLimit);
            return put(dataIndex + advance, data);
        } else {
            buffer.put(data);
            return buffer;
        }
    }

    public ByteBuffer get(long dataIndex, ByteBuffer target, ByteBuffer buffer) {
        while (target.hasRemaining()) {
            if (buffer == null || !buffer.hasRemaining()) {
                buffer = getBuffer(dataIndex);
                if (!buffer.hasRemaining()) {
                    return null;
                }
            }
            int oldLimit = buffer.limit();
            int consume = Math.min(target.remaining(), buffer.remaining());
            buffer.limit(buffer.position() + consume);
            target.put(buffer);
            buffer.limit(oldLimit);
            dataIndex += (long) consume;
        }
        return buffer;
    }

    public ByteBuffer skip(long dataIndex, int length, ByteBuffer buffer) {
        while (length > 0) {
            if (buffer == null || !buffer.hasRemaining()) {
                buffer = getBuffer(dataIndex);
                if (!buffer.hasRemaining()) {
                    return null;
                }
            }

            int consume = Math.min(buffer.remaining(), length);
            buffer.position(buffer.position() + consume);
            length -= consume;
            dataIndex += (long) consume;
        }
        return buffer;
    }

    //////////////////////////////////////////////

    protected ByteBuffer lastBuffer;
    protected long lastPosition;
    protected ByteBuffer primitiveBuffer = ByteBuffer.allocateDirect(8).order(ByteOrder.BIG_ENDIAN);

    public ByteBufferArray order(ByteOrder order) {
        primitiveBuffer.order(order);
        if (lastBuffer != null) {
            lastBuffer.order(order);
        }
        return this;
    }

    public ByteBufferArray position(long pos) {
        if (pos < 0) {
            throw new RuntimeException(lastPosition + " -> "+ pos);
        }
        if (this.lastPosition < pos) {
            long fwd = pos - lastPosition;
            if (lastBuffer != null) {
                if (fwd >= lastBuffer.remaining()) {
                    this.lastBuffer = null;
                } else {
                    lastBuffer.position(lastBuffer.position() + (int) fwd);
                }
            }
            this.lastPosition = pos;
        } else if (this.lastPosition > pos) {
            long back = lastPosition - pos;
            if (lastBuffer != null) {
                int lastBufferPos = lastBuffer.position();
                if (back > lastBufferPos) {
                    this.lastBuffer = null;
                } else {
                    lastBuffer.position(lastBufferPos - (int) back);
                }
            }
            this.lastPosition = pos;
        }
        return this;
    }

    public long position() {
        return lastPosition;
    }

    public ByteBuffer buffer() {
        return lastBuffer;
    }

    public ByteBufferArray putLong(long v) {
        ByteBuffer lastBuf = this.lastBuffer;
        if (lastBuf != null && lastBuf.remaining() >= 8) {
            lastBuf.putLong(v);
        } else {
            ByteBuffer primitiveBuffer = this.primitiveBuffer;
            primitiveBuffer.clear();
            primitiveBuffer.putLong(v);
            primitiveBuffer.flip();

            setLastBuffer(put(lastPosition, primitiveBuffer, lastBuf));
        }
        lastPosition += 8L;
        return this;
    }

    public ByteBufferArray putInt(int v) {
        ByteBuffer lastBuf = this.lastBuffer;
        if (lastBuf != null && lastBuf.remaining() >= 4) {
            lastBuf.putInt(v);
        } else {
            ByteBuffer primitiveBuffer = this.primitiveBuffer;
            primitiveBuffer.clear();
            primitiveBuffer.putInt(v);
            primitiveBuffer.flip();

            setLastBuffer(put(lastPosition, primitiveBuffer, lastBuf));
        }
        lastPosition += 4L;
        return this;
    }

    public ByteBufferArray putShort(short v) {
        ByteBuffer lastBuf = this.lastBuffer;
        if (lastBuf != null && lastBuf.remaining() >= 2) {
            lastBuf.putShort(v);
        } else {
            ByteBuffer primitiveBuffer = this.primitiveBuffer;
            primitiveBuffer.clear();
            primitiveBuffer.putShort(v);
            primitiveBuffer.flip();

            setLastBuffer(put(lastPosition, primitiveBuffer, lastBuf));
        }
        lastPosition += 2L;
        return this;
    }

    public ByteBufferArray put(byte v) {
        ByteBuffer lastBuf = this.lastBuffer;
        if (lastBuf != null && lastBuf.remaining() >= 1) {
            lastBuf.put(v);
        } else {
            ByteBuffer primitiveBuffer = this.primitiveBuffer;
            primitiveBuffer.clear();
            primitiveBuffer.put(v);
            primitiveBuffer.flip();

            setLastBuffer(put(lastPosition, primitiveBuffer, lastBuf));
        }
        lastPosition += 1L;
        return this;
    }

    public ByteBufferArray put(ByteBuffer buffer) {
        int len = buffer.remaining();
        setLastBuffer(put(lastPosition, buffer, lastBuffer));
        lastPosition += (len - buffer.remaining());
        return this;
    }


    public long getLong() {
        ByteBuffer lastBuf = this.lastBuffer;
        if (lastBuf != null && lastBuf.remaining() >= 8) {
            lastPosition += 8L;
            return lastBuf.getLong();
        } else {
            ByteBuffer primitiveBuffer = this.primitiveBuffer;
            primitiveBuffer.clear();
            setLastBuffer(get(lastPosition, primitiveBuffer, lastBuffer));
            lastPosition += 8L;

            primitiveBuffer.flip();
            return primitiveBuffer.getLong();
        }
    }

    public int getInt() {
        ByteBuffer lastBuf = this.lastBuffer;
        if (lastBuf != null && lastBuf.remaining() >= 4) {
            lastPosition += 4L;
            return lastBuf.getInt();
        } else {
            ByteBuffer primitiveBuffer = this.primitiveBuffer;
            primitiveBuffer.clear();
            primitiveBuffer.limit(4);
            setLastBuffer(get(lastPosition, primitiveBuffer, lastBuffer));
            lastPosition += 4L;

            primitiveBuffer.flip();
            return primitiveBuffer.getInt();
        }
    }

    public short getShort() {
        ByteBuffer lastBuf = this.lastBuffer;
        if (lastBuf != null && lastBuf.remaining() >= 2) {
            lastPosition += 2L;
            return lastBuf.getShort();
        } else {
            ByteBuffer primitiveBuffer = this.primitiveBuffer;
            primitiveBuffer.clear();
            primitiveBuffer.limit(2);
            setLastBuffer(get(lastPosition, primitiveBuffer, lastBuffer));
            lastPosition += 2L;

            primitiveBuffer.flip();
            return primitiveBuffer.getShort();
        }
    }


    public byte get() {
        ByteBuffer lastBuf = this.lastBuffer;
        if (lastBuf != null && lastBuf.remaining() >= 1) {
            lastPosition += 1L;
            return lastBuf.get();
        } else {
            ByteBuffer primitiveBuffer = this.primitiveBuffer;
            primitiveBuffer.clear();
            primitiveBuffer.limit(1);
            setLastBuffer(get(lastPosition, primitiveBuffer, lastBuffer));
            lastPosition += 1L;

            primitiveBuffer.flip();
            return primitiveBuffer.get();
        }
    }

    protected void setLastBuffer(ByteBuffer b) {
        if (this.lastBuffer != b) {
            if (b != null) {
                b.order(primitiveBuffer.order());
            }
            this.lastBuffer = b;
        }
    }

    public ByteBufferArray get(ByteBuffer buffer) {
        int len = buffer.remaining();
        setLastBuffer(get(lastPosition, buffer, lastBuffer));
        lastPosition += (len - buffer.remaining());
        return this;
    }

    public ByteBufferArray skip(int length) {
        setLastBuffer(skip(lastPosition, length, lastBuffer));
        lastPosition += length;
        return this;
    }

    @Override
    public String toString() {
        return String.format("ByteBufferArray(lastPos=%,d, limit=%,d, buffers=%d, indexer=%s, factory=%s, lastBuf=%s)",
                lastPosition, limit, buffers.length, indexer, factory, lastBuffer);
    }
}
