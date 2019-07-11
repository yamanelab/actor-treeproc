package csl.dataproc.tgls.util;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.*;

public class OnMemoryBufferFactory implements ByteBufferArray.BufferFactory {
    protected Path file;
    protected Map<Integer,ByteBufferArray.OnMemoryEntry> buffers = new TreeMap<>();
    protected int maxIndex;
    protected long limit = Long.MAX_VALUE;

    public OnMemoryBufferFactory() {
    }

    public OnMemoryBufferFactory(long limit, Path file) {
        this.limit = limit;
        this.file = file;
    }

    public Map<Integer, ByteBufferArray.OnMemoryEntry> getBuffers() {
        return buffers;
    }

    @Override
    public ByteBuffer createBuffer(int bufferIndex, long minDataIndex, long maxDataIndex) {
        ByteBufferArray.OnMemoryEntry b = buffers.get(bufferIndex);
        if (b == null) {
            if (maxIndex < bufferIndex) {
                maxIndex = bufferIndex;
            }
            int size = (int) (maxDataIndex - minDataIndex);
            b = new ByteBufferArray.OnMemoryEntry(minDataIndex, ByteBuffer.allocateDirect(size).order(ByteOrder.BIG_ENDIAN));
            buffers.put(bufferIndex, b);
        }
        return b.buffer;
    }

    public void setFile(Path file) {
        this.file = file;
    }

    public Path getFile() {
        return file;
    }

    @Override
    public long getLimit() {
        return limit;
    }

    @Override
    public void closeWithSize(long totalSize) {
        if (file != null) {
            saveTo(file);
        }
        buffers.clear();
    }

    public void saveTo(Path file) {
        try (FileChannel channel = FileChannel.open(file,
                StandardOpenOption.WRITE, StandardOpenOption.CREATE,
                StandardOpenOption.TRUNCATE_EXISTING);) {

            List<Integer> keys = new ArrayList<>(buffers.keySet());
            Collections.sort(keys);

            for (int k : keys) {
                ByteBufferArray.OnMemoryEntry buf = buffers.get(k);
                buf.buffer.clear();
                channel.position(buf.minDataIndex);
                channel.write(buf.buffer);
            }
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    @Override
    public void closeBuffer(int bufferIndex, ByteBuffer buffer) {
    }

    @Override
    public String toString() {
        return String.format("OnMemory(maxIndex=%,d, limit=%,d, file=%s)", maxIndex, limit, file);
    }
}
