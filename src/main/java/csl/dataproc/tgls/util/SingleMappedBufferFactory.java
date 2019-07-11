package csl.dataproc.tgls.util;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

public class SingleMappedBufferFactory implements ByteBufferArray.BufferFactory {
    protected Path file;
    protected FileChannel channel;
    protected boolean readOnly;

    public SingleMappedBufferFactory(Path file, boolean readOnly) {
        this.file = file;
        this.readOnly = readOnly;
        init();
    }
    protected void init() {
        try {
            if (readOnly) {
                channel = FileChannel.open(file, StandardOpenOption.READ);
            } else {
                channel = FileChannel.open(file, StandardOpenOption.READ,
                        StandardOpenOption.WRITE, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
            }
        } catch (Exception ex) {
            throw new RuntimeException("file:" + file + " readOnly:" + readOnly, ex);
        }
    }
    public SingleMappedBufferFactory(FileChannel channel, boolean readOnly) {
        this.channel = channel;
        this.readOnly = readOnly;
    }

    public static FileChannel openForReadWrite(Path file) {
        try {
            return FileChannel.open(file, StandardOpenOption.READ,
                    StandardOpenOption.WRITE, StandardOpenOption.CREATE);
        } catch (Exception ex) {
            throw new RuntimeException("file:" + file + " readAndWrite");
        }
    }

    @Override
    public ByteBuffer createBuffer(int bufferIndex, long minDataIndex, long maxDataIndex) {
        int bufferSize = (int) (maxDataIndex - minDataIndex);
        try {
            if (channel == null) {
                init();
            }
            if (readOnly) {
                long maxSize = getFileSize() - minDataIndex;
                if (maxSize <= 0) {
                    return null;
                }
                if (maxSize < bufferSize) {
                    bufferSize = (int) maxSize;
                }
            }

            //MappedByteBuffer
            return  channel.map(
                        readOnly ? FileChannel.MapMode.READ_ONLY : FileChannel.MapMode.READ_WRITE,
                        minDataIndex, bufferSize)
                    .order(ByteOrder.BIG_ENDIAN);

        } catch (Exception ex) {
            throw new RuntimeException("map range:" + minDataIndex + "," + maxDataIndex
                    + " size:" + bufferSize + " readOnly:" + readOnly + " file:" + file, ex);
        }
    }
    protected long getFileSize() {
        try {
            if (file != null) {
                return Files.size(file);
            } else {
                return channel.size();
            }
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    @Override
    public long getLimit() {
        if (readOnly) {
            return getFileSize();
        }
        return Long.MAX_VALUE;
    }

    @Override
    public void closeWithSize(long totalSize) {
        if (totalSize >= 0 && !readOnly) {
            try {
                channel.truncate(totalSize);
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }
        close();
    }
    public void closeBuffer(int bufferIndex, ByteBuffer buffer) {
        if (buffer instanceof MappedByteBuffer) {
            ((MappedByteBuffer) buffer).force();
        }
    }
    public void close() {
        try {
            channel.close();
            channel = null;
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    @Override
    public String toString() {
        return String.format("Mapped(file=%s,readOnly=%s)", file, readOnly);
    }
}
