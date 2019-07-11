package csl.dataproc.csv.impl;

import csl.dataproc.tgls.util.ByteBufferArray;

import java.io.Closeable;
import java.io.IOException;
import java.io.Writer;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.channels.WritableByteChannel;
import java.nio.charset.Charset;
import java.nio.charset.CharsetEncoder;
import java.nio.charset.CoderResult;

public interface CsvCharOutput extends Closeable {
    void write(String s) throws IOException;
    void write(char c) throws IOException;
    void write(CharBuffer src) throws IOException;

    static WriterOutput get(Writer writer) {
        return new WriterOutput(writer);
    }

    static ByteChannelOutput get(WritableByteChannel out, Charset charset, ByteBuffer buffer) {
        return new ByteChannelOutput(out, charset, buffer);
    }

    static ByteBufferArrayOutput get(ByteBufferArray out, Charset charset) {
        return new ByteBufferArrayOutput(out, charset);
    }

    class WriterOutput implements CsvCharOutput {
        protected Writer writer;

        public WriterOutput(Writer writer) {
            this.writer = writer;
        }
        @Override
        public void write(String s) throws IOException {
            writer.write(s);
        }
        @Override
        public void write(CharBuffer s) throws IOException {
            writer.append(s);
        }
        @Override
        public void write(char c) throws IOException {
            writer.write(c);
        }
        @Override
        public void close() throws IOException {
            writer.close();
        }
    }

    static float BUFFER_LEAST_WRITE_RATE = 0.7f;

    abstract class ByteOutput implements CsvCharOutput {
        protected Charset charset;
        protected CharsetEncoder encoder;
        protected CharBuffer singleBuffer;
        protected CharBuffer unmappedBuffer;

        public ByteOutput(Charset charset) {
            this.charset = charset;
            this.encoder = charset.newEncoder();
            this.singleBuffer = CharBuffer.allocate(1);
            this.unmappedBuffer = CharBuffer.allocate(3); //surrogate-pair + 1
        }
        @Override
        public void write(String s) throws IOException {
            write(CharBuffer.wrap(s));
        }
        @Override
        public void write(char c) throws IOException {
            singleBuffer.clear();
            singleBuffer.put(c);
            singleBuffer.flip();
            write(singleBuffer);
        }
        @Override
        public void write(CharBuffer src) throws IOException {
            while (true) {
                ByteBuffer buffer = buffer();
                int pos = buffer.position();
                CoderResult r = encoder.encode(src, buffer, false);
                int len = buffer.position() - pos;

                if (r.isUnmappable()) {
                    CharBuffer unmapped = unmappedBuffer;
                    unmapped.clear();
                    char c = src.get();
                    while (isUnmapped(c)) {
                        if (!src.hasRemaining()) {
                            break;
                        } else {
                            unmapped.put(toUnmappedChar(c));
                            c = src.get();
                        }
                    }
                    unmapped.put(isUnmapped(c) ? toUnmappedChar(c) : c);
                    unmapped.flip();
                    write(unmapped);
                } else if (len == 0 && src.hasRemaining()) {
                    writeOverflow(src, buffer, len);

                } else if (!src.hasRemaining() && !r.isOverflow()) {
                    writeSourceEnd(len);
                    break;
                } else if (r.isOverflow()) {
                    writeBuffer(len);
                } else {
                    writeNext(len);
                }
            }
        }

        private void writeOverflow(CharBuffer src, ByteBuffer buffer, int len) throws IOException {
            int tmpSize = buffer.remaining() + 128;
            while (len == 0) { //if buffer does not have sufficient spaces for next src char
                ByteBuffer tmpBuffer = ByteBuffer.allocate(tmpSize);
                encoder.encode(src, tmpBuffer, false);
                len = tmpBuffer.position();
                tmpBuffer.flip();

                int prevTmpPos = tmpBuffer.position();
                while (tmpBuffer.hasRemaining()) {
                    int lastBufferRemaining = Math.min(len, buffer.remaining() + tmpBuffer.position());
                    tmpBuffer.limit(lastBufferRemaining);
                    if (tmpBuffer.hasRemaining()) {
                        buffer.put(tmpBuffer);
                    }
                    writeBuffer(lastBufferRemaining);
                    buffer = buffer();

                    tmpBuffer.limit(Math.min(len, buffer.remaining() + tmpBuffer.position()));
                    buffer.put(tmpBuffer);
                    if (tmpBuffer.position() == prevTmpPos) { //no progress
                        throw new IOException("cannot write anything: buffer=" + buffer);
                    } else {
                        prevTmpPos = tmpBuffer.position();
                    }
                }
                tmpSize += 128;
            }
        }

        protected boolean isUnmapped(char c) {
            return Character.isSurrogate(c);
        }

        protected char toUnmappedChar(char c) {
            return '?';
        }

        @Override
        public void close() throws IOException {
            writeBuffer(0);
            ByteBuffer buffer = buffer();
            if (buffer.hasRemaining()) {
                int pos = buffer.position();
                singleBuffer.position(1);
                encoder.encode(singleBuffer, buffer, true);
                encoder.flush(buffer);
                writeBuffer(pos  - buffer.position());
            }
        }

        protected abstract ByteBuffer buffer();
        protected void writeSourceEnd(int lastEncodeLen) throws IOException {
            writeBuffer(lastEncodeLen);
        }
        protected void writeBuffer(int lastEncodeLen) throws IOException {
        }
        protected void writeNext(int lastEncodeLen) throws IOException {
            writeBuffer(lastEncodeLen);
        }
    }

    class ByteChannelOutput extends ByteOutput {
        protected WritableByteChannel channel;
        protected ByteBuffer buffer;

        public ByteChannelOutput(WritableByteChannel channel, Charset charset, ByteBuffer buffer) {
            super(charset);
            this.channel = channel;
            this.buffer = buffer;
        }

        @Override
        public void write(char c) throws IOException {
            super.write(c);
            if (c == '\n' && (buffer.position() / (float) buffer.capacity()) > BUFFER_LEAST_WRITE_RATE) {
                writeBuffer(0);
            }
        }

        @Override
        public void close() throws IOException {
            super.close();
            channel.close();
        }

        @Override
        protected ByteBuffer buffer() {
            return buffer;
        }

        @Override
        protected void writeBuffer(int lastEncodeLen) throws IOException {
            buffer.flip();
            channel.write(buffer);
            buffer.clear();
        }
    }

    class ByteBufferArrayOutput extends ByteOutput {
        protected ByteBufferArray array;

        public ByteBufferArrayOutput(ByteBufferArray array, Charset charset) {
            super(charset);
            this.array = array;
        }

        @Override
        protected ByteBuffer buffer() {
            return array.getBuffer(array.position());
        }

        @Override
        protected void writeSourceEnd(int lastEncodeLen) {
            writeBuffer(lastEncodeLen);
        }

        @Override
        protected void writeBuffer(int lastEncodeLen) {
            array.position(array.position() + lastEncodeLen);
        }

        @Override
        protected void writeNext(int lastEncodeLen)  {
            writeBuffer(lastEncodeLen);
        }

        @Override
        public void close() throws IOException {
            super.close();
            array.close();
        }
    }
}
