package csl.dataproc.csv.impl;

import csl.dataproc.tgls.util.ByteBufferArray;

import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CoderResult;

public class CsvBufferCharIterator implements CsvCharIterator {
    protected ByteBufferArray array;
    protected Charset charset;
    protected CharsetDecoder decoder;

    protected long index;
    protected ByteBuffer buffer;

    protected CharBuffer charBuffer;
    protected CharBuffer wideBuffer;

    public CsvBufferCharIterator(ByteBufferArray array, Charset charset) {
        this.array = array;
        this.charset = charset;
        charBuffer = CharBuffer.allocate(1);
        wideBuffer = CharBuffer.allocate(2); //considering 32bit char
        buffer = array.getBuffer(0);
        index = 0;
        this.decoder = charset.newDecoder();
    }

    public ByteBufferArray getArray() {
        return array;
    }

    public Charset getCharset() {
        return charset;
    }

    @Override
    public void setIndex(long index) {
        if (this.index != index) {
            decoder = decoder.reset();

            long diff = index - this.index;

            if (buffer != null &&
                    ((0 < diff && diff < buffer.remaining()) ||
                     (diff < 0 && -diff < buffer.position()))) {
                buffer.position(buffer.position() + (int) diff);
            } else {
                buffer = array.getBuffer(index);
            }

            this.index = index;
        }
    }

    @Override
    public long getIndex() {
        return index;
    }

    @Override
    public boolean hasNext() {
        return (buffer != null && buffer.hasRemaining()) ||
                index < array.limit();
    }

    @Override
    public char next() {
        charBuffer.clear();
        int prePos = buffer.position();
        CoderResult res = decoder.decode(buffer, charBuffer, false);
        if (res.isMalformed()) {
            return (char) (0xFF & buffer.get()); //force to proceed
        } else if (charBuffer.position() == 0) {
            wideBuffer.clear();
            decoder.decode(buffer, wideBuffer, false);
            wideBuffer.flip();
            if (!wideBuffer.hasRemaining()) {
                wideBuffer.clear();
                if (nextBuffer(wideBuffer)) {
                    return charBuffer.get(0);
                } else {
                    return wideBuffer.get(0);
                }
            } else {
                int readBytes = buffer.position() - prePos;
                index += readBytes;
                return wideBuffer.get(0);
            }
        } else {
            int readBytes = buffer.position() - prePos;

            if (readBytes == 0) {
                charBuffer.clear();
                nextBuffer(charBuffer);
            } else {
                index += readBytes;
            }
            return charBuffer.get(0);
        }
    }

    @Override
    public char nextSurrogate() {
        return wideBuffer.get(1);
    }

    private boolean nextBuffer(CharBuffer charBuffer) {
        boolean retryAsOneChar = false;
        if (buffer.hasRemaining()) {
            int prevFragmentLen = Math.min(128, buffer.remaining());
            ByteBuffer fragment = ByteBuffer.allocate(prevFragmentLen + 128);

            for (int i = 0; i < prevFragmentLen; ++i) { //suppose remaining is too large
                fragment.put(buffer.get());
            }

            index += prevFragmentLen;
            buffer = array.getBuffer(index);
            if (buffer != null && buffer.hasRemaining()) {
                ByteBuffer nextBufferSlice = buffer.slice();
                int nextLimit = Math.min(Math.min(
                        nextBufferSlice.position() + fragment.remaining(),
                        buffer.limit()), nextBufferSlice.capacity());
                if (nextLimit > 0) {
                    nextBufferSlice.limit(nextLimit);
                    fragment.put(nextBufferSlice);
                }
            }
            fragment.flip();

            decoder.decode(fragment, charBuffer, false);
            charBuffer.flip();
            int readBytes = fragment.position() - prevFragmentLen;

            if (charBuffer.hasRemaining() && //after flip(): read something
                    !Character.isSurrogate(charBuffer.get(0)) && //no surrogate
                    charBuffer.limit() > 1) { //wideBuffer
                //retry as 1 char
                fragment.position(0);
                this.charBuffer.clear();
                CoderResult res = decoder.decode(fragment, this.charBuffer, false);
                readBytes = fragment.position() - prevFragmentLen;
                retryAsOneChar = true;
                if (this.charBuffer.position() == 0) {
                    System.err.println(this + " : failed to read (retry) buffer=" + buffer + " fragment=" + fragment);
                }
            } else {
                if (charBuffer.position() == 0) {
                    System.err.println(this + " : failed to read buffer=" + buffer + " fragment=" + fragment);
                }
            }

            index += readBytes;
            if (readBytes < 0) {
                buffer = array.getBuffer(index);
            } else {
                buffer.position(readBytes);
            }
        } else {
            buffer = array.getBuffer(index); //suppose the buffer has sufficient remaining-size for reading 1 or 2 chars
            if (buffer != null) {
                int prePos = buffer.position();
                decoder.decode(buffer, charBuffer, false);
                charBuffer.flip();
                int readBytes = buffer.position() - prePos;

                if (charBuffer.hasRemaining() && //after flip(): read something
                        !Character.isSurrogate(charBuffer.get(0)) && //no surrogate
                        charBuffer.limit() > 1) { //wideBuffer
                    //retry as 1 char
                    this.buffer.position(prePos);
                    this.charBuffer.clear();
                    CoderResult res = decoder.decode(buffer, this.charBuffer, false);
                    readBytes = buffer.position() - prePos;
                    index += readBytes;
                    retryAsOneChar = true;
                } else {
                    index += readBytes;
                }
                if (readBytes == 0) {
                    System.err.println(this + " : failed to read: " + index + " : " + buffer);
                }
            } else {
                System.err.println(this + " : no buffer for " + index);
            }
        }
        return retryAsOneChar;
    }

    @Override
    public void close() {
        array.close();
    }

    @Override
    public boolean indexIsBytes() {
        return true;
    }
}
