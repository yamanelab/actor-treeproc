package csl.dataproc.csv.impl;

import csl.dataproc.csv.CsvReader;
import csl.dataproc.tgls.util.ByteBufferArray;

import java.io.Closeable;
import java.io.Reader;
import java.io.StringReader;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;

public interface CsvCharIterator extends Closeable {
    void setIndex(long index);
    long getIndex();
    boolean hasNext();
    char next();
    void close();
    /** @return true if {@link #getIndex()} returns the number of bytes, or false if it is the number of chars. */
    boolean indexIsBytes();

    /**
     * if last {@link #next()} returns a component of a surrogate pair ({@link Character#isSurrogate(char)}),
     *   another component of the pair must be obtained by the method.
     * @return next component of a surrogate pair
     */
    default char nextSurrogate() {
        return next();
    }

    static CsvBufferCharIterator get(ByteBufferArray array, Charset charset) {
        return new CsvBufferCharIterator(array, charset);
    }

    static ReaderCharIterator get(String data) {
        return get(new StringReader(data));
    }

    static ReaderCharIterator get(Path file) {
        try {
            return get(Files.newBufferedReader(file));
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    static ReaderCharIterator get(Path file, Charset charset) {
        try {
            return get(Files.newBufferedReader(file, charset));
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    /** the returned iterator support only once iteration */
    static ReaderCharIterator get(Reader reader) {
        return new ReaderCharIterator(reader);
    }

    /**
     * The class does not support free setIndex(i) calls.
     *  It does not allow changing the current index.
     *   Thus, it can be used only for 1 iteration of
     *    {@link CsvReader#iterator()}, {@link CsvReader#readNext()}, {@link CsvReader#scanNext()} or
     *     {@link CsvReader#stream()}.
     *
     *  <p>
     *   In this class, {@link #getIndex()} returns characters instead of byte offset
     */
    class ReaderCharIterator implements CsvCharIterator {
        protected long index;
        protected Reader reader;

        protected static final int UNREAD = -2;
        protected int nextChar = UNREAD;

        public ReaderCharIterator(Reader reader) {
            this.reader = reader;
        }

        @Override
        public long getIndex() {
            return index;
        }

        @Override
        public void setIndex(long index) {
            if (this.index != index) {
                error(new RuntimeException("invalid index: " + this.index + " <- " + index));
            }
            this.index = index;
        }

        @Override
        public boolean hasNext() {
            if (nextChar == UNREAD && reader != null) {
                try {
                    nextChar = reader.read();
                    if (nextChar == -1) {
                        close();
                    }
                } catch (Exception ex) {
                    error(ex);
                }
            }
            return nextChar != -1 && nextChar != UNREAD;
        }

        @Override
        public char next() {
            if (hasNext()) {
                ++index;
            }
            char c = (char) nextChar;
            nextChar = UNREAD;
            return c;
        }

        public void error(Exception ex) {
            throw new RuntimeException(ex);
        }

        @Override
        public void close() {
            try {
                if (reader != null) {
                    reader.close();
                    reader = null;
                }
            } catch (Exception ex) {
                error(ex);
            }
        }

        @Override
        public boolean indexIsBytes() {
            return false;
        }
    }
}
