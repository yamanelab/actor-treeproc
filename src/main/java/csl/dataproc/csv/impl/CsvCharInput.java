package csl.dataproc.csv.impl;

import csl.dataproc.csv.CsvReader;

import java.nio.CharBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/** callbacks for {@link CsvReader#processLine(CsvCharInput)} */
public interface CsvCharInput {
    void nextColumn();
    void startEscapeColumn();
    void endLine();
    void nextChar(char c);

    class MaxColumnWidthCharInput implements CsvCharInput {
        protected int columnWidth = 0;
        protected int maxColumnWidth = 0;

        @Override
        public void nextColumn() {
            maxColumnWidth = Math.max(maxColumnWidth, columnWidth);
            columnWidth = 0;
        }

        @Override
        public void startEscapeColumn() {
        }

        @Override
        public void endLine() {
            nextColumn();
        }

        @Override
        public void nextChar(char c) {
            ++columnWidth;
        }
        public int getMaxColumnWidth() {
            return maxColumnWidth;
        }

    }

    class ColumnsListCharInput implements CsvCharInput {
        protected ArrayList<String> result;
        protected CharBuffer columnBuffer;
        protected List<String> defaultData;
        protected boolean escape = false;
        protected boolean trimNonQuoted = true;
        protected boolean emptyLast;
        protected boolean emptyLine;

        public ColumnsListCharInput(int columnCapacity, int maxColumns, boolean trimNonQuoted) {
            this(columnCapacity, maxColumns, Collections.emptyList(), trimNonQuoted);
        }

        public ColumnsListCharInput(int columnCapacity, int maxColumns, List<String> defaultData, boolean trimNonQuoted) {
            result = new ArrayList<>(Math.max(maxColumns, defaultData.size()));
            this.defaultData = defaultData;
            columnBuffer = CharBuffer.allocate(Math.max(columnCapacity, 16));
            emptyLast = true;
            emptyLine = true;
        }

        public void setTrimNonQuoted(boolean trimNonQuoted) {
            this.trimNonQuoted = trimNonQuoted;
        }

        public boolean isTrimNonQuoted() {
            return trimNonQuoted;
        }

        @Override
        public void nextColumn() {
            int columnIndex = result.size();
            columnBuffer.flip();
            if (!escape && trimNonQuoted) {
                trim();
            }
            String col = columnBuffer.toString();
            boolean empty = col.isEmpty();
            emptyLast = !escape && empty;
            if (empty && columnIndex < defaultData.size()) {
                col = defaultData.get(columnIndex);
            }
            result.add(col);
            columnBuffer.clear();
            escape = false;
        }

        private void trim() {
            while (columnBuffer.hasRemaining()) {
                if (!Character.isWhitespace(columnBuffer.get())) {
                    columnBuffer.position(columnBuffer.position() - 1);
                    break;
                }
            }
            for (int pos = columnBuffer.limit() - 1, startPos = columnBuffer.position();
                 pos >= startPos; --pos) {
                if (!Character.isWhitespace(columnBuffer.get(pos))) {
                    columnBuffer.limit(pos + 1);
                    break;
                }
            }
        }

        @Override
        public void startEscapeColumn() {
            escape = true;
        }

        @Override
        public void endLine() {
            nextColumn();
            int cols = result.size();
            emptyLine = (cols == 0 || cols == 1 && emptyLast);
            endLineExtendsWithDefaultData();
        }

        public void endLineExtendsWithDefaultData() {
            for (int i = result.size(), l = defaultData.size(); i < l; ++i) {
                result.add(defaultData.get(i));
            }
            result.trimToSize();
        }

        @Override
        public void nextChar(char c) {
            if (!columnBuffer.hasRemaining()) {
                CharBuffer newBuffer = CharBuffer.allocate(columnBuffer.capacity() * 2);
                columnBuffer.flip();
                newBuffer.put(columnBuffer);
                this.columnBuffer = newBuffer;
            }
            columnBuffer.append(c);
        }

        public ArrayList<String> getResult() {
            return result;
        }

        public boolean isEmptyLast() {
            return emptyLast;
        }

        public boolean isEmptyLine() {
            return emptyLine;
        }
    }

    class MaxColumnWidthAndColumnsListCharInput implements CsvCharInput {
        protected MaxColumnWidthCharInput maxColumnWidthProcessor;
        protected ColumnsListCharInput columnsListProcessor;

        public MaxColumnWidthAndColumnsListCharInput(int columnCapacity, int maxColumn, List<String> defaultData, boolean trimNonQuoted) {
            maxColumnWidthProcessor = new MaxColumnWidthCharInput();
            columnsListProcessor = new ColumnsListCharInput(columnCapacity, maxColumn, defaultData, trimNonQuoted);
        }

        @Override
        public void nextColumn() {
            maxColumnWidthProcessor.nextColumn();
            columnsListProcessor.nextColumn();
        }

        @Override
        public void startEscapeColumn() {
            maxColumnWidthProcessor.startEscapeColumn();
            columnsListProcessor.startEscapeColumn();
        }

        @Override
        public void endLine() {
            maxColumnWidthProcessor.endLine();
            columnsListProcessor.endLine();
        }

        @Override
        public void nextChar(char c) {
            maxColumnWidthProcessor.nextChar(c);
            columnsListProcessor.nextChar(c);
        }
        public int getMaxColumnWidth() {
            return maxColumnWidthProcessor.getMaxColumnWidth();
        }

        public ArrayList<String> getResult() {
            return columnsListProcessor.getResult();
        }

        public boolean isEmptyLine() {
            return columnsListProcessor.isEmptyLine();
        }
    }
}
