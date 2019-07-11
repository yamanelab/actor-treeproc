package csl.dataproc.csv;

import java.io.Closeable;
import java.util.List;
import java.util.stream.Stream;

/** shared interface for CsvReader and ArffReader */
public interface CsvReaderI<Line extends CsvLine>
        extends Iterable<Line>, Closeable {
    long[] scanNext();
    Line readNext(long index);
    Line readNext();
    List<String> readColumns(long from);
    Iterable<long[]> ranges();
    Stream<Line> stream();
}
