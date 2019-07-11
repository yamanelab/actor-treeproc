package csl.dataproc.csv;

import csl.dataproc.tgls.util.ByteBufferArray;

import java.io.BufferedOutputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.LongStream;
import java.util.stream.Stream;

public class CsvPreProcess {
    public static void main(String[] args) {
        new CsvPreProcess().runMain(args);
    }

    protected List<ColumnOperation> columnOperations = new ArrayList<>();
    protected List<DataColumn> columns = new ArrayList<>();
    protected List<IndexSelection> columnSelections = new ArrayList<>();

    protected List<String> inputs = new ArrayList<>();
    protected String output;

    protected boolean twoIteration;

    protected CsvProcessor writeProcessor;
    protected OutputTarget outputTarget;
    protected OutputTargetShuffle outputShuffle;
    protected OutputLimitList outputLimit = new OutputLimitList();
    protected OutputSplitList outputSplit = new OutputSplitList();
    protected int lastInputColumnSize;
    protected int outputColumnSize;

    protected Logger logger = new Logger();

    public void runMain(String[] args) {
        List<String> rest = parseArgs(Arrays.asList(args));
        if (rest.contains("--help")) {
            showHelp();
            return;
        }
        if (!rest.isEmpty()) {
            logger.log("remaining args: %s", rest);
        }
        init();
        run();
    }


    public static class Logger {
        protected Instant startTime = Instant.now();
        protected Instant prevTime = startTime;
        static DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

        public void log(String fmt, Object... args) {
            Instant now = Instant.now();
            System.err.println(String.format("[%s: %s]: %s",
                    formatter.format(OffsetDateTime.ofInstant(now, ZoneId.systemDefault())),
                    Duration.between(startTime, now), String.format(fmt, args)));
            prevTime = now;
        }

        public boolean isLogPeriod() {
            Instant now = Instant.now();
            return Duration.between(prevTime, now).compareTo(Duration.ofSeconds(10)) >= 0;
        }
    }

    public List<String> parseArgs(List<String> args) {
        List<String> rest = new ArrayList<>();
        for (int i = 0; i < args.size(); i++) {
            String arg = args.get(i);
            if (arg.equals("--normalize")) { //--normalize N1,...,N2 or N1-N2
                ++i;
                addColumnOperationNormalize(args.get(i));
            } else if (arg.equals("--log")) { //--log N1,...,N2 or N1-N2
                ++i;
                addColumnOperationLog(args.get(i));
            } else if (arg.equals("--clip")) { //--clip MIN MAX N1,...,N2
                ++i;
                Number min = parseNumber(args.get(i));
                ++i;
                Number max = parseNumber(args.get(i));
                addColumnOperationClip(min, max, args.get(i));
            } else if (arg.equals("--hash")) { //--hash N1,...,N2
                ++i;
                addColumnOperationHash(args.get(i));
            } else if (arg.equals("--select")) { //--select N1,..,N2 or N1-N2
                ++i;
                addColumnSelection(args.get(i));
            } else if (arg.equals("--row-skip")) { //--row-skip N
                ++i;
                addOutputLimitSkip(parseNumber(args.get(i)).longValue());

            } else if (arg.equals("--row-select")) {//--row-select N1,...,N2 or N1-N2
                ++i;
                addOutputLimitSelect(args.get(i));

            } else if (arg.equals("--row-max")) { //--row-max Ng|Nm|Nk|N
                ++i;
                addOutputLimit(args.get(i));
            } else if (arg.equals("--row-shuffle")) { //--row-shuffle seed
                ++i;
                setOutputShuffleOnMemory(parseSeed(args.get(i)));

            } else if (arg.equals("--row-shuffle-file")) {//--row-shuffle-file seed tmpFile
                ++i;
                long seed = parseSeed(args.get(i));
                ++i;
                setOutputShuffleFile(seed, args.get(i));

            } else if (arg.equals("--split")) { //--split Ng|Nm|Nk|N
                ++i;
                addOutputSplit(args.get(i));
            } else if (arg.equals("--output")) { //--output path
                ++i;
                setOutput(args.get(i));
            } else if (arg.equals("--input")) { //--input path
                ++i;
                addInput(args.get(i));
            } else if (arg.equals("--help") || arg.equals("-h")) {
                rest.add("--help");
                break;
            } else {
                rest.add(arg);
            }
        }
        return rest;
    }

    public String getHelp() {
        return String.join("\n",
                "",
                "--normalize    <range> : normalize columns with 0..1. 2-iterations",
                "--log          <range> : convert columns by Math.log10 ",
                "--clip         <N1> <N2> <range> : clip columns by range N1..N2",
                "--hash         <range> : convert columns to hashCode() ",
                "--select       <range> : pick out columns",
                "--row-skip     <N>     : skip first N rows",
                "--row-select   <range> : pick out rows",
                "--row-max      <size>  : limit total output size ",
                "--row-shuffle  <seed>  : shuffle on memory",
                "--row-shuffle-file <seed> <tmpFilePath> : shuffle with using tmpFilePath",
                "--split        <size>  : split output by size. each outputs has the name path-%04d.suff",
                "--output       <path>  : output file path",
                "--input        <path>  : input file or directory",
                "",
                "<range> : <N1>,<N2>,...,<Nx>  : enumeration of indices",
                "          <Ns>-<Ne>           : meaning Ns,Ns+1,Ns+2,...,Ne-1. Ne is exclusive",
                "<size>  : <N>g|<N>m|<N>k|<N>b : N bytes * 10^9(g), 10^6(m), 10^3(k) or 1(b).",
                "          <N>                 : N rows or N columns",
                "                                those numbers N can have _ for readability",
                "<seed>  : an integer or a string which is converted to hashCode()");
    }

    public void addColumnOperationNormalize(String range) {
        columnOperations.add(
                new ColumnOperationNormalize(parseIndexSelection(range)));
    }

    public void addColumnOperationLog(String range) {
        columnOperations.add(
                new ColumnOperationLog(parseIndexSelection(range)));
    }

    public void addColumnOperationClip(Number min, Number max,String range) {
        columnOperations.add(
                new ColumnOperationClip(min, max, parseIndexSelection(range)));
    }

    public void addColumnOperationHash(String range) {
        columnOperations.add(
                new ColumnOperationHash(parseIndexSelection(range)));
    }

    public void addColumnSelection(String range) {
        columnSelections.add(parseIndexSelection(range));
    }

    public void addOutputLimitSkip(long num) {
        outputLimit.add(new OutputLimitSkip(new IndexSelectionRange(0, num)));
    }

    public void addOutputLimitSelect(String range) {
        outputLimit.add(new OutputLimitSelect(parseIndexSelection(range)));
    }

    public void addOutputLimit(String arg) {
        Number size = parseSize(arg);
        if (size != null) {
            outputLimit.add(new OutputLimitMaxBytes(size.longValue()));
        } else {
            outputLimit.add(new OutputLimitMaxRows(parseNumber(arg).longValue()));
        }
    }

    public void setOutputShuffleOnMemory(long seed) {
        outputShuffle = new OutputTargetShuffleOnMemory(logger, seed);
    }

    public void setOutputShuffleFile(long seed, String path) {
        outputShuffle = new OutputTargetShuffleWithFile(logger, seed, path);
    }

    public void addOutputSplit(String arg) {
        Number size = parseSize(arg);
        if (size != null) {
            outputSplit.add(new OutputSplitBytes(size.longValue()));
        } else {
            outputSplit.add(new OutputSplitRows(parseNumber(arg).longValue()));
        }
    }

    public void setOutput(String output) {
        this.output = output;
    }

    public void addInput(String path) {
        inputs.add(path);
    }

    public List<ColumnOperation> getColumnOperations() {
        return columnOperations;
    }

    public void setColumnOperations(List<ColumnOperation> columnOperations) {
        this.columnOperations = columnOperations;
    }

    public List<DataColumn> getColumns() {
        return columns;
    }

    public void setColumns(List<DataColumn> columns) {
        this.columns = columns;
    }

    public List<IndexSelection> getColumnSelections() {
        return columnSelections;
    }

    public void setColumnSelections(List<IndexSelection> columnSelections) {
        this.columnSelections = columnSelections;
    }

    public List<String> getInputs() {
        return inputs;
    }

    public void setInputs(List<String> inputs) {
        this.inputs = inputs;
    }

    public String getOutput() {
        return output;
    }

    public OutputTarget getOutputTarget() {
        return outputTarget;
    }

    public void setOutputTarget(OutputTarget outputTarget) {
        this.outputTarget = outputTarget;
    }

    public OutputTargetShuffle getOutputShuffle() {
        return outputShuffle;
    }

    public void setOutputShuffle(OutputTargetShuffle outputShuffle) {
        this.outputShuffle = outputShuffle;
    }

    public OutputLimitList getOutputLimit() {
        return outputLimit;
    }

    public void setOutputLimit(OutputLimitList outputLimit) {
        this.outputLimit = outputLimit;
    }

    public OutputSplitList getOutputSplit() {
        return outputSplit;
    }

    public void setOutputSplit(OutputSplitList outputSplit) {
        this.outputSplit = outputSplit;
    }


    public void showHelp() {
        System.out.println(getHelp());
    }

    public IndexSelection parseIndexSelection(String arg) {
        if (arg.contains("-")) {
            String[] r = arg.split("-");
            return new IndexSelectionRange(Long.valueOf(r[0]), Long.valueOf(r[1]));
        } else {
            return new IndexSelectionList(Arrays.stream(arg.split(","))
                    .map(Long::valueOf)
                    .collect(Collectors.toList()));
        }
    }

    public long parseSeed(String arg) {
        try {
            return parseNumber(arg).longValue();
        } catch (Exception ex) {
            return arg.hashCode();
        }
    }

    public Number parseNumber(String arg) {
        arg = arg.replaceAll("_", ""); //123_456_789 -> 123456789
        if (arg.contains(".")) {
            return Double.valueOf(arg);
        } else {
            return Long.valueOf(arg);
        }
    }

    public Number parseSize(String arg) {
        long n = 0;
        if (arg.endsWith("g")) {
            n = 1000_000_000L;
        } else if (arg.endsWith("m")) {
            n = 1000_000L;
        } else if (arg.endsWith("k")) {
            n = 1000L;
        } else if (arg.equals("b")) {
            n = 1L;
        }
        if (n > 0) {
            arg = arg.substring(0, arg.length() - 1).replaceAll("_", "");
            return Double.valueOf(arg) * n;
        } else {
            return null;
        }
    }

    public void init() {
        if (outputTarget == null) {
            if (outputSplit.isEmpty()) {
                outputTarget = new OutputTargetFile(logger, output);
            } else {
                outputTarget = new OutputTargetSplit(logger, output, outputSplit);
            }
        }
        outputTarget.setLimit(outputLimit);
        if (outputShuffle != null) {
            outputShuffle.setTarget(outputTarget);
            outputTarget = outputShuffle;
        }

        columnOperations.forEach(op ->
                        op.getTarget().stream()
                                .forEach(i ->
                                        getColumn((int) i)
                                                .addOperation(op)));
        twoIteration = columnOperations.stream()
                .anyMatch(ColumnOperation::hasSecondIteration);
        writeProcessor = (twoIteration ? new CsvProcessorSecond() : new CsvProcessorFirst());

        outputColumnSize = (int) columnSelections.stream()
                .mapToLong(s -> s.stream().count()).sum();

        logger.log("columnOperations: %s", columnOperations);
        logger.log("outputTarget: %s", outputTarget);
        logger.log("columns: %,d %s", columns.size(), columns);
        logger.log("outputColumnSize: %,d", outputColumnSize);
    }

    public DataColumn getColumn(int i) {
        while (i >= columns.size()) {
            columns.add(new DataColumn(columns.size()));
        }
        return columns.get(i);
    }

    ////////////////////////////

    public void run() {
        if (twoIteration) {
            CsvFileProcessorTwoIterationFirst first = new CsvFileProcessorTwoIterationFirst();
            run(first);
            logger.log("run: 2nd-iter %,d files", first.getPaths().size());
            for (Path p : first.getPaths()) {
                if (!runTwoIterationSecond(p)) {
                    break;
                }
            }
        } else {
            run(new CsvFileProcessorSingleIteration());
        }
        runAfter();
        logger.log("finish %,d [%s]", columns.size(),
                columns.stream()
                .map(DataColumn::getAfterString)
                        .collect(Collectors.joining(", ")));
    }

    public void run(CsvFileProcessor fp) {
        for (String path : inputs) {
            Path p = Paths.get(path);
            logger.log("run: %s %s", fp, p);
            if (Files.isDirectory(p)) {
                if (!runDir(p, fp)) {
                    return;
                }
            } else {
                if (!fp.run(p)) {
                    return;
                }
            }
        }
    }

    public boolean runDir(Path p, CsvFileProcessor fp) {
        try (Stream<Path> files = Files.list(p)) {
            List<Path> ps = files.collect(Collectors.toList());
            for (Path sp : ps) {
                if (Files.isDirectory(sp)) {
                    if (!runDir(sp, fp)) {
                        return false;
                    }
                } else if (includeInput(sp)) {
                    if (!fp.run(sp)) {
                        return false;
                    }
                }
            }
            return true;
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    public boolean includeInput(Path f) {
        String n = f.getFileName().toString();
        return (n.endsWith(".txt") || n.endsWith(".csv")) && !n.startsWith(".");
    }

    ////////////////////////////


    public boolean runSingleIteration(Path file) {
        try (CsvReader reader = CsvReader.get(file)) {
            String name = file.toString();
            for (CsvLine line : reader) {
                if (line.isEnd() || line.isEmptyLine()) {
                    continue;
                }
                if (!runOutputNext(line, name, writeProcessor)) {
                    return false;
                }
            }
        }
        return true;
    }

    public boolean runOutputNext(CsvLine line, String file, CsvProcessor p) {
        return outputTarget.write(line, file, p);
    }

    public void runTwoIterationFirst(Path file) {
        try (CsvReader reader = CsvReader.get(file)) {
            long lines = 0;
            for (CsvLine line : reader) {
                if (line.isEnd() || line.isEmptyLine()) {
                    continue;
                }
                if (logger.isLogPeriod()) {
                    logger.log("1st-iter line=%,d", lines);
                }
                run(line, true);
                ++lines;
            }
        }
    }

    public boolean runTwoIterationSecond(Path file) {
        try (CsvReader reader = CsvReader.get(file)) {
            String name = file.toString();
            for (CsvLine line : reader) {
                if (line.isEnd() || line.isEmptyLine()) {
                    continue;
                }
                if (!runOutputNext(line, name, writeProcessor)) {
                    return false;
                }
            }
        }
        return true;
    }

    public void run(CsvLine line, boolean first) {
        int size = line.getData().size();
        lastInputColumnSize = size;

        for (int i = 0; i < size; ++i) {
            DataColumn col = getColumn(i);
            col.apply(first, line);
        }
        for (int i = size, l = columns.size(); i < l; ++i) {
            getColumn(i).clearValue();
        }
    }

    public List<String> runOutputData() {
        if (columnSelections.isEmpty()) {
            List<String> data = new ArrayList<>(lastInputColumnSize);
            IntStream.range(0, lastInputColumnSize)
                    .forEach(c -> data.add(getColumn((int) c ).getDataString()));
            return data;
        } else {
            List<String> data = new ArrayList<>(outputColumnSize);
            columnSelections.forEach(s -> s.stream()
                    .forEach(c -> data.add(getColumn((int) c).getDataString())));
            return data;
        }
    }

    public void runAfter() {
        outputTarget.writeAfter(writeProcessor);
    }

    public interface CsvFileProcessor {
        boolean run(Path path);
    }

    public class CsvFileProcessorSingleIteration implements CsvFileProcessor {
        @Override
        public boolean run(Path path) {
            return runSingleIteration(path);
        }

        @Override
        public String toString() {
            return "single-iter";
        }
    }

    public class CsvFileProcessorTwoIterationFirst implements CsvFileProcessor {
        List<Path> paths = new ArrayList<>();

        public List<Path> getPaths() {
            return paths;
        }

        @Override
        public boolean run(Path path) {
            paths.add(path);
            runTwoIterationFirst(path);
            return true;
        }

        @Override
        public String toString() {
            return "1st-iter";
        }
    }


    ///////////////////

    public static class DataColumn {
        protected int index;
        protected DataColumnType inputType = DataColumnType.None;

        protected Object value;
        protected Object min;
        protected Object max;

        public long error;

        protected List<ColumnOperation> operations;

        public DataColumn(int index) {
            this.index = index;
        }

        public DataColumnType getInputType() {
            return inputType;
        }

        public void addOperation(ColumnOperation op) {
            if (operations == null) {
                operations = new ArrayList<>();
            }
            operations.add(op);
            setInputTypeIfNotSet(op.getInputType(index));
        }

        public void setInputTypeIfNotSet(DataColumnType inputType) {
            if (this.inputType.equals(DataColumnType.None)) {
                this.inputType = inputType;
            }
        }

        public void clearValue() {
            value = null;
        }

        public void set(CsvLine line) {
            String data = line.get(index);
            if (inputType.equals(DataColumnType.None) || inputType.equals(DataColumnType.String)) {
                value = data;
            } else if (inputType.equals(DataColumnType.Integer)) {
                try {
                    value = Long.valueOf(data);
                } catch (NumberFormatException e) {
                    try {
                        value = Double.valueOf(data).longValue();
                    } catch (NumberFormatException ne) {
                        value = 0;
                        ++error;
                    }
                }
            } else if (inputType.equals(DataColumnType.Real)) {
                try {
                    value = Double.valueOf(data);
                } catch (NumberFormatException e) {
                    value = 0;
                    ++error;
                }
            }
        }

        public String getDataString() {
            if (value == null) {
                return "";
            } else {
                return value.toString();
            }
        }

        public String getAfterString() {
            return "(" + toString() +
                    (inputType.equals(DataColumnType.None) || inputType.equals(DataColumnType.String) ? "" : ",min=" + min + ",max=" + max)
                    + (error > 0 ? ",err=" + error : "") + ")";
        }

        public Number valueAsNumber() {
            return (Number) value;
        }

        @Override
        public String toString() {
            return index + ":" + inputType.toString().charAt(0);
        }

        public void apply(boolean first, CsvLine line) {
            set(line);
            if (value != null && operations != null) {
                Object v = value;
                for (ColumnOperation op : operations) {
                    v = (first ? op.applyColumnValue(v, this) : op.applyColumnValueSecond(v, this));
                }
                this.value = v;
            }
        }
    }

    public enum DataColumnType {
        String, Real, Integer, None
    }

    ///////////////////

    public interface ColumnOperation {
        IndexSelection getTarget();

        default boolean hasSecondIteration() {
            return false;
        }

        DataColumnType getInputType(long index);

        default void apply(List<DataColumn> columns) {
            getTarget().stream()
                    .forEach(i -> apply(i, columns));
        }

        default void apply(long i, List<DataColumn> columns) {
            if (i < columns.size()) {
                applyColumn(i, columns.get((int) i));
            }
        }

        default void applyColumn(long i, DataColumn column) {}

        default void applySecond(List<DataColumn> columns) {
            apply(columns);
        }

        default Object applyColumnValue(Object value, DataColumn column) { return value; }

        default Object applyColumnValueSecond(Object value, DataColumn column) {
            return applyColumnValue(value, column);
        }
    }

    public static class ColumnOperationLog implements ColumnOperation {
        protected IndexSelection target;

        public ColumnOperationLog(IndexSelection target) {
            this.target = target;
        }

        @Override
        public IndexSelection getTarget() {
            return target;
        }

        @Override
        public DataColumnType getInputType(long index) {
            return DataColumnType.Real;
        }

        @Override
        public String toString() {
            return "--log " + target;
        }

        @Override
        public void applyColumn(long i, DataColumn column) {
            Number v = column.valueAsNumber();
            if (v != null) {
                column.value = apply(v);
            }
        }

        private double apply(Number v) {
            return Math.log10(v.doubleValue());
        }

        @Override
        public Object applyColumnValue(Object value, DataColumn column) {
            if (value instanceof Number) {
                return apply((Number) value);
            } else {
                return value;
            }
        }
    }

    public static class ColumnOperationClip implements ColumnOperation {
        protected IndexSelection target;
        protected Number min;
        protected Number max;

        public ColumnOperationClip(Number min, Number max, IndexSelection target) {
            this.target = target;
            this.min = min;
            this.max = max;
            if (min.doubleValue() > max.doubleValue()) {
                Number tmp = min;
                this.min = max;
                this.max = tmp;
            }
        }

        @Override
        public IndexSelection getTarget() {
            return target;
        }

        @Override
        public DataColumnType getInputType(long index) {
            return DataColumnType.Real;
        }

        @Override
        public String toString() {
            return "--clip " + min + " " + max + " " + target;
        }

        @Override
        public void applyColumn(long i, DataColumn column) {
            Number nv = column.valueAsNumber();
            if (nv != null) {
                double v = nv.doubleValue();
                if (v < min.doubleValue()) {
                    column.value = min;
                } else if (v > max.doubleValue()) {
                    column.value = max;
                }
            }
        }

        @Override
        public Object applyColumnValue(Object value, DataColumn column) {
            if (value instanceof Number) {
                double v = ((Number) value).doubleValue();
                if (v < min.doubleValue()) {
                    return min;
                } else if (v > max.doubleValue()) {
                    return max;
                } else {
                    return value;
                }
            } else {
                return value;
            }
        }
    }

    public static class ColumnOperationNormalize implements ColumnOperation {
        protected IndexSelection target;

        public ColumnOperationNormalize(IndexSelection target) {
            this.target = target;
        }

        @Override
        public IndexSelection getTarget() {
            return target;
        }

        @Override
        public boolean hasSecondIteration() {
            return true;
        }

        @Override
        public DataColumnType getInputType(long index) {
            return DataColumnType.Real;
        }

        @Override
        public String toString() {
            return "--normalize " + target;
        }

        @Override
        public void applyColumn(long i, DataColumn column) {
            Number nv = column.valueAsNumber();
            if (nv != null) {
                Number min = (Number) column.min;
                Number max = (Number) column.max;
                double v = nv.doubleValue();
                if (min == null || v < min.doubleValue()) {
                    column.min = v;
                }
                if (max == null || v > max.doubleValue()) {
                    column.max = v;
                }
            }
        }

        @Override
        public Object applyColumnValue(Object value, DataColumn column) {
            if (value instanceof Number) {
                Number min = (Number) column.min;
                Number max = (Number) column.max;
                double v = ((Number) value).doubleValue();
                if (min == null || v < min.doubleValue()) {
                    column.min = v;
                }
                if (max == null || v > max.doubleValue()) {
                    column.max = v;
                }
                return value;
            } else {
                return value;
            }
        }

        @Override
        public void applySecond(List<DataColumn> columns) {
            getTarget().stream()
                    .forEach(i -> {
                        if (i < columns.size()) {
                            set(columns.get((int) i));
                        }
                    });
        }

        public void set(DataColumn column) {
            Number nv = column.valueAsNumber();
            if (nv != null) {
                Number min = (Number) column.min;
                Number max = (Number) column.max;
                double v = nv.doubleValue();
                if (min != null && max != null) {
                    double mn = min.doubleValue();
                    double mx = max.doubleValue();
                    column.value = (v - mn) / (mx - mn);
                }
            }
        }

        @Override
        public Object applyColumnValueSecond(Object value, DataColumn column) {
            if (value instanceof Number) {
                Number min = (Number) column.min;
                Number max = (Number) column.max;
                double v = ((Number) value).doubleValue();
                if (min != null && max != null) {
                    double mn = min.doubleValue();
                    double mx = max.doubleValue();
                    value = (v - mn) / (mx - mn);
                }
                return value;
            } else {
                return value;
            }
        }
    }

    public static class ColumnOperationHash implements ColumnOperation {
        protected IndexSelection target;

        public ColumnOperationHash(IndexSelection target) {
            this.target = target;
        }

        @Override
        public IndexSelection getTarget() {
            return target;
        }

        @Override
        public DataColumnType getInputType(long index) {
            return DataColumnType.String;
        }

        @Override
        public String toString() {
            return "--hash " + target;
        }

        @Override
        public void applyColumn(long i, DataColumn column) {
            if (column.value != null) {
                column.value = Objects.toString(column.value).hashCode();
            }
        }

        @Override
        public Object applyColumnValue(Object value, DataColumn column) {
            if (value != null) {
                return Objects.toString(value).hashCode();
            } else {
                return null;
            }
        }
    }

    ///////////////////

    public interface IndexSelection {
        LongStream stream();
        long getMax();
        boolean contains(long index);
    }

    public static class IndexSelectionList implements IndexSelection {
        protected List<Long> elements;
        protected long max;

        public IndexSelectionList(List<Long> elements) {
            this.elements = elements;
            max = stream()
                    .max().orElse(0);
        }

        @Override
        public long getMax() {
            return max;
        }

        @Override
        public boolean contains(long index) {
            return elements.contains(index);
        }

        @Override
        public LongStream stream() {
            return elements.stream()
                    .mapToLong(Long::longValue);
        }

        @Override
        public String toString() {
            return elements.stream()
                    .map(l -> Long.toString(l))
                    .collect(Collectors.joining(","));
        }
    }

    public static class IndexSelectionRange implements IndexSelection {
        protected long start;
        protected long endExclusive; //exclusive

        public IndexSelectionRange(long start, long endExclusive) {
            this.start = start;
            this.endExclusive = endExclusive;
        }

        @Override
        public long getMax() {
            return endExclusive - 1;
        }

        @Override
        public boolean contains(long index) {
            return start <= index && index < endExclusive;
        }

        @Override
        public LongStream stream() {
            return LongStream.range(start, endExclusive);
        }

        @Override
        public String toString() {
            return start + "-" + endExclusive;
        }
    }

    /////////////////

    public interface CsvProcessor {
        default List<String> getData(CsvLine sourceLine, String sourceFile) {
            next(sourceLine, sourceFile);
            return getData();
        }

        default void next(CsvLine sourceLine, String sourceFile) {}

        default List<String> getData() { return Collections.emptyList(); }
    }

    public class CsvProcessorFirst implements CsvProcessor {
        @Override
        public void next(CsvLine sourceLine, String sourceFile) {
            run(sourceLine, true);
        }

        @Override
        public List<String> getData() {
            return runOutputData();
        }
    }

    public class CsvProcessorSecond implements CsvProcessor {
        @Override
        public void next(CsvLine sourceLine, String sourceFile) {
            run(sourceLine, false);
        }

        @Override
        public List<String> getData() {
            return runOutputData();
        }
    }

    public interface OutputTarget {
        boolean write(CsvLine sourceLine, String sourceFile, CsvProcessor p);
        default void writeAfter(CsvProcessor p) {}

        boolean writeData(byte[] data);

        void setLimit(OutputLimit limit);
    }

    public static class OutputTargetBase implements OutputTarget {
        protected Charset charset = StandardCharsets.UTF_8;
        protected long totalRows;
        protected long totalSize;
        protected long fileRows;
        protected long fileSize;
        protected long excludedCount;
        protected Logger logger;

        protected OutputLimit limit;

        public OutputTargetBase(Logger logger) {
            this.logger = logger;
        }

        @Override
        public boolean write(CsvLine sourceLine, String sourceFile, CsvProcessor p) {
            List<String> data = p.getData(sourceLine, sourceFile);

            ByteBuffer buf = charset.encode(String.join(",", data) + "\n");
            int size = buf.remaining();

            if (limit == null || limit.isIncluded(totalRows, data, totalSize, size)) {
                writeData(buf, data);
                totalSize += size;
                fileSize += size;
                totalRows++;
                fileRows++;
            } else {
                excludedCount++;
            }

            if (logger.isLogPeriod()) {
                logger.log("write %s : totalSize=%,d, totalRows=%,d, fileSize=%,d, fileRows=%,d, excluded=%,d, file=%s", getOutputStatus(),
                        totalSize, totalRows, fileSize, fileRows, excludedCount, sourceFile);
            }

            return !(limit != null && limit.isOver(totalRows, data, totalSize, size));
        }

        public void writeData(ByteBuffer buf, List<String> data) {

        }

        @Override
        public boolean writeData(byte[] data) {
            int size = data.length;
            if (limit == null || limit.isIncluded(totalRows, null, totalSize, size)) {
                writeData(ByteBuffer.wrap(data), null);
                totalSize += size;
                fileSize += size;
                totalRows++;
                fileRows++;
            } else {
                excludedCount++;
            }

            if (logger.isLogPeriod()) {
                logger.log("write %s : totalSize=%,d, totalRows=%,d, fileSize=%,d, fileRows=%,d excluded=%,d,", getOutputStatus(),
                        totalSize, totalRows, fileSize, fileRows, excludedCount);
            }

            return !(limit != null && limit.isOver(totalRows, null, totalSize, size));
        }

        @Override
        public void writeAfter(CsvProcessor p) {
            logger.log("writeAfter %s : totalSize=%,d, totalRows=%,d, excluded=%,d", getOutputStatus(), totalSize, totalRows, excludedCount);
        }

        public String getOutputStatus() {
            return "";
        }

        public void setLimit(OutputLimit limit) {
            this.limit = limit;
        }

        @Override
        public String toString() {
            return (limit  == null ? "" : " " + limit);
        }
    }

    public static class OutputTargetFile extends OutputTargetBase {
        protected String file;
        protected ByteBufferArray output;

        public OutputTargetFile(Logger logger, String file) {
            super(logger);
            this.file = file;
        }

        @Override
        public void writeData(ByteBuffer buf, List<String> data) {
            if (output == null) {
                Path p = Paths.get(file);
                Path parent = p.getParent();
                if (parent != null && !Files.exists(parent)) {
                    logger.log("create directory: %s", parent);
                    try {
                        Files.createDirectories(parent);
                    } catch (Exception ex) {
                        throw new RuntimeException(ex);
                    }
                }
                logger.log("writeData: file=%s", p);
                output = ByteBufferArray.createForWrite(p);
            }
            output.put(buf);
        }

        @Override
        public void writeAfter(CsvProcessor p) {
            super.writeAfter(p);
            if (output != null) {
                output.close();
                output = null;
            }
        }

        @Override
        public String toString() {
            return "--output " + file +  super.toString();
        }
    }


    public interface OutputLimit {
        boolean isIncluded(long rows, List<String> data, long totalSize, long nextBytes);

        default boolean isOver(long rows, List<String> data, long totalSize, long nextBytes) {
            return false;
        }
    }

    public static class OutputLimitList implements OutputLimit {
        protected List<OutputLimit> list;

        public OutputLimitList() {
            this.list = new ArrayList<>();
        }

        public void add(OutputLimit l) {
            list.add(l);
        }

        @Override
        public boolean isIncluded(long rows, List<String> data, long totalSize, long nextBytes) {
            if (list.isEmpty()) {
                return true;
            }
            return list.stream()
                    .mapToInt(l -> l.isIncluded(rows, data, totalSize, nextBytes) ? 1 : 0)
                    .sum() > 0;
        }

        @Override
        public boolean isOver(long rows, List<String> data, long totalSize, long nextBytes) {
            if (list.isEmpty()) {
                return false;
            }
            return list.stream()
                    .mapToInt(l -> l.isOver(rows, data, totalSize, nextBytes) ? 1 : 0)
                    .sum() > 0;
        }

        @Override
        public String toString() {
            return list.stream()
                    .map(Object::toString)
                    .collect(Collectors.joining(" "));
        }
    }

    public static class OutputLimitSkip implements OutputLimit {
        protected IndexSelection target;

        public OutputLimitSkip(IndexSelection target) {
            this.target = target;
        }

        @Override
        public boolean isIncluded(long rows, List<String> data, long totalSize, long nextBytes) {
            return isIncluded(rows);
        }

        public boolean isIncluded(long rows) {
            return !target.contains(rows);
        }

        @Override
        public String toString() {
            return "--row-skip " + target.getMax();
        }
    }

    public static class OutputLimitSelect implements OutputLimit {
        protected IndexSelection target;

        public OutputLimitSelect(IndexSelection target) {
            this.target = target;
        }

        @Override
        public boolean isIncluded(long rows, List<String> data, long totalSize, long nextBytes) {
            return isIncluded(rows);
        }

        public boolean isIncluded(long rows) {
            return target.contains(rows);
        }

        @Override
        public boolean isOver(long rows, List<String> data, long totalSize, long nextBytes) {
            return isOver(rows);
        }

        public boolean isOver(long rows) {
            return rows >= target.getMax();
        }

        @Override
        public String toString() {
            return "--row-select " + target;
        }
    }

    public static class OutputLimitMaxRows implements OutputLimit {
        protected long maxRows;

        public OutputLimitMaxRows(long maxRows) {
            this.maxRows = maxRows;
        }

        @Override
        public boolean isIncluded(long rows, List<String> data, long totalSize, long nextBytes) {
            return isIncluded(rows);
        }

        public boolean isIncluded(long rows) {
            return rows < maxRows;
        }

        @Override
        public boolean isOver(long rows, List<String> data, long totalSize, long nextBytes) {
            return isOver(rows);
        }

        public boolean isOver(long rows) {
            return rows >= maxRows;
        }

        @Override
        public String toString() {
            return "--row-max " + String.format("%,d", maxRows).replaceAll(",", "_");
        }
    }

    public static class OutputLimitMaxBytes implements OutputLimit {
        protected long maxBytes;

        public OutputLimitMaxBytes(long maxBytes) {
            this.maxBytes = maxBytes;
        }

        @Override
        public boolean isIncluded(long rows, List<String> data, long totalSize, long nextBytes) {
            return isIncluded(totalSize, nextBytes);
        }

        @Override
        public boolean isOver(long rows, List<String> data, long totalSize, long nextBytes) {
            return isOverBytes(totalSize + nextBytes);
        }

        public boolean isIncluded(long totalBytes, long nextBytes) {
            return totalBytes + nextBytes < maxBytes;
        }

        public boolean isOverBytes(long totalBytes) {
            return totalBytes >= maxBytes;
        }

        @Override
        public String toString() {
            return "--row-max " + String.format("%,d", maxBytes).replaceAll(",", "_") + "b";
        }
    }

    public static class OutputTargetSplit extends OutputTargetBase {
        protected String argFile;
        protected String file;
        protected String suffix;
        protected Path dir;
        protected OutputSplit split;

        protected long splitCount;
        protected ByteBufferArray splitOutput;

        public OutputTargetSplit(Logger logger, String filePath) {
            super(logger);
            this.argFile = filePath;
            Path p = Paths.get(filePath);
            dir = p.getParent();
            String name = p.getFileName().toString();
            int dot = name.lastIndexOf(".");
            if (dot > 0) {
                file = name.substring(0, dot);
                suffix = name.substring(dot);
            } else {
                file = name;
                suffix = "";
            }
        }

        public OutputTargetSplit(Logger logger, String filePath, OutputSplit split) {
            this(logger, filePath);
            this.split = split;
        }

        public void setSplit(OutputSplit split) {
            this.split = split;
        }

        @Override
        public void writeData(ByteBuffer buf, List<String> data) {
            if (splitOutput == null || (split != null && split.isSplit(splitCount, fileRows, fileSize, buf.remaining()))) {
                if (splitOutput != null) {
                    splitOutput.close();
                }
                String fileName = String.format("%s-%04d%s", file, splitCount, suffix);
                Path file;
                if (dir == null) {
                    file = Paths.get(fileName);
                } else {
                    if (splitOutput == null && !Files.exists(dir)) { //first time
                        logger.log("create directory: %s", dir);
                        try {
                            Files.createDirectories(dir);
                        } catch (Exception ex) {
                            throw new RuntimeException(ex);
                        }
                    }
                    file = dir.resolve(fileName);
                }
                splitOutput = ByteBufferArray.createForWrite(file);
                fileRows = 0;
                fileSize = 0;
                splitCount++;
            }
            splitOutput.put(buf);
        }

        @Override
        public void writeAfter(CsvProcessor p) {
            super.writeAfter(p);
            if (splitOutput != null) {
                splitOutput.close();
                splitOutput = null;
            }
        }

        @Override
        public String getOutputStatus() {
            return String.format("splitCount=%,d", splitCount);
        }

        @Override
        public String toString() {
            return "--output " + super.toString() + " " + split;
        }
    }

    public interface OutputSplit {
        boolean isSplit(long splitCount, long fileRows, long fileBytes, long nextBytes);
    }

    public static class OutputSplitList implements OutputSplit {
        protected List<OutputSplit> list;

        public OutputSplitList() {
            list = new ArrayList<>();
        }

        public void add(OutputSplit s) {
            list.add(s);
        }

        public boolean isEmpty() {
            return list.isEmpty();
        }

        @Override
        public boolean isSplit(long splitCount, long fileRows, long fileBytes, long nextBytes) {
            if (list.isEmpty()) {
                return false;
            }
            return list.stream()
                    .mapToInt(s -> s.isSplit(splitCount, fileRows, fileBytes, nextBytes) ? 1 : 0)
                    .sum() > 0;
        }

        @Override
        public String toString() {
            return list.stream()
                    .map(Object::toString)
                    .collect(Collectors.joining(" "));
        }
    }

    public static class OutputSplitRows implements OutputSplit {
        protected long maxFileRows;

        public OutputSplitRows(long maxFileRows) {
            this.maxFileRows = maxFileRows;
        }

        @Override
        public boolean isSplit(long splitCount, long fileRows, long fileBytes, long nextBytes) {
            return isSplit(fileRows);
        }

        public boolean isSplit(long fileRows) {
            return fileRows >= maxFileRows;
        }

        @Override
        public String toString() {
            return "--split " + String.format("%,d", maxFileRows).replaceAll(",", "_");
        }
    }

    public static class OutputSplitBytes implements OutputSplit {
        protected long maxFileBytes;

        public OutputSplitBytes(long maxFileBytes) {
            this.maxFileBytes = maxFileBytes;
        }

        @Override
        public boolean isSplit(long splitCount, long fileRows, long fileBytes, long nextBytes) {
            return isSplitBytes(fileBytes, nextBytes);
        }

        public boolean isSplitBytes(long fileBytes, long nextBytes) {
            return fileBytes + nextBytes >= maxFileBytes;
        }

        @Override
        public String toString() {
            return "--split " + String.format("%,d", maxFileBytes).replaceAll(",", "_") + "b";
        }
    }

    public interface OutputTargetShuffle extends OutputTarget {
        void setTarget(OutputTarget target);
    }

    public static class OutputTargetShuffleOnMemory extends OutputTargetBase implements OutputTargetShuffle {
        protected long seed;
        protected List<byte[]> dataArrays;

        protected OutputTarget target;

        public OutputTargetShuffleOnMemory(Logger logger, long seed) {
            super(logger);
            this.seed = seed;
            dataArrays = new ArrayList<>();
        }

        @Override
        public void writeData(ByteBuffer buf, List<String> data) {
            byte[] array = new byte[buf.remaining()];
            buf.get(array);
            dataArrays.add(array);
        }

        @Override
        public boolean writeData(byte[] data) {
            dataArrays.add(data);
            return true;
        }

        @Override
        public void writeAfter(CsvProcessor p) {
            super.writeAfter(p);
            logger.log("start shuffle: seed=%,d", seed);
            Random rand = new Random(seed);
            Collections.shuffle(dataArrays, rand);
            for (byte[] data : dataArrays) {
                if (!target.writeData(data)) {
                    break;
                }
            }
            target.writeAfter(p);
        }

        @Override
        public String getOutputStatus() {
            return "shuffle-on-memory";
        }

        @Override
        public void setTarget(OutputTarget target) {
            this.target = target;
        }

        @Override
        public String toString() {
            return "--shuffle " + seed + " " + target;
        }
    }

    public static class OutputTargetShuffleWithFile extends OutputTargetBase implements OutputTargetShuffle  {
        protected long seed;
        protected String tmpFile;
        protected Map<String,Integer> fileToIndex = new HashMap<>();
        protected List<String> files = new ArrayList<>();

        protected OutputStream out;
        protected ByteBuffer buffer = ByteBuffer.wrap(new byte[UNIT_SIZE]).order(ByteOrder.BIG_ENDIAN);

        protected OutputTarget target;

        static final int UNIT_SIZE = 12;

        protected Map<String,CsvReader> readerCache = new LinkedHashMap<>();
        static final int READER_CACHE_SIZE = 100;

        public OutputTargetShuffleWithFile(Logger logger, long seed, String tmpFile) {
            super(logger);
            this.seed = seed;
            this.tmpFile = tmpFile;
        }

        @Override
        public boolean write(CsvLine sourceLine, String sourceFile, CsvProcessor p) {
            try {
                //tmpFile: (long filePosition, int fileIndex), ...
                int fi = fileToIndex.computeIfAbsent(sourceFile, sf -> {
                    int s = files.size();
                    files.add(sf);
                    return s;
                });
                if (out == null) {
                    Path tmpPath = Paths.get(tmpFile);
                    Path dir = tmpPath.getParent();
                    if (dir != null && !Files.exists(dir)) {
                        logger.log("create tmpFile.dir=%s", dir);
                        try {
                            Files.createDirectories(dir);
                        } catch (Exception ex) {
                            throw new RuntimeException(ex);
                        }
                    }
                    logger.log("tmpFile=%s", tmpPath);
                    out = new BufferedOutputStream(Files.newOutputStream(tmpPath), 5_000_000);
                }
                buffer.clear();
                buffer.putLong(sourceLine.getStart());
                buffer.putInt(fi);
                out.write(buffer.array());
                totalRows++;
                totalSize += UNIT_SIZE;
                if (logger.isLogPeriod()) {
                    logger.log("tmpFile.pos=%,d, lines=%,d, file=%s fileId=%,d", totalSize, totalRows, sourceFile, fi);
                }
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
            return true;
        }

        @Override
        public void writeAfter(CsvProcessor p) {
            if (out != null){
                try {
                    out.close();
                    out = null;
                } catch (Exception ex) {
                    throw new RuntimeException(ex);
                }
            }
            super.writeAfter(p);
            long lines = 0;
            long size;
            try {
                size = Files.size(Paths.get(tmpFile));
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
            try (ByteBufferArray index = ByteBufferArray.createForReadWrite(Paths.get(tmpFile)).order(ByteOrder.BIG_ENDIAN)) {
                index.limit(size);
                long startPos = findStartPos(index);
                Random rand = new Random(seed);
                lines = (size - startPos) / UNIT_SIZE;
                logger.log("start shuffle: seed=%d, size=%,d, startPos=%,d, lines=%,d", seed, size, startPos, lines);
                while (lines > 0) {
                    long n = (long) (rand.nextDouble() * lines);
                    long targetPos = startPos + n * UNIT_SIZE;
                    //next record: 0:(long start,int fileIdx), 12:(long start,int fileIdx), ...
                    index.position(startPos);
                    long fileIndexStart = index.getLong();
                    int fileIdStart = index.getInt();

                    //target record: random position
                    index.position(targetPos);
                    long fileIndex = index.getLong();
                    int fileId = index.getInt();
                    //move data[targetPos] <- data[startPos]; data[startPos] <- (-1,-1)
                    index.position(targetPos);
                    index.putLong(fileIndexStart);
                    index.putInt(fileIdStart);

                    index.position(startPos);
                    index.putLong(-1L);
                    index.putInt(-1);
                    startPos += UNIT_SIZE;
                    --lines;
                    //write selected data[targetPos] to file
                    if (!writeNext(fileIndex, fileId, p)) {
                        break;
                    }
                }
            } finally {
                closeReaders();
            }
            if (lines == 0) {
                logger.log("remove tmpFile=%s", tmpFile);
                try {
                    Files.deleteIfExists(Paths.get(tmpFile));
                } catch (Exception ex) {
                    throw new RuntimeException(ex);
                }
            }
            target.writeAfter(p);
        }

        private long findStartPos(ByteBufferArray index) {
            long size = index.limit();
            long startPos = 0;
            index.position(startPos);
            while (startPos < size) {
                long fileIndex = index.getLong();
                index.getInt();
                if (fileIndex >= 0) {
                    break;
                }
                startPos += UNIT_SIZE;
            }
            return startPos;
        }

        public boolean writeNext(long index, int fileId, CsvProcessor p) {
            String path = files.get(fileId);
            CsvReader r = getReader(path);
            CsvLine line = r.readNext(index);
            return this.target.write(line, path, p);
        }

        public CsvReader getReader(String path) {
            CsvReader r = readerCache.computeIfAbsent(path,
                    p -> CsvReader.get(Paths.get(p)));
            int size = readerCache.size();
            if (size > READER_CACHE_SIZE) {
                Iterator<Map.Entry<String, CsvReader>> iter = readerCache.entrySet().iterator();
                while (size > READER_CACHE_SIZE && iter.hasNext()) {
                    Map.Entry<String, CsvReader> e = iter.next();
                    e.getValue().close();
                    iter.remove();
                    --size;
                }
            }
            return r;
        }

        public void closeReaders() {
            Iterator<Map.Entry<String, CsvReader>> iter = readerCache.entrySet().iterator();
            while (iter.hasNext()) {
                Map.Entry<String, CsvReader> e = iter.next();
                e.getValue().close();
                iter.remove();
            }
        }

        @Override
        public void setTarget(OutputTarget target) {
            this.target = target;
        }

        @Override
        public String toString() {
            return "--shuffle-file " + seed + " " + tmpFile + " "  +target;
        }
    }
}
