package csl.dataproc.birch;

import csl.dataproc.csv.*;
import csl.dataproc.dot.DotAttribute;
import csl.dataproc.dot.DotGraph;
import csl.dataproc.dot.DotNode;
import csl.dataproc.tgls.util.ArraysAsList;
import csl.dataproc.tgls.util.JsonWriter;

import java.io.BufferedWriter;
import java.io.Closeable;
import java.io.IOException;
import java.io.Serializable;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.MessageDigest;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.function.Function;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * An implementation of BIRCH proposed by
 *  T.Zhang et. al in "BIRCH: an efficient data clustering method for very large databases" (ACM SIGMOD'96).
 *
 *  (Currently it does not support automatic rebuilding)
 */
public class BirchBatch {

    public static void main(String[] args) {
        BirchBatch b = new BirchBatch();
        List<String> rest = b.parseArgs(args);
        if (!rest.contains("--help")) {
            if (!rest.isEmpty()) {
                System.err.println("remaining-args: " + rest);
            }
            b.run();
        }
    }

    public static BirchBatch get() {
        return new BirchBatch();
    }

    public BirchBatch(BirchConfig config) {
        this.config = config;
    }

    public BirchBatch() {
        this(new BirchConfig());
    }

    protected BirchConfig config;
    protected CsvIO settings = new CsvIO();
    protected String input = null;
    protected String inputShuffle = null;
    protected boolean normalize;
    protected List<DataField> fields = new ArrayList<>();
    protected DataField classLabel;
    protected boolean skipPurity = false;
    protected BirchPurity purity = new BirchPurity();

    protected Extract normalizerAll = null;
    protected Map<Integer,Extract> normalizerFields = new LinkedHashMap<>();

    protected Path dotFile;

    protected Path outDir;
    protected String outNamePattern = "%d.txt";
    protected String outSingleName = null;
    protected Charset charset = StandardCharsets.UTF_8;
    protected String summaryName = "node-info.txt";
    protected int skipHeaders = 0;

    protected Duration constructTime;
    protected long constructPoints;

    protected BirchNode root;
    protected boolean experiment = false;
    protected Double calculatedPurity;

    protected Timer timer;
    protected Instant startTime;

    public List<String> parseArgs(String... args) {
        return parseArgs(ArraysAsList.get(args));
    }

    public List<String> parseArgs(List<String> args) {
        List<String> rest = new ArrayList<>();

        for (int i = 0, l = args.size(); i < l; ++i) {
            String s = args.get(i);
            if (s.equals("--delimiter")) {
                ++i;
                s = args.get(i);
                char n;
                if (s.length() >= 2 && s.charAt(0) == '\\') {
                    n = s.charAt(1);
                    if (n == 'n') {
                        n = '\n';
                    } else if (n == 't') {
                        n = '\t';
                    } else if (n == 'r') {
                        n = '\r';
                    }
                } else {
                    n = s.charAt(0);
                }
                settings.setDelimiter(n);
            } else if (s.equals("-i") || s.equals("--input")) { //-i file
                ++i;
                input = args.get(i);

            } else if (s.equals("-t") || s.equals("--threshold")) { //-t maxNodeEntries maxDistance
                ++i;
                s = args.get(i);
                config.maxNodeEntries = Integer.valueOf(s);
                ++i;
                s = args.get(i);
                config.maxDistance = Double.valueOf(s);
            } else if (s.equals("-d") || s.equals("--distance")) { //-d N
                ++i;
                s = args.get(i);
                config.distanceFunction = getDistanceFunction(s);
                if (config.distanceFunction == null) {
                    config.distanceFunction = BirchNode.d0CentroidEuclidDistance;
                }
            } else if (s.equals("-f") || s.equals("--fields")) { //-f N,....
                ++i;
                s = args.get(i);
                fields.addAll(fieldsForColumns(parseFieldIndexes(s)));
            } else if (s.equals("-c") || s.equals("--class")) { //-c N
                ++i;
                s = args.get(i);
                classLabel = new DataField(Integer.valueOf(s));

            } else if (s.equals("--fields-minmax")) { //--fields-minmax min max
                ++i;
                String min = args.get(i);
                ++i;
                String max = args.get(i);

                normalizerAll = new ExtractMinMax(Double.valueOf(min), Double.valueOf(max));

            } else if (s.equals("--field-minmax")) { //--field-minmax N min max
                ++i;
                String idx = args.get(i);
                ++i;
                String min = args.get(i);
                ++i;
                String max = args.get(i);

                normalizerFields.put(Integer.valueOf(idx), new ExtractMinMax(Double.valueOf(min), Double.valueOf(max)));

            } else if (s.equals("--fields-log")) { //--fields-log min max
                ++i;
                String min = args.get(i);
                ++i;
                String max = args.get(i);

                normalizerAll = new ExtractLog(Double.valueOf(min), Double.valueOf(max));

            } else if (s.equals("--field-log")) { //--field-log N min max
                ++i;
                String idx = args.get(i);
                ++i;
                String min = args.get(i);
                ++i;
                String max = args.get(i);

                normalizerFields.put(Integer.valueOf(idx), new ExtractLog(Double.valueOf(min), Double.valueOf(max)));

            } else if (s.equals("--fields-zscore")) { //--fields-zscore mean std
                ++i;
                String min = args.get(i);
                ++i;
                String max = args.get(i);

                normalizerAll = new ExtractZ(Double.valueOf(min), Double.valueOf(max));

            } else if (s.equals("--field-zscore")) { //--field-zscore N mean std
                ++i;
                String idx = args.get(i);
                ++i;
                String min = args.get(i);
                ++i;
                String max = args.get(i);

                normalizerFields.put(Integer.valueOf(idx), new ExtractZ(Double.valueOf(min), Double.valueOf(max)));


            } else if (s.equals("--dot")) { //--dot file
                ++i;
                s = args.get(i);
                dotFile = Paths.get(s);
            } else if (s.equals("--output")) { //--output dir
                ++i;
                s = args.get(i);
                outDir = Paths.get(s);
            } else if (s.equals("--outname")) { //--outname pattern #default is "%d.txt"
                ++i;
                outNamePattern = args.get(i);
            } else if (s.equals("--out-single")) { //--out-single name # output-dir/name
                ++i;
                outSingleName = args.get(i);
            } else if (s.equals("--charset")) { //--charset cs
                ++i;
                s = args.get(i);
                charset = Charset.forName(s);
            } else if (s.equals("--summaryname")) {
                ++i;
                summaryName = args.get(i);
            } else if (s.equals("--skip")) {
                ++i;
                s = args.get(i);
                skipHeaders = Integer.valueOf(s);
            } else if (s.equals("--shuffle")) { //--shuffle file
                ++i;
                s = args.get(i);
                inputShuffle = s;
            } else if (s.equals("--normalize")) {
                normalize = true;
            } else if (s.equals("--skip-purity")) {
                skipPurity = true;
            } else if (s.equals("--debug")) {
                config.debug = true;
            } else if (s.equals("--experiment")) {
                experiment = true;
            } else if (s.equals("--help") || s.equals("-h")) {
                showHelp();
                rest.add("--help");
                break;
            } else {
                rest.add(s);
            }
        }

        purity.setClassLabel(classLabel);
        purity.setInputFile(getInputFileForRead());
        rest = purity.parseArgs(rest);

        if (args.isEmpty()) {
            showHelp();
            rest.add("--help");
        }

        if (normalizerAll != null || !normalizerFields.isEmpty()) {
            for (ListIterator<DataField> iter = fields.listIterator(); iter.hasNext(); ) {
                DataField f = iter.next();
                Extract n = getNormalizer(f.index);
                if (n != null) {
                    f = new DataField(f.index, n);
                    iter.set(f);
                }
            }
        }

        if (classLabel != null) {
            config = config.toTest();
        }

        return rest;
    }

    /**
     * @param idx field index
     * @return a normalizer if specified, or null if not specified
     */
    public Extract getNormalizer(int idx) {
        return normalizerFields.getOrDefault(idx, normalizerAll);
    }

    public static BirchNode.DistanceFunction getDistanceFunction(String name) {
        switch (name) {
            case "0":
                return BirchNode.d0CentroidEuclidDistance;
            case "1":
                return BirchNode.d1CentroidManhattanDistance;
            case "2":
                return BirchNode.d2AverageInterClusterDistance;
            case "3":
                return BirchNode.d3AverageIntraClusterDistance;
            case "4":
                return BirchNode.d4VarianceIncreaseDistance;
            default:
                return null;
        }
    }

    public void showHelp() {
        System.out.println(help());
    }

    public String help() {
        return String.join("\n",
                getClass().getName(),
                "arguments:",
                "    -i|--input <file>         : input CSV file",
                "    -t|--threshold <B> <T>    : B is a max node size (int), T is a distance threshold (double)",
                "    -d|--distance <N>         : selects a distance function DN (N=0|1|2|3|4)",
                "    -f|--fields <N1>,<N2>,... : column indices extracted as data points. ",
                "                                   each column can take a range F-T, meaning F,F+1,F+2,...T",
                "    -c|--class <N>            : specify the index of class label. it changes to the testing mode with calculating the purity.",
                "    --fields-zscore <mean> <std>   : normalizing all fields by minus mean and divide std",
                "    --field-zscore <N> <mean> <std>: normalizing the field N by minus mean and divide std",
                "    --fields-minmax <min> <max>   : limiting the range max..min and normalizing 0..1 for all fields ",
                "    --field-minmax <N> <min> <max>: limiting the range max..min and normalizing 0..1 for the field N",
                "    --fields-log <min> <max>  : apply log10 with limiting the range max..min and normalizing 0..1 for all fields ",
                "    --field-log <N> <min> <max>: apply log10 with normalizing for the field N",
                "    --output <dir>            : output directory containing files as dir/N1/N2/.../I.txt",
                "    --outname <pattern>       : output file name pattern. default is \"%d.txt\".",
                "                                 An output file is a text file and contains source byte offsets as entries-points separated by newlines.",
                "    --out-single <name.csv>   : switch to output of single-file format. name.csv and name-cf.csv will be generated. ",
                "    --charset <name>          : input charset name. default is UTF-8.",
                "    --dot <file>              : output .dot file",
                "    --delimiter <c>           : the delimiter of the input file. default is comma(,)",
                "    --skip <N>                : skips header N lines of the input file",
                "    --summaryname <name>      : the name of summary CSV file for each node. default is \"node-info.txt\"",
                "    --shuffle <file>          : use the shuffled CSV file as the input. if the file does not exist, creates it from -i file.",
                "    --normalize               : normalize input (note: suppose shuffled data is already normalized)",
                "    --classsize <N>           : specify each class table size used for calculating purity, can be a smaller size of the max number.",
                "    --skip-purity             : skip calculating purity"
                ) +
                "\n" + String.join("\n", purity.getHelp());
    }

    public void run() {
        shuffle();
        construct();
        output();
    }

    public void shuffle() {
        if (inputShuffle != null) {
            Path shufflePath = Paths.get(inputShuffle);
            if (!Files.exists(shufflePath)) {
                createAndSaveShuffleFromInput();
            }
        }
    }

    public void createAndSaveShuffleFromInput() {
        ArrayList<BirchNodeInput> inputs = new ArrayList<>();
        try (InputI i = getInput(input)) {
            CsvReader reader = i.getInput();
            i.makeDataPointStream(reader).forEach(inputs::add);
            inputs.trimToSize();
            Collections.shuffle(inputs);
        }
        try (CsvWriter writer = new CsvWriter().withOutputFile(Paths.get(inputShuffle), charset)) {
            writer.save(inputs.stream()
                .map(BirchNodeInput::toLine));
        }
    }

    public Iterable<BirchNodeInput> processNormalize(Iterable<BirchNodeInput> points) {
        if (normalize) {
            ArrayList<BirchNodeInput> normalizedPoints = new ArrayList<>();
            points.forEach(normalizedPoints::add);
            normalizedPoints.trimToSize();
            normalize(normalizedPoints);
            return normalizedPoints;
        } else {
            return points;
        }
    }

    public void normalize(List<BirchNodeInput> inputs) {
        int dim = fields.size();
        double[] maxList = new double[dim];
        double[] minList = new double[dim];

        Arrays.fill(maxList, Double.MIN_VALUE);
        Arrays.fill(minList, Double.MAX_VALUE);

        //obtains max and min values
        for (BirchNodeInput input : inputs) {
            double[] ls = input.getClusteringFeature().getLinearSum();
            for (int i = 0; i < dim; ++i) {
                maxList[i] = Math.max(maxList[i], ls[i]);
                minList[i] = Math.min(minList[i], ls[i]);
            }
        }

        //update the data
        for (BirchNodeInput input : inputs) {
            double[] ls = input.getClusteringFeature().getLinearSum();
            for (int i = 0; i < dim; ++i) {
                ls[i] = (ls[i] - minList[i]) / (maxList[i] - minList[i]);
            }
        }
    }

    public void construct() {
        try (InputI input = getInputForRead()) {
            CsvReader reader = input.getInput();
            root = construct(input, fields, reader);
        }
    }

    public InputI getInputForRead() {
        return getInput(getInputFileForRead());
    }

    public String getInputFileForRead() {
        return inputShuffle == null ? input : inputShuffle;
    }

    public InputI getInput(String input) {
        return new Input(input, charset.name(), new CsvIO.CsvSettings(settings),
                skipHeaders, fields, classLabel, config);
    }

    public interface InputI extends Closeable {
        CsvReader getInput();
        Stream<BirchNodeInput> makeDataPointStream(Iterable<CsvLine> lines);
        BirchNodeInput makeDataPoint(CsvLine line);

        void close();

        default Stream<BirchNodeInput> makeDataPointStream() {
            return makeDataPointStream(getInput());
        }
    }

    public static class Input implements InputI, Serializable {
        protected transient CsvReader input;
        protected String inputFile;
        protected String charsetName;
        protected CsvIO.CsvSettings settings;
        protected int skipHeaders;
        protected List<DataField> fields;
        protected DataField classLabel;
        protected BirchConfig config;

        public Input() { }

        public Input(String inputFile, String charsetName, CsvIO.CsvSettings settings,
                     int skipHeaders, List<DataField> fields, DataField classLabel, BirchConfig config) {
            this.inputFile = inputFile;
            this.charsetName = charsetName;
            this.settings = settings;
            this.skipHeaders = skipHeaders;
            this.fields = fields;
            this.classLabel = classLabel;
            this.config = config;
        }

        @Override
        public CsvReader getInput() {
            if (input == null) {
                input = CsvReader.get(Paths.get(inputFile), Charset.forName(charsetName)).setFields(settings);
            }
            return input;
        }

        @Override
        public void close() {
            if (input != null) {
                input.close();
            }
            input = null;
        }

        @Override
        public Stream<BirchNodeInput> makeDataPointStream(Iterable<CsvLine> lines) {
            return StreamSupport.stream(lines.spliterator(), false)
                    .skip(skipHeaders)
                    .filter(line -> !line.isEmptyLine())
                    .map(this::makeDataPoint);
        }

        @Override
        public BirchNodeInput makeDataPoint(CsvLine line) {
            Stream<DataField> dataCons;
            if (classLabel == null) {
                dataCons = fields.stream();
            } else {
                dataCons = Stream.concat(fields.stream(), Stream.of(classLabel));
            }

            return config.newPoint(line.getStart(),
                    dataCons.mapToDouble(f -> f.extract(line))
                            .toArray());
        }
    }

    public void output() {
        if (outDir != null) {
            try {
                Files.createDirectories(outDir);
            } catch (Exception ex) {
                ex.printStackTrace();
            }
        }
        if (dotFile != null) {
            toDot(root).save(dotFile.toFile());
        }

        if (outDir != null && (!experiment || outSingleName != null)) {
            try {
                if (outSingleName == null) {
                    save(root, outDir);
                } else {
                    saveSingle(root, outDir);
                }
            } catch (Exception ex) {
                ex.printStackTrace();
            }
        }

        long es = countEntries(root);
        long ds = countEntryPoints(root);
        System.out.printf("entries:    %,d\n", es);
        System.out.printf("dataPoints: %,d\n", ds);
        System.out.printf("time: %s\n", CsvStat.toStr(constructTime));
        outputPurity();

        if (outDir != null){
            try {
                saveTreeStat(root, outDir);
            } catch (Exception ex) {
                ex.printStackTrace();
            }
        }
    }

    public void outputPurity() {
        if (!skipPurity && config.isTest()) {
            purity.init();
            double p = purity.run(root);
            System.out.printf("DendrogramPurity : %f\n", p);
            calculatedPurity = p;
        }
    }

    public static class DataField implements Serializable {
        public int index;
        public Function<String,Double> generator;

        static final long serialVersionUID = 1L;

        public DataField() {}

        public DataField(int i) {
            this(i, extract);
        }

        public DataField(int index, Function<String, Double> generator) {
            this.index = index;
            this.generator = generator;
        }

        public double extract(CsvLine line) {
            return extractFromValue(line.get(index));
        }

        public double extractFromValue(String str) {
            return generator.apply(str);
        }

        @Override
        public String toString() {
            return "[" + index + "]" + generator;
        }
    }

    public static Extract extract = new Extract();

    public static class Extract implements Function<String,Double>, Serializable {
        static final long serialVersionUID = 1L;

        @Override
        public Double apply(String s) {
            return extract(s);
        }

        public double extract(String s) {
            return extractWithoutClip(s);
        }

        public double extractWithoutClip(String s) {
            try {
                if (s.isEmpty()) {
                    return 0;
                } else {
                    char c = s.charAt(0);
                    if (Character.isDigit(c) || c == '-' || c == '+') { //ignore NaN, Infinity, HexNumeral
                        return Double.valueOf(s);
                    } else {
                        return hash(s);
                    }
                }
            } catch (Exception ex) {
                return hash(s);
            }
        }
        static MessageDigest md;
        static {
            try {
                md = MessageDigest.getInstance("MD5");
            } catch (Exception ex) {
                ex.printStackTrace();
            }
        }
        private double hash(String s) {
            //int n = s.hashCode();
            //return (double) (0x0000_0000_FFFF_FFFFL & n);

            byte[] ds = md.digest(s.getBytes());
            int hash = 1;
            for (byte d : ds) {
                hash = 31 * hash + d;
            }
            return hash;
        }


        public Map<String,Object> toJson() {
            Map<String,Object> map = new LinkedHashMap<>();
            map.put("type", getName());
            return map;
        }

        public String getName() {
            return "extract";
        }

        @Override
        public String toString() {
            return getName();
        }
    }

    public static class ExtractZ extends Extract {
        static final long serialVersionUID = 1L;
        public double mean;
        public double std;

        public ExtractZ(double mean, double std) {
            this.mean = mean;
            this.std = std;
        }

        @Override
        public double extract(String s) {
            return super.extract(s) / std;
        }

        @Override
        public double extractWithoutClip(String s) {
            return (super.extractWithoutClip(s) - mean);
        }

        @Override
        public Map<String, Object> toJson() {
            Map<String,Object> map = super.toJson();
            map.put("mean", mean);
            map.put("std", std);
            return map;
        }

        @Override
        public String getName() {
            return "z-score";
        }

        @Override
        public String toString() {
            return getName() + "(" + mean + "," + std + ")";
        }
    }

    public static class ExtractMinMax extends Extract {
        static final long serialVersionUID = 1L;
        public double min;
        public double max;

        public ExtractMinMax(double min, double max) {
            this.min = min;
            this.max = max;
        }

        @Override
        public double extract(String s) {
            double d = super.extract(s);
            if (max < d) {
                d = max;
            }
            if (min > d) {
                d = min;
            }
            return (d - min) / (max - min);
        }
        @Override
        public Map<String, Object> toJson() {
            Map<String,Object> map = super.toJson();
            map.put("min", min);
            map.put("max", max);
            return map;
        }

        @Override
        public String getName() {
            return "min-max";
        }

        @Override
        public String toString() {
            return getName() + "(" + min + "," + max + ")";
        }
    }

    public static class ExtractLog extends Extract {
        static final long serialVersionUID = 1L;

        public double min;
        public double max;

        public ExtractLog(double min, double max) {
            this.min = min;
            this.max = max;
        }

        @Override
        public double extract(String s) {
            double d = super.extract(s);
            if (max < d) {
                d = max;
            }
            if (min > d) {
                d = min;
            }
            return (d - min) / (max - min);
        }

        @Override
        public double extractWithoutClip(String s) {
            double d = super.extractWithoutClip(s);
            if (d == 0) {
                d = min;
            } else {
                if (d < 0) {
                    d = -Math.log10(-d);
                } else {
                    d = Math.log10(d);
                }
            }
            return d;
        }

        @Override
        public Map<String, Object> toJson() {
            Map<String,Object> map = super.toJson();
            map.put("min", min);
            map.put("max", max);
            return map;
        }

        @Override
        public String getName() {
            return "log10";
        }

        @Override
        public String toString() {
            return getName() + "(" + min + "," + max + ")";
        }
    }

    public static List<DataField> fields(Function<String,Double>... gens) {
        List<DataField> fs = new ArrayList<>(gens.length);
        for (int i = 0; i < gens.length; ++i) {
            fs.add(new DataField(i, gens[i]));
        }
        return fs;
    }

    /**
     * <pre>
     * str ::= i,i,...
     * i   ::= int
     *       | int-int   //"from" and "to", inclusive
     *  </pre>
     */
    public int[] parseFieldIndexes(String str) {
        return Arrays.stream(str.split(Pattern.quote(",")))
                .flatMapToInt(s -> {
                    if (s.contains("-")) {
                        String[] cols = s.split(Pattern.quote("-"));
                        String from = cols[0];
                        String to = cols[1];
                        return IntStream.range(Integer.valueOf(from), Integer.valueOf(to) + 1);
                    } else {
                        return IntStream.of(Integer.valueOf(s));
                    }
                }).toArray();
    }

    public static List<DataField> fieldsForColumns(int... columns) {
        return Arrays.stream(columns)
                .mapToObj(DataField::new)
                .collect(Collectors.toList());
    }

    public BirchNode construct(InputI input, List<DataField> fields, Iterable<CsvLine> lines) {
        return construct(fields.size(),
                processNormalize(input.makeDataPointStream(lines)::iterator));
    }

    public BirchNode construct(int dimension, Iterable<BirchNodeInput> points) {
        root = newRoot(dimension);
        timer = new Timer(Duration.ofSeconds(1));
        startTime = timer.getPrevTime();
        points.forEach(this::inputData);
        logConstruct(Duration.between(startTime, timer.getPrevTime()), timer.getCount(), root, null);
        setConstructTimeNow(startTime);
        return root;
    }

    public void inputData(BirchNodeInput e) {
        root = root.put(e);
        ++constructPoints;
        if (timer.next()) {
            logConstruct(Duration.between(startTime, timer.getPrevTime()), timer.getCount(), root, e);
        }
    }

    protected void setConstructTimeNow(Instant start) {
        constructTime = Duration.between(start, Instant.now());
    }

    protected BirchNode newRoot(int dimension) {
        if (config.debug) {
            BirchNodeCFTree.trace = new BirchNodeCFTree.PutTraceLog();
        }
        return config.newRoot(dimension);
    }

    public static class Timer {
        /** the number of calls of {@link #next()}*/
        long count;
        /** the next check point of {@link #count} which can return true by {@link #next()} */
        long nextCheckPoint = 100;
        long prevCheckPoint = 0;

        Duration limit;
        Instant prevTime;

        private static long oneSecNano = 1000_000_000L;
        private static long maxSeconds = Long.MAX_VALUE / oneSecNano;

        public Timer(Duration limit) {
            this.limit = limit;
            start();
        }

        public void start() {
            count = 0;
            prevTime = Instant.now();
        }

        public boolean next() {
            boolean over = false;
            if (count >= nextCheckPoint) {
                Instant nextTime = Instant.now();
                Duration checkDuration = Duration.between(prevTime, nextTime);

                over = limit.compareTo(checkDuration) <= 0;

                long countElapsed = count - prevCheckPoint;
                Duration unitTime = (countElapsed == 0 ? checkDuration : checkDuration.dividedBy(countElapsed));

                long unitTimeNanos = nanos(unitTime);
                Duration limitElapsed = (over ? limit : limit.minus(checkDuration));
                Duration times = (unitTimeNanos == 0 ? limitElapsed : limitElapsed.dividedBy(unitTimeNanos));

                prevCheckPoint = nextCheckPoint;
                nextCheckPoint += nanos(times);
                prevTime = nextTime;
            }
            ++count;
            return over;
        }

        private long nanos(Duration d) {
            return (Math.min(d.getSeconds(), maxSeconds) * oneSecNano + d.getNano());
        }

        public long getCount() {
            return count;
        }

        public Instant getPrevTime() {
            return prevTime;
        }
    }

    public void logConstruct(Duration d, long count, BirchNode root, BirchNodeInput e) {
        System.err.println(String.format("[%s]: %,d, %s", CsvStat.toStr(d), count, e != null ? e : "(finish)"));
    }

    /** the number of BirchNodeCFEntry */
    public long countEntries(BirchNode e) {
        return new CountEntriesVisitor().run(e);
    }

    public long countEntryPoints(BirchNode e) {
        return new CountEntryPointsVisitor().run(e);
    }


    public void save(BirchNode e, Path dir) {
        new SaveVisitor(summaryName, outNamePattern).run(e, dir);
    }

    public void saveSingle(BirchNode e, Path dir) {
        new SaveSingleFileVisitor().run(e, dir.resolve(outSingleName));
    }

    public void saveTreeStat(BirchNode e, Path dir) {
        JsonWriter.write(getTreeStats(e), dir.resolve("tree-stats.json"));
    }

    public Map<String,Object> getTreeStats(BirchNode e) {
        Map<String,Object> json = new LinkedHashMap<>();
        Map<Integer,Long> depthToNodes = getTreeDepthToNodes(e);
        int size = depthToNodes.keySet().stream()
                .mapToInt(Integer::intValue)
                .max().orElse(-1) + 1;
        List<Long> counts = new ArrayList<>(size);
        long total = 0;
        for (int i = 0; i < size; ++i) {
            Long n = depthToNodes.get(i);
            if (n == null) {
                n = 0L;
            }
            counts.add(n);
            total += n;
        }
        json.put("depthToNodes", counts);
        json.put("maxDepth", size);
        json.put("entries", countEntries(e));
        json.put("nodes", total);
        json.put("entryPoints", countEntryPoints(e));
        json.put("constructTime", CsvStat.toStr(constructTime));
        json.put("constructPoints", constructPoints);

        //other parameters
        json.put("input", input);
        json.put("inputShuffle", inputShuffle);
        json.put("normalize", normalize);
        json.put("skipHeaders", skipHeaders);
        json.put("skipPurity", skipPurity);
        json.put("fields", fields.stream()
                .map(f -> f.index)
                .collect(Collectors.toList()));
        if (classLabel != null) {
            json.put("classLabel", classLabel.index);
        }
        if (normalizerAll != null) {
            json.put("normalizerAll", normalizerAll.toJson());
        }
        if (normalizerFields != null) {
            Map<String,Object> m = new LinkedHashMap<>();
            normalizerFields.forEach((f, range) -> {
                m.put(Integer.toString(f), range.toJson());
            });
            json.put("logFields", m);
        }
        json.put("maxNodeEntries", config.maxNodeEntries);
        json.put("maxDistance", config.maxDistance);
        json.put("distanceFunction", config.distanceFunction);
        //confusing: json.put("classSize", classSize); //optional parameter for calculating purity: LongList table size

        if (calculatedPurity != null) {
            json.put("purity", String.format("%.3f", calculatedPurity));
        }
        return json;
    }

    public Map<Integer, Long> getTreeDepthToNodes(BirchNode e) {
        return new CountDepthVisitor().run(e);
    }

    ////////

    public DotGraph toDot(BirchNode node) {
        return new DotVisitor().run(node);
    }

    public static class CountEntriesVisitor extends BirchNode.BirchVisitor implements Serializable {
        protected long count;

        static final long serialVersionUID = 1L;

        public long run(BirchNode node) {
            count = 0;
            node.accept(this);
            return count;
        }

        public long getCount() {
            return count;
        }

        @Override
        public boolean visitEntry(BirchNodeCFEntry e) {
            ++count;
            return super.visitEntry(e);
        }
    }

    public static class CountEntryPointsVisitor extends BirchNode.BirchVisitor implements Serializable {
        protected long count;

        static final long serialVersionUID = 1L;

        public long run(BirchNode node) {
            count = 0;
            node.accept(this);
            return count;
        }

        public long getCount() {
            return count;
        }

        @Override
        public boolean visitEntry(BirchNodeCFEntry e) {
            count += e.getIndexes().size();
            return super.visitEntry(e);
        }
    }

    public static class CountDepthVisitor extends BirchNode.BirchVisitor {
        protected Map<Integer,Long> depthToNodes = new HashMap<>();
        protected int depth = 0;

        public Map<Integer,Long> run(BirchNode n) {
            depthToNodes.clear();
            n.accept(this);
            return depthToNodes;
        }

        public Map<Integer, Long> getDepthToNodes() {
            return depthToNodes;
        }

        @Override
        public boolean visit(BirchNode node) {
            depthToNodes.compute(depth, (k,v) -> v == null ? 1L : (v + 1L));
            return true;
        }

        @Override
        public boolean visitTree(BirchNodeCFTree t) {
            if (super.visitTree(t)) {
                ++depth;
                return true;
            } else {
                return false;
            }
        }

        @Override
        public void visitAfterTree(BirchNodeCFTree t) {
            --depth;
        }
    }

    public static class SaveVisitor extends BirchNode.BirchVisitor {
        protected int nextIndex;
        protected LinkedList<Path> dirs = new LinkedList<>();
        protected String summaryName;
        protected String outNamePattern;

        public SaveVisitor() {
            this("node-info.txt", "%d.txt");
        }

        public SaveVisitor(String summaryName, String outNamePattern) {
            this.summaryName = summaryName;
            this.outNamePattern = outNamePattern;
        }

        public void run(BirchNode e, Path dir) {
            dirs.push(dir);
            e.accept(this);
        }

        @Override
        public boolean visitTree(BirchNodeCFTree e) {
            Path dir = dirs.peek();
            int ci = 0;
            CsvWriter summary = new CsvWriter();
            summary.withOutputFile(dir.resolve(summaryName));
            summary.writeLine("#index", "type", "childSize", "CF.entries", "CF.squareSum", "CF.centroid");
            summary.writeLine(summary(-1, e));
            for (BirchNode child : e.getChildren()) {
                summary.writeLine(summary(ci, child));
                ++ci;
            }

            summary.close();

            dirs.push(dir); //always push a head item for simplifying code
            return true;
        }

        @Override
        public void visitTreeChild(BirchNodeCFTree tree, int i, BirchNodeCFTree.NodeHolder nodeHolder) {
            dirs.pop();
            Path subDir = dirs.peek().resolve(Integer.toString(i));
            dirs.push(subDir);
            if (!Files.exists(subDir)) {
                try {
                    Files.createDirectory(subDir);
                } catch (Exception ex) {
                    System.err.println(ex + ": " + i + ", " + nodeHolder.node + ", " + subDir);
                    ex.printStackTrace();
                }
            }
            nextIndex = i;
        }

        @Override
        public void visitAfterTree(BirchNodeCFTree t) {
            dirs.pop();
        }

        @Override
        public boolean visitEntry(BirchNodeCFEntry e) {
            Path dir = dirs.peek();
            Path file = dir.resolve(String.format(outNamePattern, nextIndex));
            try (BufferedWriter w = Files.newBufferedWriter(file)) {
                for (int d = 0, dl = e.getIndexes().size(); d < dl; ++d) {
                    w.write(Long.toString(e.getIndexes().get(d)));
                    w.write('\n');
                }
            } catch (Exception ex) {
                ex.printStackTrace();
            }
            return true;
        }


        public List<String> summary(int index, BirchNode e) {
            List<String> data = new ArrayList<>();
            try {

                if (index < 0) {
                    data.add("");
                } else {
                    data.add(Integer.toString(index));
                }
                if (isTreeNode(e)) {
                    data.add("n");
                    data.add(Integer.toString(e.getChildSize()));
                } else {
                    data.add("e");
                    data.add("");
                }
                ClusteringFeature cf = e.getClusteringFeature();
                data.add(Integer.toString(cf.getEntries()));
                data.add(Double.toString(cf.getSquareSum()));
                for (double d : cf.getCentroid()) {
                    data.add(Double.toString(d));
                }
            } catch (Exception ex) {
                ex.printStackTrace();
            }
            return data;
        }

        public boolean isTreeNode(BirchNode n) {
            return n instanceof BirchNodeCFTree;
        }
    }

    public static class DotVisitor extends BirchNode.BirchVisitor {
        protected DotGraph graph;
        protected LinkedList<DotNode> stack = new LinkedList<>();
        protected int childIndex;

        public DotGraph run(BirchNode node) {
            initGraph();
            node.accept(this);
            return graph;
        }

        protected void initGraph() {
            graph = new DotGraph();
            graph.getGraphAttrs().add(DotAttribute.graphRankDirectionLR());
        }

        @Override
        public boolean visitTree(BirchNodeCFTree t) {
            String cfStr = cfString(childIndex, t);
            add(String.format("%s[%d]", cfStr, t.getChildSize()), true, false);
            return true;
        }

        @Override
        public void visitTreeChild(BirchNodeCFTree tree, int i, BirchNodeCFTree.NodeHolder nodeHolder) {
            childIndex = i;
        }

        @Override
        public void visitAfterTree(BirchNodeCFTree t) {
            stack.pop();
        }

        @Override
        public boolean visitEntry(BirchNodeCFEntry e) {
            String cfStr = cfString(childIndex, e);
            add(cfStr, false, true);
            return true;
        }

        public String cfString(int index, BirchNode e) {
            /*
            dataCount += (e instanceof BirchNodeCFEntry) ? ((BirchNodeCFEntry)e).dataPointList.size() : 0;

            int majorityCls = -1;
            StringBuilder clsList = new StringBuilder("{");
            if(e instanceof BirchNodeCFEntry){
                HashMap<Integer, Integer> clsCount = new HashMap<>();
                for(BirchNodeInput d : ((BirchNodeCFEntry)e).dataPointList){
                    int cls = d.getLbl();
                    if(clsCount.containsKey(cls)){
                        clsCount.put(cls, clsCount.get(cls) + 1);
                    }else{
                        clsCount.put(cls, 1);
                    }
                }

                int max = 0;
                for(Integer cls : clsCount.keySet()){
                    clsList.append(cls).append("(").append( clsCount.get(cls)).append("), ");
                    if(max < clsCount.get(cls)){
                        max = clsCount.get(cls);
                        majorityCls = cls;
                    }
                }
            }
            clsList.append("}");

            ClusteringFeature cf = e.getClusteringFeature();
            return String.format("[%d] CF[N=%d, LS={%s}, SS=%.4f]\nC={%s}\nentries:%d\nclass:%s\nchildSize:%d\ndataPointSize:%d\n%s\nmajorityCls:%d\nclsList:%s",
                    index,
                    cf.getEntries(),
                    toStr(cf.getLinearSum()),
                    cf.getSquareSum(),
                    toStr(cf.getCentroid()),
                    cf.entries,
                    e.getClass().getSimpleName(),
                    (e instanceof BirchNodeCFEntry) ? 0 : e.getChildSize(),
                    (e instanceof BirchNodeCFEntry) ? ((BirchNodeCFEntry)e).dataPointList.size() : -1,
                    "parent: " + ((e.getParent() != null) ? e.getParent().getClass().getSimpleName() : "FALSE"),
                    majorityCls,
                    clsList.toString());
             */
            ClusteringFeature cf = e.getClusteringFeature();
            return String.format("[%d] CF[N=%d, LS={%s}, SS=%.4f]\nC={%s}",
                    index,
                    cf.getEntries(),
                    toStr(cf.getLinearSum()),
                    cf.getSquareSum(),
                    toStr(cf.getCentroid()));
        }


        public String toStr(double[] data) {
            return Arrays.stream(data)
                    .mapToObj(v -> String.format("%.4f", v))
                    .collect(Collectors.joining(", "));
        }

        protected void add(String label, boolean push, boolean box) {
            DotNode parentDot = stack.peek();
            DotNode nodeDot = graph.add(label);
            if (parentDot != null) {
                graph.addEdge(parentDot, nodeDot);
            }
            if (box) {
                nodeDot.getAttrs().add(DotAttribute.shapeBox());
            }
            if (push) {
                stack.push(nodeDot);
            }
        }
    }

    public static class SaveSingleFileVisitor extends BirchNode.BirchVisitor {
        protected String file;
        protected LinkedList<Long> nodeStack = new LinkedList<>();

        protected CsvWriter entryWriter; //childIndex1,childIndex2,childIndex3,...,"L"entrySize,entryIndex1,entryIndex2,...,entryIndex_entrySize
        protected CsvWriter nodeCfWriter; //childIndex1,...,"CF",CF.entries,CF.squareSum,CF.linearSum[0],CF.linearSum[1],... : file-cf.csv
        protected long entryCount;
        protected long nodeCount;

        public void run(BirchNode e, Path file) {
            try {
                start(file);
                e.accept(this);
            } finally {
                end();
            }
        }

        public void start(Path filePath) {
            this.file = filePath.toString();
            Path dirPath = filePath.getParent();
            if (dirPath != null && !dirPath.toString().isEmpty() && !Files.exists(dirPath)) {
                try {
                    Files.createDirectories(dirPath);
                } catch (Exception ex) {
                    throw new RuntimeException(ex);
                }
            }
            entryWriter = new CsvWriter().withOutputFile(filePath);
            System.err.println("Entry-file: " + filePath);
            openNodeCfWriter(filePath);
        }

        public void end() {
            System.err.println(String.format("%,d entries, %,d nodes", entryCount, nodeCount));
            if (entryWriter != null) {
                this.entryWriter.close();
            }
            if (nodeCfWriter != null) {
                nodeCfWriter.close();
            }
        }

        protected void openNodeCfWriter(Path filePath) {
            Path dirPath = filePath.getParent();
            String name = filePath.getFileName().toString();
            int dot = name.lastIndexOf(".");
            String prefix = name;
            String suffix = "";
            if (dot > 0) {
                prefix = name.substring(0, dot);
                suffix = name.substring(dot);
            }
            String cfName = prefix + "-cf" + suffix;
            Path cfPath = dirPath == null ? Paths.get(cfName) : dirPath.resolve(cfName);
            nodeCfWriter = new CsvWriter().withOutputFile(cfPath);
            System.err.println("Node-CF-file: " + cfPath);
        }

        @Override
        public boolean visitEntry(BirchNodeCFEntry e) {
            ++entryCount;
            LongList ll = e.indexes;
            int ls = ll.size();
            ArrayList<String> line = new ArrayList<>(ls + nodeStack.size() + 1);

            nodeStack.forEach(n ->
                    line.add(Long.toString(n)));
            line.add("L" + ls);
            for (int i = 0; i < ls; ++i) {
                long n = ll.get(i);
                line.add(Long.toString(n));
            }

            entryWriter.writeLine(line);
            writeNodeCf(e.getClusteringFeature());
            return true;
        }

        @Override
        public boolean visitTree(BirchNodeCFTree t) {
            writeNodeCf(t.getClusteringFeature());
            ++nodeCount;
            nodeStack.addLast(-1L); //start: if the node has a child, then there is a visitTreeChild
            return true;
        }

        public void writeNodeCf(ClusteringFeature cf) {
            if (cf == null) {
                return;
            }
            int lsLen = cf.getLinearSum().length;
            ArrayList<String> line = new ArrayList<>(nodeStack.size() + 3 + lsLen);

            nodeStack.forEach(n ->
                    line.add(Long.toString(n)));

            line.add("CF");
            line.add(Integer.toString(cf.getEntries()));
            line.add(Double.toString(cf.getSquareSum()));
            for (double d : cf.getLinearSum()) {
                line.add(Double.toString(d));
            }

            nodeCfWriter.writeLine(line);
        }

        @Override
        public void visitTreeChild(BirchNodeCFTree tree, int i, BirchNodeCFTree.NodeHolder nodeHolder) {
            nodeStack.removeLast();
            nodeStack.addLast((long) i); //next child
        }

        @Override
        public void visitAfterTree(BirchNodeCFTree t) {
            nodeStack.removeLast();
        }
    }

}
