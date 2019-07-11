package csl.dataproc.birch;

import csl.dataproc.csv.CsvIO;
import csl.dataproc.csv.CsvLine;

import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.stream.Stream;

public class BirchStatInput {
    public static void main(String[] args) {
        new BirchStatInput().run(args);
    }

    BatchExperiment batch;
    Instant startTime;
    long count;
    BirchNodeInput last;

    long lastDistanceCount = -1;
    Instant lastDistanceTime = Instant.EPOCH;
    Duration distanceTime;
    long distanceCount = -1;
    boolean showRangeStats = true;
    Set<Integer> targetClass = new HashSet<>();

    public void run(String[] args) {
        startTime = Instant.now();
        batch = new BatchExperiment();
        List<String> rest = batch.parseArgs(args);

        if (rest.contains("--help")) {
            System.out.println(
                    "--show-distance <N>s|<N>ms|<N>c : N seconds, N milli-seconds, or N count\n" +
                    "--no-range-stats\n" +
                    "-tc|--target-class <cls> : filter data-points by the class value (repeatable)");
            return;
        }

        List<String> targetClsValues = new ArrayList<>();
        for (int i = 0; i < rest.size(); ++i) {
            String arg = rest.get(i);
            if (arg.equals("--show-distance")) { //--show-distance Ns|Nms|Nc
                ++i;
                arg = rest.get(i);
                if (arg.endsWith("ms")) {
                    distanceTime = Duration.ofMillis(Long.valueOf(arg.substring(0, arg.length() - 2)));
                } else if (arg.endsWith("s")) {
                    distanceTime = Duration.ofSeconds(Long.valueOf(arg.substring(0, arg.length() - 1)));
                } else if (arg.endsWith("c")) {
                    distanceCount = Long.valueOf(arg.substring(0, arg.length() - 1));
                } else {
                    throw new RuntimeException("error: " + arg);
                }
            } else if (arg.equals("--no-range-stats")) {
                showRangeStats = false;
            } else if (arg.equals("-tc") || arg.equals("--target-class")) {
                ++i;
                targetClsValues.add(rest.get(i));
            }
        }

        for (String cls: targetClsValues) {
            int c = (int) batch.getClassLabel().extractFromValue(cls);
            System.out.println("#targetClass," + cls + "," + c);
            targetClass.add(c);
        }

        try (BirchBatch.InputI i = batch.getInputForRead()) {
            i.makeDataPointStream()
                .forEach(this::process);
            report((InputExperiment) i);
        }
    }

    public static class BatchExperiment extends BirchBatch {
        @Override
        public InputI getInputForRead() {
            return new InputExperiment(input, charset.name(), new CsvIO.CsvSettings(settings),
                    skipHeaders, fields, classLabel, config);
        }

        public DataField getClassLabel() {
            return classLabel;
        }
    }

    public static class InputExperiment extends BirchBatch.Input {
        public InputExperiment(String inputFile, String charsetName, CsvIO.CsvSettings settings, int skipHeaders,
                               List<BirchBatch.DataField> fields, BirchBatch.DataField classLabel, BirchConfig config) {
            super(inputFile, charsetName, settings, skipHeaders, fields, classLabel, config);
        }

        public Map<Integer, DataStat> stats = new LinkedHashMap<>();
        public Set<Double> classes = new HashSet<>();

        @Override
        public BirchNodeInput makeDataPoint(CsvLine line) {
            Stream<BirchBatch.DataField> dataCons;
            if (classLabel == null) {
                dataCons = fields.stream();
            } else {
                dataCons = Stream.concat(fields.stream(), Stream.of(classLabel));
            }

            if (classLabel != null) {
                classes.add(classLabel.extract(line));
            }

            BirchNodeInput input = config.newPoint(line.getStart(),
                    dataCons.mapToDouble(f -> extract(f, line))
                            .toArray());
            return input;
        }

        protected double extract(BirchBatch.DataField f, CsvLine line) {
            String str = line.get(f.index);

            if (f.generator instanceof BirchBatch.Extract) {
                BirchBatch.Extract el = (BirchBatch.Extract) f.generator;
                stats.computeIfAbsent(f.index,
                        i -> new DataStat(i, el))
                        .set(str);
            } else {
                stats.computeIfAbsent(f.index, DataStat::new).set(f.generator.apply(str));
            }
            return f.extract(line);
        }
    }

    public static class DataStat {
        public int index;
        public BirchBatch.Extract extract;

        public double max = Double.MIN_VALUE;
        public double min = Double.MAX_VALUE;

        public double exMax = Double.MIN_VALUE;
        public double exMin = Double.MAX_VALUE;

        public long minOverCount = 0;
        public long maxOverCount = 0;
        public double minSpec = Integer.MIN_VALUE;
        public double maxSpec = Integer.MAX_VALUE;
        public boolean allZero = true;

        public DataStat(int index) {
            this(index, null);
        }

        public DataStat(int index, BirchBatch.Extract extract) {
            this.index = index;
            this.extract = extract;
        }

        public void set(String str) {
            if (extract != null) {
                double v = BirchBatch.extract.extractWithoutClip(str); //original
                set(v);
                if (extract instanceof BirchBatch.ExtractLog) {
                    BirchBatch.ExtractLog log = (BirchBatch.ExtractLog) extract;
                    double lv = extract.extractWithoutClip(str);
                    setExMinMax(lv, log.min, log.max);
                } else if (extract instanceof BirchBatch.ExtractMinMax) {
                    BirchBatch.ExtractMinMax ex = (BirchBatch.ExtractMinMax) extract;
                    double lv = extract.extractWithoutClip(str);
                    setExMinMax(lv, ex.min, ex.max);
                } else if (extract instanceof BirchBatch.ExtractZ) {
                    BirchBatch.ExtractZ z = (BirchBatch.ExtractZ) extract;
                    double lv = extract.extractWithoutClip(str);
                    setExMinMax(lv, 0.0, z.std);
                    minSpec = z.mean;
                }
            }
        }

        public void setExMinMax(double lv, double min, double max) {
            if (lv > exMax) {
                exMax = lv;
            }
            if (lv < exMin) {
                exMin = lv;
            }
            if (lv < min) {
                minOverCount++;
            }
            if (lv > max) {
                maxOverCount++;
            }
            minSpec = min;
            maxSpec = max;
        }

        public void set(double d) {
            if (max < d) {
                max = d;
            }
            if (min > d) {
                min = d;
            }
            if (d != 0) {
                allZero = false;
            }
        }

        public static String getCsvHead() {
            return "#index,min,max,exMin,exMax,minOverCount,maxOverCount,exName,spec1,spec2";
        }

        public String toCsvLine() {
            return String.format("%d,%f,%f,%f,%f,%d,%d,%s,%f,%f",
                    index, min, max, (allZero ? 0 : exMin), (allZero ? 0 : exMax), minOverCount, maxOverCount,
                    (extract == null) ? "" : extract.getName(), minSpec, maxSpec);
        }

        @Override
        public String toString() {
            return String.format("index=%d, min=%,f, max=%,f, logMin=%,f, logMax=%,f, minOverCount=%,d, maxOverCount=%,d, exName=%s, minSpec=%,f, maxSpec=%,f",
                    index, min, max, (allZero ? 0 : exMin), (allZero ? 0 : exMax), minOverCount, maxOverCount,
                    (extract == null) ? "" : extract.getName(), minSpec, maxSpec);
        }
    }



    public void process(BirchNodeInput point) {
        if (!targetClass.isEmpty()) {
            if (point instanceof BirchNodeInput.BirchNodeInputLabeled) {
                int lbl = ((BirchNodeInput.BirchNodeInputLabeled) point).getLabel();
                if (!targetClass.contains(lbl)) {
                    return;
                }

            } else {
                return;
            }
        }

        ++count;
        Instant now = Instant.now();
        if (last != null &&
                (checkDistanceCount() ||
                 checkDistanceTime(now))) {
            ClusteringFeature cf1 = last.getClusteringFeature();
            ClusteringFeature cf2 = point.getClusteringFeature();
            double d0 = cf1.centroidEuclidDistance(cf2);
            double d1 = cf1.centroidManhattanDistance(cf2);
            double d2 = cf1.averageInterClusterDistance(cf2);
            double d3 = cf1.averageIntraClusterDistance(cf2);
            double d4 = cf1.varianceIncreaseDistance(cf2);
            lastDistanceTime = now;
            lastDistanceCount = count;
            int lastLabel = -1;
            int nextLabel = -1;
            if (last instanceof BirchNodeInput.BirchNodeInputLabeled) {
                lastLabel = ((BirchNodeInput.BirchNodeInputLabeled) last).getLabel();
            }
            if (point instanceof BirchNodeInput.BirchNodeInputLabeled) {
                nextLabel = ((BirchNodeInput.BirchNodeInputLabeled) point).getLabel();
            }
            System.out.println(String.format("%s,%d,%d,%d,%d,%d,%f,%f,%f,%f,%f,%f,%f,%f,%f",
                    now, count,
                    last.getId(), point.getId(), lastLabel, nextLabel,
                    Arrays.stream(cf1.getLinearSum()).sum(),
                    Arrays.stream(cf2.getLinearSum()).sum(),
                    cf1.getSquareSum(),
                    cf2.getSquareSum(),
                    d0, d1, d2, d3, d4));
        } else if (last == null && (distanceCount >= 0 || distanceTime != null)) {
            System.out.println("#time,count,last.id,next.id,last.label,next.label,last.lsum,next.lsum,last.ssum,next.ssum,d0,d1,d2,d3,d4");
        }
        last = point;
    }

    private boolean checkDistanceCount() {
        if (distanceCount < 0) {
            return false;
        } else {
            return distanceCount <= (count - lastDistanceCount);
        }
    }

    private boolean checkDistanceTime(Instant now) {
        if (distanceTime == null) {
            return false;
        } else {
            return (distanceTime.compareTo(Duration.between(lastDistanceTime, now)) <= 0);
        }
    }

    public void report(InputExperiment i) {
        if (showRangeStats) {
            System.out.println(String.format("#time=%s, count=%,d, classes=%,d",
                    Duration.between(startTime, Instant.now()), count, i.classes.size()));
            System.out.println(DataStat.getCsvHead());
            for (DataStat stat : i.stats.values()) {
                System.out.println(stat.toCsvLine());
            }
        }
    }
}
