package csl.dataproc.birch;

import csl.dataproc.csv.CsvLine;
import csl.dataproc.csv.CsvReader;
import csl.dataproc.csv.CsvStat;
import csl.dataproc.csv.CsvWriter;
import csl.dataproc.vfdt.count.LongCount;

import java.io.Closeable;
import java.io.IOException;
import java.io.Serializable;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.function.Consumer;
import java.util.stream.Collectors;

public class DendrogramPurityUnit implements Closeable {
    protected BirchNode root;
    protected Instant startTime;
    protected Instant prevTime;
    protected long logPeriod = 10;
    protected PurityInput input;

    protected Map<Integer, List<DataPoint>> clsToInputData = new HashMap<>();
    protected Map<Long, SubTreeTable> idToTable;
    protected double pairSize = 0;
    protected long totalDataPoints;

    protected int classSize = 1024;

    protected long noLabelData = 0;

    protected ExecutorService classService;
    protected ExecutorService entryService;

    protected Set<Integer> selectedClasses = null;

    protected PurityLogger logger;

    public DendrogramPurityUnit(PurityInput input) {
        this(input, System.out::println);
    }

    public DendrogramPurityUnit(PurityInput input, PurityLogger logger) {
        this.input = input;
        startTime = Instant.now();
        prevTime = startTime;
        this.logger = logger;
        input.setLogger(logger);
        input.init();
    }

    public interface PurityLogger {
        void log(String line);
        default void close() {}
    }

    public interface PurityInput {
        void init();
        int getDataPointClass(long dataPointIndex);
        default void close() throws IOException { }
        void setLogger(PurityLogger logger);
    }

    public static class PurityInputSingleFile implements PurityInput {
        protected String file;
        protected BirchBatch.DataField classLabel;
        protected CsvReader reader;
        protected PurityLogger logger = System.err::println;

        public PurityInputSingleFile(String file, BirchBatch.DataField classLabel) {
            this.file = file;
            this.classLabel = classLabel;
        }

        public void init() {
            initReader();
        }

        public void setLogger(PurityLogger logger) {
            this.logger = logger;
        }

        @Override
        public int getDataPointClass(long dataPointIndex) {
            initReader();
            return getDataPointClass(reader.readNext(dataPointIndex));
        }

        protected void initReader() {
            if (reader == null) {
                reader = CsvReader.get(Paths.get(file));
            }
        }

        public int getDataPointClass(CsvLine line) {
            return (int) ((long) classLabel.extract(line));
        }

        @Override
        public void close() {
            if (reader != null) {
                reader.close();
                reader = null;
            }
        }

        public Set<Integer> loadSelectedClasses(String inputClassCountFile, int n, long seed) {
            Set<Integer> cls = new HashSet<>(n);
            LongCount total = loadClassCountsFromInputFile(inputClassCountFile);
            if (n > 0) {
                total = sampleClassCounts(total, n, seed);
            }
            int index = 0;
            List<long[]> selectedList = new ArrayList<>(); //for log
            for (LongCount.LongKeyValueIterator i = total.iterator();
                 i.hasNext(); ) {
                int k = (int) i.getKey();
                cls.add(k);
                long count = i.nextValue();
                selectedList.add(new long[] {k, count});

                ++index;
            }

            selectedList.sort(Comparator.comparingLong(e -> e[1]));
            long totalCount = 0;
            long median = 0;
            for (long[] e : selectedList) {
                if (index < 10 ||
                        ((n / 2 - 10) < index && index < (n / 2 + 10)) ||
                        n - 10 < index) {
                    logger.log(String.format("#selected-class[%,d]: %,32d  count:%,d", index, e[0], e[1]));
                }
                totalCount += e[1];
            }
            int medianIdx = selectedList.size() / 2;
            if (medianIdx < selectedList.size()) {
                median = selectedList.get(medianIdx)[1];
            }
            logger.log(String.format("#selected-classes %,d: totalCount=%,d median=%,d average=%,.3f",
                    selectedList.size(), totalCount, median, (totalCount / (double) selectedList.size())));

            return cls;
        }

        public LongCount loadClassCountsFromInputFile(String inputClassCountFile) {
            if (inputClassCountFile == null || !Files.exists(Paths.get(inputClassCountFile))) {
                LongCount count = loadClassCountsFromFromInputFile();
                if (inputClassCountFile != null) { //saving
                    try (CsvWriter w = new CsvWriter().withOutputFile(Paths.get(inputClassCountFile))) {
                        for (LongCount.LongKeyValueIterator i = count.iterator(); i.hasNext(); ) {
                            long k = i.getKey();
                            long v = i.nextValue();
                            w.writeLine(Long.toString(k),Long.toString(v));
                        }
                    }
                }
                return count;
            } else {
                LongCount count = new LongCount(1024);
                try (CsvReader r = CsvReader.get(Paths.get(inputClassCountFile))) {
                    for (CsvLine line : r) {
                        long k = line.getLong(0);
                        long v = line.getLong(1);
                        count.add(k, v);
                    }
                }
                return count;
            }
        }

        protected LongCount loadClassCountsFromFromInputFile() {
            LongCount count = new LongCount(1024);
            try (CsvReader r = CsvReader.get(Paths.get(file))) {
                for (CsvLine line : r) {
                    int k = getDataPointClass(line);
                    count.increment(k);
                }
            }
            return count;
        }


        public LongCount sampleClassCounts(LongCount totalClassCounts, int n, long seed) {
            List<Long> keys = new ArrayList<>();
            long total = 0;
            long size = 0;
            for (LongCount.LongKeyValueIterator i = totalClassCounts.iterator(); i.hasNext(); ) {
                long k = i.getKey();
                total += i.nextValue();
                keys.add(k);
                size++;
            }
            long failureMax = Math.max(total / 1000L, 10);
            long leastCount = Math.max(total / size, 2);

            Random random = new Random(seed);
            List<Long> selected = new ArrayList<>(n);
            long failure = 0;
            while (selected.size() < n && !keys.isEmpty()) {
                int next = random.nextInt(keys.size());
                long v = totalClassCounts.get(next);
                if (v >= leastCount) {
                    selected.add(keys.remove(next));
                } else if (failure > failureMax) {
                    leastCount = 0;
                    failure = 0;
                } else {
                    failure++;
                }
            }
            LongCount count = new LongCount(n);
            for (long k : selected) {
                count.add(k, totalClassCounts.get(k));
            }
            return count;
        }
    }

    public static class PurityInputSingleFileOnMemoryClass extends PurityInputSingleFile {
        LongCount pointToClass;
        LongCount classCount;

        public PurityInputSingleFileOnMemoryClass(String file, BirchBatch.DataField classLabel) {
            super(file, classLabel);
        }

        public void init() {
            initReader();
            logger.log("start purity point to class loading");
            Instant startTime = Instant.now();
            long count = 0;
            long lastPos = 0;
            LongCount pointToClass = new LongCount(10_000); //80MB
            LongCount classCount = new LongCount(10_000);  //80MB
            for (CsvLine line : reader) {
                long p = line.getStart();
                long k = getDataPointClass(line);
                pointToClass.add(p, k);
                classCount.increment(k);
                if ((count % 100_000) == 0) {
                    logStatus("loading", startTime, count, p);
                }
                ++count;
                lastPos = p;
            }
            logStatus("finish", startTime, count, lastPos);
            this.pointToClass = pointToClass;
            this.classCount = classCount;
            close();
        }

        private void logStatus(String msg, Instant startTime, long count, long pos) {
            Duration el = Duration.between(startTime, Instant.now());
            Runtime rt = Runtime.getRuntime();
            long free = rt.freeMemory() / 1000_000L;
            long total = rt.totalMemory() / 1000_000L;
            logger.log(String.format("time=%s, count=%,d, pos=%,d, totalMemMB=%,d, freeMemMB=%,d : %s", el, count, pos, total, free, msg));
        }

        @Override
        protected LongCount loadClassCountsFromFromInputFile() {
            if (classCount != null) {
                return classCount;
            }
            return super.loadClassCountsFromFromInputFile();
        }

        @Override
        public int getDataPointClass(long dataPointIndex) {
            return (int) pointToClass.get(dataPointIndex);
        }
    }

    public void setClassSize(int classSize) {
        this.classSize = classSize;
    }

    public int getClassSize() {
        return classSize;
    }

    public void setSelectedClasses(Set<Integer> selectedClasses) {
        this.selectedClasses = selectedClasses;
    }

    public Set<Integer> getSelectedClasses() {
        return selectedClasses;
    }

    public static class DataPoint implements  Serializable {
        //public long id; //reduce memory usage
        public int cls;
        public long parent;

        public DataPoint() {}

        public DataPoint(long id, int cls, long parent) {
            //this.id = id;
            this.cls = cls;
            this.parent = parent;
        }

        @Override
        public String toString() {
            return "DataPoint(" /*+ id + "," + */ + "k=" + cls + ",parent=" + parent + ")";
        }
    }

    public static class SubTreeTable implements Serializable {
        protected long parent;
        protected long id;

        protected long allDataPointSize;
        protected LongCount clsDataPointSize;
        protected List<DataPoint> data;

        public SubTreeTable() {}

        public SubTreeTable(long parent, long id, int classSize) {
            this.parent = parent;
            this.id = id;
            this.allDataPointSize = 0;
            this.clsDataPointSize = new LongCount(classSize);
        }

        public long getAllDataPointSize() {
            return allDataPointSize;
        }

        public LongCount getClsDataPointSize() {
            return clsDataPointSize;
        }

        public void addAllDataPointSize(long n) {
            allDataPointSize += n;
        }

        public void addClsDataPointSize(int cls, long n) {
            clsDataPointSize.add(cls, n);
        }

        public List<DataPoint> getData() {
            if (data == null) {
                return Collections.emptyList();
            }
            return data;
        }

        public void setData(List<DataPoint> data) {
            this.data = data;
            for (DataPoint p : data) {
                addClsDataPointSize(p.cls, 1);
            }
        }

        public void setAllDataPointSize(long allDataPointSize) {
            this.allDataPointSize = allDataPointSize;
        }

        public long getId() {
            return id;
        }

        public long getParent() {
            return parent;
        }

        public void addSubNode(SubTreeTable t) {
            addAllDataPointSize(t.getAllDataPointSize());
            for (LongCount.LongKeyValueIterator iter = t.getClsDataPointSize().iterator();
                 iter.hasNext(); ) {
                int k = (int) iter.getKey();
                long v = iter.nextValue();
                addClsDataPointSize(k, v);
            }
        }
    }

    public double calculatePurity(BirchNode root) {
        init(root);
        constructTable(root);
        setPairSize();

        log(String.format("# pairSize: %,.0f,  nodes: %,d, allData: %,d, selectedClassesData: %,d",
                pairSize,
                getAllNodes().size(),
                getRoot().getAllDataPointSize(),
                getRoot().getClsDataPointSize().total()));
        check();

        try {
            double p = calculatePurityForClasses();
            log(String.format("purity: %.4f", p));
            return p;
        } finally {
            finish();
        }
    }


    public void init(BirchNode root) {
        int n = Runtime.getRuntime().availableProcessors() / 2 + 1;
        classService = Executors.newFixedThreadPool(n);
        entryService = Executors.newFixedThreadPool(n);
        log("#start DendrogramPurity calculation...");
        this.root = root;
    }

    public void finish() {
        log(String.format("#test time: %s", CsvStat.toStr(Duration.between(startTime, Instant.now()))));
        entryService.shutdown();
        classService.shutdown();
    }

    /////////////////////////////////

    public void constructTable(BirchNode root) {
        this.idToTable = new HashMap<>();
        root.accept(new BirchVisitorTableConstruction(new TableConstructionHost() {
            @Override
            public void put(long id, SubTreeTable table) {
                DendrogramPurityUnit.this.constructTableNode(id, table);
            }

            @Override
            public List<DataPoint> load(long id, BirchNodeCFEntry e) {
                return DendrogramPurityUnit.this.constructDataPoints(id, e);
            }

            @Override
            public void finishNode(long id) {
                DendrogramPurityUnit.this.constructFinishNode(id);
            }
        }, classSize, selectedClasses));
        log(String.format("constructTable finish : totalDataPoints=%,d", totalDataPoints));
    }

    public interface TableConstructionHost {
        void put(long id, SubTreeTable table);
        List<DataPoint> load(long id, BirchNodeCFEntry e);
        void finishNode(long id);
    }

    public static class BirchVisitorTableConstruction extends BirchNode.BirchVisitor {
        protected TableConstructionHost host;
        protected int classSize;
        protected long index;
        protected LinkedList<Long> stack;
        protected Set<Integer> selectedClasses;

        public BirchVisitorTableConstruction(TableConstructionHost host, int classSize, Set<Integer> selectedClasses) {
            this.host = host;
            this.classSize = classSize;
            stack = new LinkedList<>();
            this.selectedClasses = selectedClasses;
        }

        /** call once for each node */
        public long nextId(BirchNode n) {
            long i = index;
            ++index;
            return i;
        }

        public long parent() {
            return stack.isEmpty() ? -1L : stack.getLast();
        }

        @Override
        public boolean visitEntry(BirchNodeCFEntry e) {
            SubTreeTable table = new SubTreeTable(parent(), nextId(e), classSize);
            table.setAllDataPointSize(e.getIndexes().size());

            List<DataPoint> rawPoints = host.load(table.getId(), e);
            if (selectedClasses != null && !selectedClasses.isEmpty()) {
                rawPoints = rawPoints.stream()
                        .filter(p -> selectedClasses.contains(p.cls))
                        .collect(Collectors.toList());
            }
            table.setData(rawPoints);

            host.put(table.getId(), table);
            host.finishNode(table.getId());
            return true;
        }

        @Override
        public boolean visitTree(BirchNodeCFTree t) {
            SubTreeTable table = new SubTreeTable(parent(), nextId(t), classSize);
            host.put(table.getId(), table);
            stack.addLast(table.getId());
            return true;
        }

        @Override
        public void visitAfterTree(BirchNodeCFTree t) {
            host.finishNode(stack.removeLast());
        }
    }

    public void constructTableNode(long id, SubTreeTable table) {
        idToTable.put(id, table);

        for (DataPoint p : table.getData()) { //for upper nodes, getData will return empty
            clsToInputData.computeIfAbsent(p.cls, c -> new ArrayList<>())
                    .add(p);
            ++totalDataPoints;
        }
        logPeriod(String.format("constructTableNode: totalDataPoints=%,d, nodes=%,d", totalDataPoints, idToTable.size()));
    }

    public void constructFinishNode(long id) {
        SubTreeTable table = idToTable.get(id);
        if (table.parent != -1) {
            idToTable.get(table.parent).addSubNode(table);
        }
    }

    public List<DataPoint> constructDataPoints(long eId, BirchNodeCFEntry e) {
        LongList l = e.getIndexes();
        List<DataPoint> dps = new ArrayList<>(l.size());
        for (int i = 0, s = l.size(); i < s; ++i) {
            long offset = l.get(i);
            int k = input.getDataPointClass(offset);
            if (k != 0) {
                dps.add(new DataPoint(offset, k, eId));
            } else {
                noLabelData++;
            }
        }
        return dps;
    }

    /////////////////////////////////

    public void setPairSize() {
        //total number of pairs
        for (List<DataPoint> e : clsToInputData.values()) {
            long s = e.size();
            pairSize += (s * (s - 1.0)) / 2.0;
        }
    }

    public SubTreeTable getRoot() {
        return idToTable.get(0L);
    }

    public void check() {
        if (noLabelData > 0) {
            log(String.format("invalid no-labelled-data-points: %,d", noLabelData));
        }
        //total data size
        long rootTotal = getRoot().getAllDataPointSize();
        if (totalDataPoints != rootTotal) {
            log(String.format("invalid map size: clsToInputData %,d vs root-table %,d", totalDataPoints, rootTotal));
        }

        //k data
        clsToInputData.forEach((k, v) -> {
            long rootSize = getRoot().getClsDataPointSize().get(k);
            if (rootSize != v.size()) {
                log(String.format("invalid class %d: clsToInputData %,d vs root-table %,d", k,
                        v.size(), rootSize));
            }
        });
    }

    public Collection<SubTreeTable> getAllNodes() {
        return idToTable.values();
    }

    public SubTreeTable getNode(long id) {
        return idToTable.get(id);
    }

    static DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    public void log(String s) {
        Instant now = Instant.now();
        logger.log(String.format("[%s: %s] %s",
                formatter.format(OffsetDateTime.ofInstant(now, ZoneId.systemDefault())), Duration.between(startTime, now), s));
    }

    public void logPeriod(String str) {
        logPeriod(str, logPeriod);
    }

    public synchronized void logPeriod(String str, long limitSeconds) {
        Instant now = Instant.now();
        Duration d = Duration.between(prevTime, now);
        if (d.getSeconds() >= limitSeconds) {
            prevTime = now;
            d = Duration.between(startTime, now);
            logger.log(String.format("[%s: %s: +%ds] %s",
                    formatter.format(OffsetDateTime.ofInstant(now, ZoneId.systemDefault())), d, limitSeconds, str));
        }
    }

    /////////////////////////////////////

    public double calculatePurityForClasses() {
        //suppose entries are const
        return clsToInputData.entrySet().stream()
                .map(e -> classService.submit(() -> calculatePurityForEntry(e.getKey(), e.getValue())))
                .mapToDouble(DendrogramPurityUnit::calculatePurityFutureDouble)
                .sum();
    }

    public static double calculatePurityFutureDouble(Future<Double> d) {
        try {
            return d.get();
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    public double calculatePurityForEntry(int k, List<DataPoint> data) {
        int classDataSize = data.size();

        log(String.format("k: %,d,  data.size: %,d", k, classDataSize));

        List<Future<Double>> rests = new ArrayList<>(classDataSize);
        PurityProgress progress = new PurityProgress(k, classDataSize * (classDataSize -1L) / 2L);

        for (int i = 0; i < classDataSize; ++i) {
            final int x = i;
            rests.add(entryService.submit(() -> calculatePurityForEntryRest(k, data, x, progress)));
        }
        double purity = rests.stream()
                .mapToDouble(DendrogramPurityUnit::calculatePurityFutureDouble)
                .sum();

        log(progress.format(purity));
        return purity;
    }


    public double calculatePurityForEntryRest(int k, List<DataPoint> data, int i,
                                              PurityProgress progress) {
        int classDataSize = data.size();
        double purity = 0;
        DataPoint dataI = data.get(i);

        long countI = 0;
        for (int j = i + 1; j < classDataSize; ++j) {
            DataPoint dataJ = data.get(j);
            SubTreeTable lca = calculatePurityLeastCommonAncestor(dataI, dataJ);
            if (lca == null) {
                log(String.format("# null error: %,d: %s, %,d: %s,  LCA:null",
                        i, nodes(dataI), j, nodes(dataJ)));
                throw new RuntimeException("error k=" + k + " i=" + i + " j=" + j);
            }
            double pur = ((double) lca.getClsDataPointSize().get(k))
                        / ((double) lca.getAllDataPointSize());

            purity += pur / pairSize;

            ++countI;

            if (countI >= 10_000L) {
                if (progress.next(countI)) {
                    log(progress.format(purity));
                }
                countI = 0;
            }
        }
        if (progress.next(countI)) {
            log(progress.format(purity));
        }
        return purity;
    }


    public SubTreeTable calculatePurityLeastCommonAncestor(DataPoint dataI, DataPoint dataJ) {
        long i = dataI.parent;
        long j = dataJ.parent;
        while (i != j) {
            i = getNode(i).getParent();
            j = getNode(j).getParent();
        }
        return getNode(i);
    }

    public List<String> nodes(DataPoint n) {
        List<String> ns = nodes(n.parent);
        ns.add("L(" + "k=" + n.cls + ")");
        return ns;
    }

    private List<String> nodes(long nid) {
        if (nid == -1L) {
            return new ArrayList<>();
        } else {
            SubTreeTable table = getNode(nid);
            List<String> ns = nodes(table.getParent());
            ns.add("[" + table.getId() + "]");
            return ns;
        }
    }

    public void close() throws IOException {
        input.close();
        logger.close();
    }

    public static class PurityProgress {
        public int k;
        public long totalSteps;
        public Instant startTime;
        public long count;
        public Instant prevPrintTime;
        public Duration lapDuration;

        public PurityProgress(int k, long totalSteps) {
            this.k = k;
            startTime = Instant.now();
            prevPrintTime = startTime;
            this.totalSteps = totalSteps;
            lapDuration = Duration.ZERO;
        }

        public synchronized boolean next(long countI) {
            count += countI;
            Instant now = Instant.now();
            Duration diff = Duration.between(prevPrintTime, now);
            if (diff.getSeconds() > 20) {
                prevPrintTime = now;
                return true;
            } else {
                return false;
            }
        }

        public String format(double purity) {
            return String.format("--- @%4d : %2.2f%% in %15s \t( tmpPurity: %f )",
                    k, ((count / (double) totalSteps) * 100.0),
                    CsvStat.toStr(Duration.between(startTime, Instant.now())),
                    purity);
        }
    }
}


