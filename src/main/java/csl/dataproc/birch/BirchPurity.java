package csl.dataproc.birch;

import csl.dataproc.csv.CsvLine;
import csl.dataproc.csv.CsvReader;
import csl.dataproc.tgls.util.JsonReader;
import csl.dataproc.tgls.util.JsonWriter;

import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.function.Consumer;

public class BirchPurity {
    protected BirchBatch.DataField classLabel;

    protected String inputFile;
    protected String inputClassCountFile;
    protected String treeFile; //childIndex1,childIndex2,...Ln,dataIndex1,dataIndex2,...dataIndex_n

    protected String purityLogFile;
    protected boolean purityOnMemoryPointToClass = true; //if false, use default impl, using random-access by csvReader.readNext(pos)
    protected DendrogramPurityUnit purityUnit;

    protected long logPeriod = 10;
    protected Instant startTime = Instant.now();
    protected Instant prevTime = startTime;

    protected long loadedNodes;
    protected long loadedEntries;
    protected long duplicatedEntries;

    protected int classSize = 1024;
    protected int sampleClasses = 0;

    protected long seed = 0xCAFE_BABE;

    protected String outJson;

    public static void main(String[] args) {
        new BirchPurity().start(args);
    }

    public void start(String[] args) {
        List<String> rest = parseArgs(Arrays.asList(args));
        if (!rest.contains("--help")) {
            init();

            BirchNode root = loadNodes();
            double p = run(root);
            System.out.println(String.format("purity: %f", p));

            if (outJson != null) {
                injectJson(outJson, p);
            }
        } else {
            System.out.println(String.join("\n", getHelp()));
        }
    }

    public void init() {
        log(String.format("purity init: inputFile=%s, classLabel=%s", inputFile, classLabel));

        DendrogramPurityUnit.PurityInputSingleFile input;
        if (purityOnMemoryPointToClass) {
            input = new DendrogramPurityUnit.PurityInputSingleFileOnMemoryClass(inputFile, classLabel);
        } else {
            input = new DendrogramPurityUnit.PurityInputSingleFile(inputFile, classLabel);
        }
        purityUnit = initPurity(input);
        purityUnit.setClassSize(sampleClasses > 0 ? sampleClasses : classSize);

        purityUnit.setSelectedClasses(input.loadSelectedClasses(inputClassCountFile, sampleClasses, seed));
        if (sampleClasses > 0) {
            log(String.format("purity init: sample classes: %,d", purityUnit.getSelectedClasses().size()));
        }
    }

    protected DendrogramPurityUnit initPurity(DendrogramPurityUnit.PurityInput input) {
        DendrogramPurityUnit.PurityLogger logger;
        if (purityLogFile != null) {
            logger = new PurityLoggerFile(Paths.get(purityLogFile));
        } else {
            logger = System.out::println;
        }
        return new DendrogramPurityUnit(input, logger);
    }

    static class PurityLoggerFile implements DendrogramPurityUnit.PurityLogger {
        protected Path file;
        protected PrintStream out;
        public PurityLoggerFile(Path file) {
            this.file = file;
        }

        @Override
        public synchronized void log(String line) {
            if (out == null) {
                try {
                    out = new PrintStream(file.toFile());
                } catch (Exception ex) {
                    ex.printStackTrace();
                }
            }
            out.println(line);
        }

        @Override
        public void close() {
            if (out != null) {
                out.close();
            }
        }
    }

    public double run(BirchNode root) {
        try {
            return purityUnit.calculatePurity(root);
        } finally {
            try {
                purityUnit.close();
            } catch (IOException ex) {
                throw new RuntimeException(ex);
            }
        }
    }

    public List<String> parseArgs(List<String> args) {
        List<String> rest = new ArrayList<>();
        if (args.isEmpty()) {
            rest.add("--help");
            return rest;
        }
        for (int i = 0, l = args.size() ; i < l; ++i) {
            String arg = args.get(i);
            if (arg.equals("--input")) {
                ++i;
                inputFile = args.get(i);
            } else if (arg.equals("--input-class-count")) {
                ++i;
                inputClassCountFile = args.get(i);
            } else if (arg.equals("--tree")) {
                ++i;
                treeFile = args.get(i);
            } else if (arg.equals("--class") || arg.equals("-c")) {
                ++i;
                classLabel = new BirchBatch.DataField(Integer.valueOf(args.get(i)));
            } else if (arg.equals("--log-period")) {
                ++i;
                logPeriod = Long.valueOf(args.get(i));
            } else if (arg.equals("--seed")) {
                ++i;
                arg = args.get(i);
                try {
                    seed = Long.valueOf(arg);
                } catch (NumberFormatException nfe) {
                    seed = arg.hashCode();
                }
            } else if (arg.equals("--sample-classes")) {
                ++i;
                sampleClasses = Integer.valueOf(args.get(i));

            } else if (arg.equals("--classsize")) {
                ++i;
                arg = args.get(i);
                classSize = Integer.valueOf(arg);
            } else if (arg.equals("--out-json")) {
                ++i;
                outJson = args.get(i);
            } else if (arg.equals("--purity-log-out")) {
                ++i;
                purityLogFile = args.get(i);
            } else if (arg.equals("--purity-old")) {
                ++i;
                purityOnMemoryPointToClass = false;
            } else if (arg.equals("-h") || arg.equals("--help")) {
                rest.add("--help");
                break;
            } else {
                rest.add(arg);
            }
        }
        return rest;
    }

    @SuppressWarnings("unchecked")
    public void injectJson(String jsonFile, double purity) {
        Path jsonPath = Paths.get(jsonFile);
        if (Files.exists(jsonPath)) {
            log(String.format("inject to existing json: %s", jsonPath));
            try {
                Map<String, Object> json = (Map<String, Object>) JsonReader.read(jsonPath.toFile());
                json.put("purity", String.format("%.3f", purity));
                JsonWriter.write(json, jsonPath.toFile());
            } catch (Exception ex) {
                log("error: cannot read as json: " + jsonFile);
            }
        } else {
            log(String.format("save to json: %s", jsonPath));
            try {
                Map<String, Object> json = new HashMap<>();
                json.put("purity", String.format("%.3f", purity));
                JsonWriter.write(json, jsonPath.toFile());
            } catch (Exception ex) {
                log("error: write json: " + jsonFile);
            }
        }
    }

    public void setInputFile(String inputFile) {
        this.inputFile = inputFile;
    }

    public void setClassLabel(BirchBatch.DataField classLabel) {
        this.classLabel = classLabel;
    }

    public List<String> getHelp() {
        return Arrays.asList(
                "",
                "--input <input.csv> ",
                "--input-class-count <count.csv> : class count file: k,count. if it does not exist, it creates from the input",
                "--tree <tree.csv>   : saved tree file: childIndex1,childIndex2,...,Ln,data1,data2,...,data_n",
                "--class N           : class label: N-th column in input.csv",
                "--max-threads T     : thread size. default is 100.",
                "--sample-classes C  : max class size",
                "--out-json <file.json>: write or inject a \"purity\" entry to the file",
                "--seed str          ",
                "--log-period S",
                "--purity-log-out <file.txt> : save logs by DendrogramPurityUnit to the file for reducing log messages",
                "--purity-old: using old impl. of mapping point to class, based on random-access. currently, using on-memory table by sequential access");
    }

    public void setInputClassCountFile(String inputClassCountFile) {
        this.inputClassCountFile = inputClassCountFile;
    }

    public void setTreeFile(String treeFile) {
        this.treeFile = treeFile;
    }

    public void setPurityUnit(DendrogramPurityUnit purityUnit) {
        this.purityUnit = purityUnit;
    }

    public void setClassSize(int classSize) {
        this.classSize = classSize;
    }

    public void setSampleClasses(int sampleClasses) {
        this.sampleClasses = sampleClasses;
    }

    public void setSeed(long seed) {
        this.seed = seed;
    }

    public BirchBatch.DataField getClassLabel() {
        return classLabel;
    }

    public String getInputFile() {
        return inputFile;
    }

    public String getInputClassCountFile() {
        return inputClassCountFile;
    }

    public String getTreeFile() {
        return treeFile;
    }

    public DendrogramPurityUnit getPurityUnit() {
        return purityUnit;
    }

    public long getLogPeriod() {
        return logPeriod;
    }

    public Instant getStartTime() {
        return startTime;
    }

    public Instant getPrevTime() {
        return prevTime;
    }

    public long getLoadedNodes() {
        return loadedNodes;
    }

    public long getLoadedEntries() {
        return loadedEntries;
    }

    public int getClassSize() {
        return classSize;
    }

    public int getSampleClasses() {
        return sampleClasses;
    }

    public long getSeed() {
        return seed;
    }

    //////////////////////

    public BirchNode loadNodes() {
        BirchNodeCFTree root = new BirchNodeCFTree();
        ++loadedNodes;
        log(String.format("loadNodes: start treeFile=%s", treeFile));
        try (CsvReader r = CsvReader.get(Paths.get(treeFile))) {
            List<Integer> prevPath = Collections.emptyList();
            long lines = 0;
            long errors = 0;
            for (CsvLine line : r) {
                try {
                    List<Integer> path = new ArrayList<>();
                    List<Long> dataIndices = new ArrayList<>();

                    if (line.isEmptyLine() || line.isEnd()) {
                        continue;
                    }

                    loadPathAndDataPoints(line, path, dataIndices);
                    if (!prevPath.isEmpty() && prevPath.size() != path.size()) {
                        log(String.format("loadNodes: lines=%,d pathSize %d:%s -> %d:%s %s", lines, prevPath.size(), prevPath, path.size(), path, line.getData()));
                    }
                    prevPath = path;
                    BirchNode node = root;
                    if (!path.isEmpty()) {
                        try {
                            BirchNodeCFEntry e = loadEntry(node, path, dataIndices.size());
                            dataIndices.forEach(e.getIndexes()::add);
                        } catch (Exception ex) {
                            throw new RuntimeException(String.format("loaded nodes=%,d, entries=%,d: %s %s", loadedNodes, loadedEntries, path, node), ex);
                        }
                        logPeriod(String.format("loaded nodes=%,d, entries=%,d", loadedNodes, loadedEntries));
                    }
                } catch (Exception ex) {
                    if (errors < 1000) {
                        log(String.format("loadNodes: lines=%,d skip: errors=%,d %s", lines, errors, line));
                    } else {
                        logPeriod(String.format("loadNodes: lines=%,d skip: errors=%,d %s", lines, errors, line));
                    }
                    ++errors;
                }
                ++lines;
            }
        }
        log(String.format("finish loading %s: nodes=%,d, entries=%,d, duplicatedEntries=%,d", treeFile, loadedNodes, loadedEntries, duplicatedEntries));
        return root;
    }

    private void loadPathAndDataPoints(CsvLine line, List<Integer> path, List<Long> points) {
        boolean dataPoints = false;
        for (String data : line.getData()) {
            if (!dataPoints) {
                if (data.startsWith("L")) {
                    dataPoints = true;
                } else {
                    path.add(Integer.valueOf(data));
                }
            } else {
                points.add(Long.valueOf(data));
            }
        }
    }

    private BirchNodeCFEntry loadEntry(BirchNode tree, List<Integer> path, int capacity) {
        int s = path.size() - 1;
        BirchNode node = tree;
        for (int i = 0; i < s; ++i) {
            node = loadNode(node, path.get(i));
        }
        return loadEntry(node, path.get(s), capacity);
    }

    private BirchNode loadNode(BirchNode tree, int n) {
        if (n < tree.getChildSize()) {
            return tree.getChildren().get(n);
        } else {
            while (n >= tree.getChildSize()) {
                BirchNodeCFTree node = new BirchNodeCFTree();
                ((BirchNodeCFTree) tree).addChildWithoutAddingCf(node);
                ++loadedNodes;
            }
            return tree.getChildren().get(n);
        }
    }


    private BirchNodeCFEntry loadEntry(BirchNode tree, int n, int capacity) {
        if (n < tree.getChildSize()) {
            ++duplicatedEntries;
            return (BirchNodeCFEntry) tree.getChildren().get(n);
        } else {
            while (n >= tree.getChildSize()) {
                BirchNodeCFEntry e = new BirchNodeCFEntry(null, new LongList.ArrayLongList(capacity));
                ((BirchNodeCFTree) tree).addChildWithoutAddingCf(e);
                ++loadedEntries;
            }
            return (BirchNodeCFEntry) tree.getChildren().get(n);
        }
    }


    static DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    public void log(String str) {
        Instant now = Instant.now();
        System.out.println(String.format("[%s: %s] %s",
                formatter.format(OffsetDateTime.ofInstant(now, ZoneId.systemDefault())), Duration.between(startTime, now), str));
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
            System.out.println(String.format("[%s: %s: +%ds] %s",
                    formatter.format(OffsetDateTime.ofInstant(now, ZoneId.systemDefault())), d, limitSeconds, str));
        }
    }
}
