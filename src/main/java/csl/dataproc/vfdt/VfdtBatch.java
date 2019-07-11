package csl.dataproc.vfdt;

import csl.dataproc.csv.*;
import csl.dataproc.csv.arff.ArffAttribute;
import csl.dataproc.csv.arff.ArffAttributeType;
import csl.dataproc.csv.arff.ArffHeader;
import csl.dataproc.dot.DotGraph;
import csl.dataproc.tgls.util.ArraysAsList;
import csl.dataproc.tgls.util.JsonWriter;

import java.io.Serializable;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class VfdtBatch {

    public static void main(String[] args) {
        VfdtBatch batch = new VfdtBatch();
        List<String> rest = batch.parseArgs(ArraysAsList.get(args));
        if (!rest.contains("--help")) {
            if (!rest.isEmpty()) {
                System.err.println("remaining-args: " + rest);
            }
            batch.run();
        }
    }

    protected VfdtConfig config;
    protected String learnInput = null;
    protected String testInput = null;
    protected Path outputDir = null;
    protected Path dotFile;

    protected ArffHeader spec;
    protected List<ArffAttributeType> types;
    protected VfdtNode root;

    protected Map<Long,AnswerStat> statMap = new TreeMap<>();

    protected Duration learnTime;
    protected Duration testTime;
    protected Duration totalTime;
    protected long learnInstanceCount;
    protected long testInstanceCount;
    protected boolean experiment = false;

    public VfdtBatch(VfdtConfig config) {
        this.config = config;
    }

    public VfdtBatch() {
        this(new VfdtConfig());
    }


    /** "learners/vfdt/vfdt.c:_processArgs" */
    public List<String> parseArgs(List<String> args) {
        List<String> rest = new ArrayList<>();
        spec = new ArffHeader("spec", new ArrayList<>(), 0);

        ArffAttributeType clsType = null;
        for (int i = 0, l = args.size(); i < l; ++i) {
            String arg = args.get(i);
            if (arg.equals("-l") || arg.equals("--learn")) { //-l file
                ++i;
                learnInput = args.get(i);
            } else if (arg.equals("-t") || arg.equals("--test")) { //-t file
                ++i;
                testInput = args.get(i);
            } else if (arg.equals("-a") || arg.equals("--attr")) { //-a i|r|e <e1>,<e2>,...,<eN>
                ++i;
                arg = args.get(i);
                ArffAttributeType type = ArffAttributeType.SimpleAttributeType.Real;
                if (arg.equals("i")) {
                    type = ArffAttributeType.SimpleAttributeType.Integer;
                } else if (arg.equals("r")) {
                    type = ArffAttributeType.SimpleAttributeType.Real;
                } else if (arg.equals("e")) {
                    ++i;
                    arg = args.get(i);
                    type = new ArffAttributeType.EnumAttributeType(ArraysAsList.get(arg.split(Pattern.quote(","))));
                }
                spec.getAttributes().add(new ArffAttribute("a" + spec.getAttributes().size(), type));
            } else if (arg.equals("-as") || arg.equals("--attrs")) { //-as N i|r|e <e1>,<e2>,...
                ++i;
                arg = args.get(i);
                int repeat = Integer.valueOf(arg);
                ++i;
                arg = args.get(i);
                ArffAttributeType type = ArffAttributeType.SimpleAttributeType.Real;
                if (arg.equals("i")) {
                    type = ArffAttributeType.SimpleAttributeType.Integer;
                } else if (arg.equals("r")) {
                    type = ArffAttributeType.SimpleAttributeType.Real;
                } else if (arg.equals("e")) {
                    ++i;
                    arg = args.get(i);
                    type = new ArffAttributeType.EnumAttributeType(ArraysAsList.get(arg.split(Pattern.quote(","))));
                }
                for (int r = 0; r < repeat; ++r) {
                    spec.getAttributes().add(new ArffAttribute("a" + spec.getAttributes().size(), type));
                }
            } else if (arg.equals("-c") || arg.equals("--class")) { //-c <e1>,<e2>,...,<eN>
                ++i;
                arg = args.get(i);
                clsType = new ArffAttributeType.EnumAttributeType(ArraysAsList.get(arg.split(Pattern.quote(","))));
            } else if (arg.equals("--dot")) { //--dot file.dot
                ++i;
                dotFile = Paths.get(args.get(i));
            } else if (arg.equals("--output")) { //--output dir
                ++i;
                outputDir = Paths.get(args.get(i));
            } else if (arg.equals("--experiment")) {
                experiment = true;
            } else if (arg.equals("--help") || arg.equals("-h")) {
                showHelp();
                rest.add("--help");
                break;
            } else {
                rest.add(arg);
            }
        }
        if (clsType != null) {
            spec.getAttributes().add(new ArffAttribute("class", clsType));
        }

        rest = config.parseArgs(rest);

        if (args.isEmpty()) {
            showHelp();
            rest.add("--help");
        }

        return rest;
    }

    public void showHelp() {
        System.out.println(String.join("\n", getHelp()));
    }

    public List<String> getHelp() {
        List<String> list = new ArrayList<>();
        list.add(getClass().getName());
        list.add("  arguments:");
        list.add("    -l|--learn <file>   : input file (.arff or csv (require -a and -c))");
        list.add("    -t|--test <file>    : test file");
        list.add("    -a|--attr i|r|e ... : add an attribute type");
        list.add("               -a i:  discrete long");
        list.add("               -a r:  continuous double ");
        list.add("               -a e '<v1>,<v2>,...,<vN>':  discrete enumeration values");
        list.add("    -as|--attrs N i|r|e ... : add N attribute types");
        list.add("    -c '<v1>,<v2>,...,<vN>' : discrete class values");
        list.add("    --output <dir>  : stats output directory");
        list.add("    --dot <file>    :  dot output file");
        list.add("    ");
        list.addAll(config.getHelp());
        return list;
    }

    public VfdtConfig getConfig() {
        return config;
    }

    public void setConfig(VfdtConfig config) {
        this.config = config;
    }

    public ArffHeader getSpec() {
        return spec;
    }

    public void setSpec(ArffHeader spec) {
        this.spec = spec;
    }

    public String getLearnInput() {
        return learnInput;
    }

    public void setLearnInput(String learnInput) {
        this.learnInput = learnInput;
    }

    public String getTestInput() {
        return testInput;
    }

    public void setTestInput(String testInput) {
        this.testInput = testInput;
    }

    public Path getOutputDir() {
        return outputDir;
    }

    public void setOutputDir(Path outputDir) {
        this.outputDir = outputDir;
    }

    public Path getDotFile() {
        return dotFile;
    }

    public void setDotFile(Path dotFile) {
        this.dotFile = dotFile;
    }

    public List<ArffAttributeType> getTypes() {
        return types;
    }

    public void setTypes(List<ArffAttributeType> types) {
        this.types = types;
    }


    public Duration getLearnTime() {
        return learnTime;
    }

    public Duration getTestTime() {
        return testTime;
    }

    public Duration getTotalTime() {
        return totalTime;
    }

    public Map<Long, AnswerStat> getStatMap() {
        return statMap;
    }

    public VfdtNode getRoot() {
        return root;
    }


    public void run() {
        Instant start = Instant.now();
        try {
            learn();
            test();
        } finally {
            totalTime = Duration.between(start, Instant.now());
        }
        output();
    }

    public void output() {
        outputTime();
        if (dotFile != null) {
            saveDot(dotFile);
        }
        if (outputDir != null) {
            output(outputDir);
        }
    }

    public void outputTime() {
        System.out.println(
                "learn-time: " + CsvStat.toStr(learnTime) + "\n" +
                "total-time: " + CsvStat.toStr(totalTime) + "\n" +
                "test-time : " + CsvStat.toStr(testTime));
    }


    /** "learners/vfdt/vfdt.c:main" */
    public void learn() {
        learn(Paths.get(learnInput));
    }

    public void learn(Path path) {
        CsvReaderI<? extends CsvLine> input = open(path);
        if (types == null) {
            setTypesFromSpec();
        }
        learn(input);

        try {
            input.close();
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    public void setTypesFromSpec() {
        if (spec != null) {
            types = spec.getAttributes()
                    .stream()
                    .map(ArffAttribute::getType)
                    .collect(Collectors.toList());
        }
    }

    public void learn(Iterable<? extends CsvLine> input) {
        Instant start = Instant.now();
        try {

            if (root == null) {
                root = config.newRoot(types);
            }
            for (CsvLine line : input) {
                if (line.isEmptyLine() || line.isEnd()) {
                    continue;
                }
                VfdtNode.DataLine data = VfdtNode.parse(types, line);
                root = root.put(data);
                ++learnInstanceCount;
            }
        } finally {
            learnTime = Duration.between(start, Instant.now());
        }
    }

    public void test() {
        if (testInput != null) {
            Path path = Paths.get(testInput);
            test(path);
        }
    }

    public CsvReaderI<? extends CsvLine> open(Path path) {
        if (path.toString().endsWith(".arff")) {
            ArffReader reader = ArffReader.get(path);
            if (spec == null || spec.getAttributes().isEmpty()) {
                spec = reader.getHeader();
            }
            return reader;
        } else {
            return CsvReader.get(path);
        }
    }

    public void test(Path path) {
        Instant start = Instant.now();
        try {
            CsvReaderI<? extends CsvLine> input = open(path);
            if (types == null) {
                setTypesFromSpec();
            }

            List<String> enumValues = getClassEnumValues();

            for (CsvLine line : input) {
                if (line.isEmptyLine() || line.isEnd()) {
                    continue;
                }
                VfdtNode.DataLine data = VfdtNode.parse(types, line);

                long cls = root.downToLeaf(data).getClassValue();
                if (!experiment) {
                    testOutput(data, cls, enumValues);
                }
                testStat(data, cls);
            }

            try {
                input.close();
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        } finally {
            testTime = Duration.between(start, Instant.now());
        }
    }

    public List<String> getClassEnumValues() {
        int clsIndex = types.size() - 1;
        ArffAttributeType clsType = types.get(clsIndex);
        List<String> enumValues = null;
        if (clsType instanceof ArffAttributeType.EnumAttributeType) {
            enumValues = ((ArffAttributeType.EnumAttributeType) clsType).getValues();
        }
        return enumValues;
    }

    public void testOutput(VfdtNode.DataLine data, long cls, List<String> classEnumValues) {
        String clsVal = "";
        if (classEnumValues != null && cls >= 0 && cls < classEnumValues.size()) {
            clsVal = ":" + classEnumValues.get((int) cls);
        }
        System.out.println(data + " -> " + cls + clsVal);
    }

    public void testStat(VfdtNode.DataLine data, long cls) {
        long ans = data.getLong(types.size() - 1);
        statMap.computeIfAbsent(ans, AnswerStat::new).put(cls);
        ++testInstanceCount;
    }

    public static class AnswerStat implements Serializable {
        public long classValue;
        public long success;
        public long count;
        public Map<Long,Long> failClassToCount = new TreeMap<>();

        static final long serialVersionUID = 1L;

        public AnswerStat() {}

        public AnswerStat(long classValue) {
            this.classValue = classValue;
        }

        public void put(long exp) {
            ++count;
            if (classValue == exp) {
                ++success;
            } else {
                failClassToCount.compute(exp, (e, c) -> c == null ? 1L : c + 1);
            }
        }
    }

    public void saveDot(Path file) {
        toDot().save(file.toFile());
    }

    public DotGraph toDot() {
        return new VfdtVisitorDot().run(root);
    }

    public void output(Path dir) {
        if (!experiment) {
            new VfdtVisitorStat().run(dir, root);
        }
        outputAnswerStats(dir);
        outputTreeStats(root, dir);
    }

    public void outputAnswerStats(Path dir) {

        Set<Long> failClasses = new HashSet<>();
        for (AnswerStat v : statMap.values()) {
            failClasses.addAll(v.failClassToCount.keySet());
        }
        List<Long> failClassList = failClasses.stream()
                .sorted().collect(Collectors.toList());;

        Path statsFile = dir.resolve("test-stats.csv");
        try (CsvWriter writer = new CsvWriter().withOutputFile(statsFile)) {
            CsvLine head = new CsvLine();
            head.addColumns("answerClassValue", "count", "successCount", "successRate");
            failClassList.stream()
                    .map(e -> e + ":failCount")
                    .forEachOrdered(head::addColumns);
            writer.writeLine(head);
            for (AnswerStat s : statMap.values()) {
                CsvLine line = new CsvLine();
                double rate = ((double) s.success) / s.count;
                line.addColumns(String.valueOf(s.classValue), String.valueOf(s.count), String.valueOf(s.success), String.format("%.3f", rate));
                int colStart = line.size();
                for (Map.Entry<Long,Long> clsToCount: s.failClassToCount.entrySet()) {
                    int i = failClassList.indexOf(clsToCount.getKey());
                    line.setColumn(i + colStart, String.valueOf(clsToCount.getValue()));
                }
                writer.writeLine(line);
            }
        }
    }

    public void outputTreeStats(VfdtNode e, Path dir) {
        JsonWriter.write(getTreeStats(e), dir.resolve("tree-stats.json"));
    }

    public Map<String,Object> getTreeStats(VfdtNode e) {

        Map<String,Object> json = new HashMap<>();
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
        json.put("nodes", total);
        json.put("learnTime", CsvStat.toStr(learnTime));
        json.put("totalTime", CsvStat.toStr(totalTime));
        json.put("testTime", CsvStat.toStr(testTime));
        json.put("learnInstanceCount", learnInstanceCount);
        json.put("testInstanceCount", testInstanceCount);

        double totalSuccess = 0;
        double totalCount = 0;
        for (AnswerStat s : statMap.values()) {
            totalSuccess += s.success;
            totalCount += s.count;
        }
        double errorRate = 1.0 - ((double) totalSuccess / (double) totalCount);
        json.put("errorRate", String.format("%.3f", errorRate));
        return json;
    }

    public Map<Integer,Long> getTreeDepthToNodes(VfdtNode e) {
        return new CountDepthVisitor().run(e);
    }

    public static class CountDepthVisitor extends VfdtNode.NodeVisitor {
        protected Map<Integer,Long> depthToNodes = new HashMap<>();
        protected int depth = 0;

        public Map<Integer,Long> run(VfdtNode n) {
            depthToNodes.clear();
            n.accept(this);
            return depthToNodes;
        }

        public Map<Integer, Long> getDepthToNodes() {
            return depthToNodes;
        }

        @Override
        public boolean visit(VfdtNode node) {
            depthToNodes.compute(depth, (k,v) -> v == null ? 1L : (v + 1L));
            return true;
        }

        @Override
        public boolean visitSplit(VfdtNodeSplit node) {
            if (super.visitSplit(node)) {
                ++depth;
                return true;
            } else {
                return false;
            }
        }

        @Override
        public void visitAfterSplit(VfdtNodeSplit node) {
            --depth;
        }
    }

}
