package csl.dataproc.csv;

import csl.dataproc.csv.arff.ArffAttribute;
import csl.dataproc.csv.arff.ArffAttributeType;
import csl.dataproc.csv.arff.ArffHeader;
import csl.dataproc.tgls.util.ArraysAsList;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Serializable;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * read and report .arff or CSV files. "-" means reading from System.in
 */
public class CsvStat {
    public static void main(String[] args) {
        CsvStat stat = new CsvStat();
        if (!stat.parseArgs(ArraysAsList.get(args)).contains("--help")) {
            stat.start();
        } else {
            stat.getHelp().forEach(System.out::println);
        }
    }
    public static class CsvConfig implements Serializable {
        public char delimiter = ',';
        public Charset charset = StandardCharsets.UTF_8;
        public int skipHeaders = 0;
        public boolean forceArff = false;

        static final long serialVersionUID = 1L;

        public List<String> parseArgs(List<String> args) {
            List<String> rest = new ArrayList<>();
            if (args.isEmpty()) {
                rest.add("--help");
                return rest;
            }
            for (int i = 0, l = args.size(); i < l; ++i) {
                String s = args.get(i);
                if (s.equals("--delimiter")) { //--delimiter c
                    ++i;
                    s = args.get(i);
                    char n;
                    if (s.length() >= 2 && s.charAt(0) == '\\') {
                        n = s.charAt(1);
                        if (n == 'f') {
                            n = '\f';
                        } else if (n == 't') {
                            n = '\t';
                        } else if (n == 'r') {
                            n = '\r';
                        }
                    } else {
                        n = s.charAt(0);
                    }
                    delimiter = n;
                } else if (s.equals("--charset")) { //--charset cs
                    ++i;
                    s = args.get(i);
                    charset = Charset.forName(s);
                } else if (s.equals("--skip")) {
                    ++i;
                    s = args.get(i);
                    skipHeaders = Integer.valueOf(s);
                } else if (s.equals("--help") || s.equals("-h")) {
                    rest.add("--help");
                    break;
                } else if (s.equals("--arff")) {
                    forceArff = true;
                } else {
                    rest.add(s);
                }
            }
            return rest;
        }

        public List<String> getHelp() {
            List<String> list = new ArrayList<>();
            list.add("    --delimiter <c> : set delimiter of columns. default is ','. '\\t', '\\f' and '\\r' are allowed. ");
            list.add("    --charset <cs>  : set charset. default is UTF-8");
            list.add("    --skip <n>      : skip header n lines");
            list.add("    --arff          : force reading as .arff format");
            return list;
        }
    }

    protected List<String> inputs = new ArrayList<>();
    protected CsvConfig config = new CsvConfig();
    protected boolean reportColumnValues = true;
    protected boolean reportAttributes = true;
    protected boolean reportStats = true;
    protected boolean generateArffHeader = false;

    public List<String> parseArgs(List<String> args) {
        args = config.parseArgs(args);
        List<String> rest2 = new ArrayList<>();
        for (String arg : args) {
            if (arg.equals("--novalues")) {
                reportColumnValues = false;
            } else if (arg.equals("--noattributes")) {
                reportAttributes = false;
            } else if (arg.equals("--nostats")) {
                reportStats = false;
            } else if (arg.equals("--genarff")) {
                generateArffHeader = true;
            } else if (arg.equals("--help")) {
                rest2.add(arg);
                break;
            } else {
                inputs.add(arg);
            }
        }
        return rest2;
    }

    public List<String> getHelp() {
        List<String> list = new ArrayList<>();
        list.add(getClass().getName() + " <options> <file>...");
        list.add("  read CSV files and show statistics");
        list.add("");
        list.add("  options:");
        list.addAll(config.getHelp());
        list.add("    --novalues : skip reporting column values");
        list.add("    --noattributes : skip reporting @attribute lines");
        list.add("    --nostats  : skip reporting statistics (useful for --genarff)");
        list.add("    --genarff  : output .arff header");
        list.add("  <file> : read CSV from file. if file ends with .arff, read as arff file. '-' means stdin");
        return list;
    }

    public void start() {
        for (String input : inputs) {
            run(input);
        }
    }

    public void run(String file) {
        if (file.equals("-")) {
            if (config.forceArff) {
                runArff(System.in);
            } else {
                runCsv(System.in);
            }
        } else {
            Path path = Paths.get(file);

            if (config.forceArff || path.toString().endsWith(".arff")) {
                runArff(path);
            } else {
                runCsv(path);
            }
        }
    }

    public void runArff(Path file) {
        try (ArffReader reader = ArffReader.get(file, config.charset)) {
            source = file.toString();
            runArff(reader);
        }
    }
    public void runArff(InputStream in) {
        try (ArffReader reader = ArffReader.get(
                new BufferedReader(new InputStreamReader(in, config.charset)))) {
            source = "" + in;
            run(reader, null);
        }
    }

    public void runCsv(Path file) {
        try (CsvReader reader = CsvReader.get(file, config.charset).withDelimiter(config.delimiter)) {
            source = file.toString();
            run(reader, null);
        }
    }

    public void runCsv(InputStream in) {
        try (CsvReader reader = CsvReader.get(
                new BufferedReader(new InputStreamReader(in, config.charset))).withDelimiter(config.delimiter)) {
            source = "" + in;
            run(reader, null);
        }
    }


    protected String source;
    protected Instant startTime;
    protected Instant endTime;

    protected long lines;
    protected long emptyLines;
    protected int maxLineStringLength;
    protected List<ColumnStat> columnStats = new ArrayList<>();

    public static class ColumnStat implements Serializable {
        public long doubleData;
        public long intData;
        public long longData;
        public long stringData;
        public long emptyData;
        public long unknownData; //'?'
        public int maxStringLength;

        static final long serialVersionUID = 1L;

        public Map<String,Integer> valueCounts = new HashMap<>();

        public Number minNumber;
        public Number maxNumber;
    }

    protected int valueCountLimit = 10000;

    public void run(Iterable<? extends CsvLine> reader, ArffHeader header) {
        lines = 0;
        emptyLines = 0;
        maxLineStringLength = 0;
        columnStats.clear();
        startTime = Instant.now();
        for (CsvLine line : reader) {
            long lineNum = lines;
            ++lines;
            int col = 0;

            if (line.isEmptyLine()) {
                ++emptyLines;
            }

            maxLineStringLength = Math.max(maxLineStringLength, (int) (line.getEnd() - line.getStart()));

            if (lineNum < config.skipHeaders) {
                continue;
            }

            for (String data : line.getData()) {
                ColumnStat colStat;
                if (col < columnStats.size()) {
                    colStat = columnStats.get(col);
                } else {
                    colStat = new ColumnStat();
                    columnStats.add(colStat);
                }

                if (reportColumnValues) {
                    int valueCountSize = colStat.valueCounts.size();
                    colStat.valueCounts.compute(data, (v, count) ->
                            (count == null ?
                                    (valueCountSize < valueCountLimit ? Integer.valueOf(1) : null) :
                                    Integer.valueOf((count) + 1)));
                }

                colStat.maxStringLength = Math.max(colStat.maxStringLength, data.length());

                if (data.equals("?")) {
                    colStat.unknownData++;
                } else if (data.isEmpty()) {
                    colStat.emptyData++;
                } else {
                    try {
                        long i = Long.valueOf(data);
                        if (i >= Integer.MIN_VALUE && i <= Integer.MAX_VALUE) {
                            colStat.intData++;
                        } else {
                            colStat.longData++;
                        }
                        if (colStat.minNumber == null || i < colStat.minNumber.longValue()) {
                            colStat.minNumber = i;
                        }
                        if (colStat.maxNumber == null || i > colStat.maxNumber.longValue()) {
                            colStat.maxNumber = i;
                        }
                    } catch (Exception ex) {
                        try {
                            double d = Double.valueOf(data);
                            if (colStat.minNumber == null || d < colStat.minNumber.doubleValue()) {
                                colStat.minNumber = d;
                            }
                            if (colStat.maxNumber == null || d > colStat.maxNumber.doubleValue()) {
                                colStat.maxNumber = d;
                            }
                            colStat.doubleData++;
                        } catch (Exception ex2) {
                            colStat.stringData++;
                        }
                    }
                }
                ++col;
            }
        }
        endTime = Instant.now();
        if (reportStats) {
            report(header);
        }
        if (generateArffHeader) {
            System.out.println(generateArffHeader());
        }
    }

    public void report(ArffHeader header) {
        List<String> lines = new ArrayList<>();
        lines.add("source: " + source);
        lines.add("lines: " + this.lines);
        lines.add("emptyLines: " + emptyLines);
        lines.add("maxLineStringLength: " + maxLineStringLength);
        lines.add("columns: " + columnStats.size());

        if (header != null) {
            lines.add("arff.name: " + header.getName());
            lines.add("arff.dataStart: " + header.getDataIndex());
            lines.add("arff.attributes: " + header.getAttributes().size());
        }
        if (startTime != null && endTime != null) {
            lines.add("time: " + toStr(Duration.between(startTime, endTime)));
        }

        int i = 0;
        for (ColumnStat colStat : columnStats) {
            String valuesStr = "";
            if (reportColumnValues) {
                valuesStr = String.format(", values=%s", colStat.valueCounts.size() + (colStat.valueCounts.size() >= valueCountLimit ? "<" : ""));
            }

            String maxStr = "";
            if (colStat.maxNumber instanceof Long) {
                maxStr = String.format(", maxValue=%,d", colStat.maxNumber.longValue());
            } else if (colStat.maxNumber instanceof Double) {
                maxStr = String.format(", maxValue=%,f", colStat.maxNumber.doubleValue());
            }

            String minStr = "";
            if (colStat.minNumber instanceof Long) {
                minStr = String.format(", minValue=%,d", colStat.minNumber.longValue());
            } else if (colStat.minNumber instanceof Double) {
                minStr = String.format(", minValue=%,f", colStat.minNumber.doubleValue());
            }

            lines.add(String.format("column[%d]: ints=%d, longs=%d, doubles=%d, strings=%d, unknowns=%d, empties=%d, maxStrLen=%d%s%s%s",
                    i, colStat.intData, colStat.longData, colStat.doubleData, colStat.stringData, colStat.unknownData, colStat.emptyData,
                    colStat.maxStringLength, valuesStr, minStr, maxStr));
            if (reportAttributes && header != null && i < header.getAttributes().size()) {
                ArffAttribute attr = header.getAttributes().get(i);
                lines.add("  " + attr.toString());
            }
            ++i;
        }
        if (header != null) {
            for (; i < header.getAttributes().size(); ++i) {
                ArffAttribute attr = header.getAttributes().get(i);
                lines.add(String.format("column[%d]: ?", i));
                lines.add("  " + attr.toString());
            }
        }

        if (reportColumnValues) {
            lines.addAll(reportColumnValues());
        }

        lines.forEach(System.out::println);
    }

    public List<String> reportColumnValues() {
        List<String> lines = new ArrayList<>();
        int i = 0;
        for (ColumnStat colStat : columnStats) {
            lines.add(String.format("column[%d] values:", i));
            colStat.valueCounts.entrySet().stream()
                    .sorted(Map.Entry.comparingByValue((l,r) -> Integer.compare(r, l)))
                    .map(e -> "    " + e.getKey() + " : " + e.getValue())
                    .limit(30)
                    .forEach(lines::add);
            ++i;
        }
        return lines;
    }

    public void runArff(ArffReader reader) {
        ArffHeader header = reader.getHeader();
        run(reader, header);
    }

    public ArffHeader generateArffHeader() {
        List<ArffAttribute> attrs = new ArrayList<>();
        int i = 0;
        for (ColumnStat col : columnStats) {
            ArffAttributeType type;
            if (col.valueCounts.size() < 100 && col.doubleData == 0) {
                type = new ArffAttributeType.EnumAttributeType(new ArrayList<>(col.valueCounts.keySet()));
            } else if (col.doubleData > 0 && col.stringData == 0) {
                type = ArffAttributeType.SimpleAttributeType.Real;
            } else if (col.doubleData == 0 && col.stringData == 0 &&
                    (col.longData > 0 || col.intData > 0)) {
                type = ArffAttributeType.SimpleAttributeType.Integer;
            } else {
                type = ArffAttributeType.SimpleAttributeType.String;
            }
            attrs.add(new ArffAttribute("attr" + i, type));
            ++i;
        }
        return new ArffHeader(source, attrs, 0);
    }


    public static String toStr(Duration d) {
        if (d == null) {
            return "-";
        } else {
            return d.toString().replaceAll("([A-Z])(\\d)", "$1\t$2");
        }
    }
}
