package csl.dataproc.actor.birch.remote;

import csl.dataproc.csv.CsvLine;
import csl.dataproc.csv.CsvReader;
import csl.dataproc.csv.CsvWriter;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.NumberFormat;
import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class BirchInputLimit {
    public static void main(String[] args) {
        new BirchInputLimit().run(args);
    }

    protected List<String> inputFiles;
    protected String outputFile;
    protected long limitBytes = -1L;
    protected long limitLines = -1L;
    protected long limitFiles = -1L;

    protected CsvWriter output;
    protected long countBytes = 0;
    protected long countLines = 0;
    protected long countFiles = 0;

    protected Instant startTime = Instant.now();
    protected Instant prevTime = startTime;
    protected long logPeriod = 10;

    public void run(String... args) {
        if (parseArgs(args)) {
            start();
        }
    }

    public void help() {
        System.out.println("\n" +
                "--limit-bytes <bytes>\n" +
                "--limit-lines <lines>\n" +
                "--limit-files <files>\n" +
                "<inputFileOrDir>... <outputFile> : obtains .csv files from inputFileOrDir \n");
    }


    public boolean parseArgs(String... args) {
        List<String> files = new ArrayList<>();
        String lastFile = null;
        for (int i = 0, l = args.length; i < l; ++i) {
            String arg = args[i];
            if (arg.equals("--limit-bytes")) {
                ++i;
                arg = args[i];
                limitBytes = toLong(arg);
            } else if (arg.equals("--limit-lines")) {
                ++i;
                arg = args[i];
                limitLines = toLong(arg);
            } else if (arg.equals("--limit-files")) {
                ++i;
                arg = args[i];
                limitFiles = toLong(arg);
            } else if (arg.equals("--help") || arg.equals("-h")) {
                help();
                return false;
            } else {
                if (lastFile != null) {
                    files.add(lastFile);
                }
            }
            lastFile = arg;
        }
        outputFile = lastFile;
        inputFiles = files;
        if (inputFiles.isEmpty() || outputFile == null) {
            help();
        }
        return true;
    }

    private long toLong(String arg) {
        try {
            return NumberFormat.getInstance().parse(arg).longValue();
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    public void start() {
        try (CsvWriter output = new CsvWriter().withOutputFile(Paths.get(outputFile))){
            this.output = output;
            log(String.format("start %s", outputFile));
            for (String input : inputFiles) {
                Path inputPath = Paths.get(input);
                input(inputPath);
            }
        }
        log(String.format("end %s countFiles=%,d countLines=%,d countBytes=%,d", outputFile, countFiles, countLines, countBytes));
    }

    public boolean input(Path inputPath) {
        if (Files.isRegularFile(inputPath) && inputPath.getFileName().toString().endsWith(".csv")) {
            return inputFile(inputPath);
        } else if (Files.isDirectory(inputPath)) {
            try (Stream<Path> items = Files.list(inputPath)) {
                for (Path selected :items.sorted(Comparator.comparing(e -> e.getFileName().toString()))
                        .collect(Collectors.toList())) {
                    if (!input(selected)) {
                        return false;
                    }
                }
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
            return true;
        } else {
            return true;
        }
    }

    public boolean inputFile(Path inputPath) {
        logPeriod(String.format("inputFile %s countFiles=%,d countLines=%,d countBytes=%,d", inputPath, countFiles, countLines, countBytes));
        if (limitFiles > 0 && countFiles >= limitFiles) {
            log(String.format("inputFile stop %s countFiles=%,d : limitFiles=%,d", inputPath, countFiles, limitFiles));
            return false;
        }
        countFiles++;
        long lastEnd = 0;
        try (CsvReader r = CsvReader.get(inputPath)) {
            for (CsvLine line : r) {
                long size = line.getEnd() - line.getStart();
                if ((limitLines > 0 && countLines >= limitLines) ||
                    (limitBytes > 0 && countBytes + size >= limitBytes)) {
                    log(String.format("inputFile stop %s countFiles=%,d countLines=%,d countBytes=%,d : limitFiles=%,d limitLines=%,d limitBytes=%,d",
                            inputPath, countFiles, countLines, countBytes, limitFiles, limitLines, limitBytes));
                    return false;
                }
                output.writeLine(line);
                countBytes += size;
                lastEnd += size;
                ++countLines;
            }
        }
        return true;
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

    public void logPeriod(String str, long limitSeconds) {
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
