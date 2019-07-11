package csl.dataproc.actor.birch.remote;

import csl.dataproc.csv.CsvLine;
import csl.dataproc.csv.CsvReader;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

/**
 * load files generated by {@link csl.dataproc.actor.birch.ActorBirchVisitorSaveSingleFile} and
 *    {@link ActorBirchServer} (fileIndex.csv) ,
 *  and compute purity of entries.
 */
public class BirchSavedSingleFileLoader {
    protected List<SourceFile> sources = new ArrayList<>();
    protected LinkedList<SourceFile> recentUsed = new LinkedList<>(); //first is most recent

    protected Path fileIndexPath;
    protected Path sourcePath;
    protected int maxOpeningFiles = 100;

    public static void main(String[] args) {
        new BirchSavedSingleFileLoader().run(args);
    }

    public void run(String[] args) {
        if (parseArgs(args)) {
            loadFileIndex(fileIndexPath);
        }
        Purity p = computePurity();
        System.out.println(p);
    }

    public Purity computePurity() {
        Purity p = new Purity();
        try (CsvReader reader = CsvReader.get(sourcePath)) {
            for (CsvLine line : reader) {
                List<ImageEntry> es = readImageEntries(line);
                Map<String, List<ImageEntry>> group = group(es);

                List<ImageEntry> majority = group.values().stream()
                        .reduce((a,b) -> a.size() > b.size() ? a : b)
                        .orElse(Collections.emptyList());

                p.totalPurity += majority.size() / (double) es.size();
                p.totalMajorities += majority.size();
                p.totalDataPoints += es.size();
                p.totalClusters++;
            }
        }
        return p;
    }

    public static class Purity {
        public double totalPurity;
        public long totalDataPoints;
        public long totalMajorities;
        public long totalClusters;

        @Override
        public String toString() {
            return String.format("totalDataPoints=%,d, totalMajorities=%,d, totalClusters=%,d, purity=%.4f",
                    totalDataPoints, totalMajorities, totalClusters, totalPurity);
        }
    }

    public void help() {
        System.out.println("--index <fileIndex.csv>  <out.csv>");
    }

    public boolean parseArgs(String[] args) {
        for (int i = 0, l = args.length; i < l; ++i) {
            String arg = args[i];
            if (arg.equals("--index")) {
                ++i;
                arg = args[i];
                fileIndexPath = Paths.get(arg);
            } else if (arg.equals("--help") || arg.equals("-h")) {
                help();
                return false;
            } else {
                sourcePath = Paths.get(arg);
            }
        }

        if (sourcePath == null || fileIndexPath == null) {
            help();
            return false;
        } else {
            return true;
        }
    }

    /**
     * @param path fileIndex.csv generated by {@link ActorBirchServer}: path.csv,offset
     */
    public void loadFileIndex(Path path) {
        try (CsvReader r = CsvReader.get(path)) {
            for (CsvLine line : r) {
                SourceFile next = new SourceFile(Paths.get(line.get(0)), line.getLong(1), this);
                sources.add(next);
            }
            sources.sort(Comparator.comparing(SourceFile::getStart));
            SourceFile last = null;
            for (SourceFile next : sources) {
                if (last != null) {
                    last.end = next.start;
                }
                last = next;
            }
        }
        if (sources instanceof ArrayList<?>) {
            ((ArrayList<SourceFile>) sources).trimToSize();
        }
    }

    public static class SourceFile {
        public Path file;
        public long start;
        /**
         * exclusive
         */
        public long end;

        protected CsvReader reader;
        protected BirchSavedSingleFileLoader loader;

        public SourceFile(Path file, long start, BirchSavedSingleFileLoader loader) {
            this.file = file;
            this.start = start;
            this.loader = loader;
        }

        public long getStart() {
            return start;
        }

        public int compareIndex(long index) {
            if (index < start) {
                return -1;
            } else if (containsIndex(index)) {
                return 0;
            } else {
                return 1;
            }
        }

        public boolean containsIndex(long index) {
            return start <= index && (start > end || end == 0 || index < end);
        }

        public boolean hasReader() {
            return reader != null;
        }

        public void clearReader() {
            if (reader != null) {
                reader.close();
                reader = null;
            }
        }

        public void openReader() {
            if (reader == null) {
                reader = CsvReader.get(file);
            }
        }

        public CsvLine readLine(long offset) {
            if (reader == null) {
                loader.open(this);
            }
            return reader.readNext(offset);
        }
    }

    public static class SourceFileOffset {
        public SourceFile source;
        public long sourceOffset;

        public SourceFileOffset(SourceFile source, long sourceOffset) {
            this.source = source;
            this.sourceOffset = sourceOffset;
        }

        public CsvLine readLine() {
            return source.readLine(sourceOffset);
        }
    }

    public SourceFileOffset getOffset(long index) {
        //use cache
        int count = 0;
        for (Iterator<SourceFile> iter = recentUsed.iterator();
             iter.hasNext() && count < 5; ) {
            SourceFile f = iter.next();
            if (f.containsIndex(index)) {
                return new SourceFileOffset(f, index - f.start);
            }
            ++count;
        }

        int min = 0;
        int max = sources.size();
        while (max - min > 5) {
            int i = (max - min) / 2 + min;
            SourceFile f = sources.get(i);
            int c = f.compareIndex(index);
            if (c == 0) {
                return new SourceFileOffset(f, index - f.start);
            } else if (c < 0) {
                if (i == max) {
                    return null;
                } else {
                    max = i;
                }
            } else {
                if (i == min) {
                    return null;
                } else {
                    min = i;
                }
            }
        }
        for (int i = min; i < max; ++i) {
            SourceFile f = sources.get(i);
            if (f.containsIndex(index)) {
                return new SourceFileOffset(f, index - f.start);
            }
        }
        return null;
    }

    public void open(SourceFile file) {
        if (!file.hasReader()) {
            int size = recentUsed.size();
            while (size > maxOpeningFiles) {
                SourceFile removed = recentUsed.removeLast();
                removed.clearReader();
                --size;
            }
            recentUsed.addFirst(file);
            file.openReader();
        }
    }

    ////////////////

    public static class ImageEntry {
        public String path;
        public String label;

        public ImageEntry(String path, String label) {
            this.path = path;
            this.label = label;
        }

        public String getPath() {
            return path;
        }

        public String getLabel() {
            return label;
        }
    }

    public ImageEntry readImageEntry(long index) {
        CsvLine line = getOffset(index).readLine();
        return new ImageEntry(line.get(0), line.get(1));
    }

    /**
     *
     * @param line childIndex1,childIndex2,...,Ln,entryIndex1,entryIndex2,...entryIndex_n
     * @return list of  entryIndex1..n
     */
    public List<ImageEntry> readImageEntries(CsvLine line) {
        boolean offset = false;
        ArrayList<ImageEntry> es = new ArrayList<>();
        for (String col : line.getData()) {
            if (!offset) {
                if (col.startsWith("L")) {
                    offset = true;
                }
            } else {
                try {
                    es.add(readImageEntry(Long.valueOf(col)));
                } catch (NumberFormatException nfe) {
                    System.err.println(nfe + ": " + col + " : " + line);
                }
            }
        }
        return es;
    }

    public Map<String,List<ImageEntry>> readImageEntriesWithGrouping(CsvLine line) {
        return group(readImageEntries(line));
    }

    public Map<String,List<ImageEntry>> group(List<ImageEntry> es) {
        Map<String, List<ImageEntry>> map = new HashMap<>();
        es.forEach(e ->
            map.computeIfAbsent(e.getLabel(), l -> new ArrayList<>(5))
                    .add(e));
        return map;
    }
}
