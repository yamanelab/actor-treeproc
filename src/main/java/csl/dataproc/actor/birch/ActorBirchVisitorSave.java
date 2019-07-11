package csl.dataproc.actor.birch;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import csl.dataproc.actor.stat.StatSystem;
import csl.dataproc.actor.visit.TraversalMessages;
import csl.dataproc.actor.visit.TraversalVisitActor;
import csl.dataproc.birch.*;
import csl.dataproc.csv.CsvWriter;

import java.io.Serializable;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

public class ActorBirchVisitorSave extends TraversalVisitActor<BirchNode, ActorBirchVisitorSave.SaveSendVisitor> {
    protected BirchBatch.InputI input;

    public static void run(ActorSystem system, BirchNode root, String summaryName, String outNamePattern, String dir,
                           BirchBatch.InputI input) {
        try {
            TraversalVisitActor.start(system, ActorBirchVisitorSave.class,
                    root, summaryName, outNamePattern, dir, input)
                    .get();
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    public ActorBirchVisitorSave(BirchNode root, String summaryName, String outNamePattern, String dir,
                                 BirchBatch.InputI input, CompletableFuture<SaveSendVisitor> finisher) {
        super(root, new BirchMessages.BirchTraversalState<>(new SaveSendVisitor()), finisher);
        dirs = new LinkedList<>();
        dirs.push(Paths.get(dir));
        this.summaryName = summaryName;
        this.outNamePattern = outNamePattern;

        this.input = input;
    }

    @Override
    public void preStart() throws Exception {
        super.preStart();
        this.initState.visitor.ref = self();
        System.err.println("start save: " + self());
    }

    @Override
    public Receive createReceive() {
        return builder()
                .match(BirchMessages.VisitTree.class, this::visitTree)
                .match(BirchMessages.VisitEntry.class, this::visitEntry)
                .match(BirchMessages.VisitAfter.class, this::visitAfterTree)
                .match(BirchMessages.VisitChild.class, this::visitTreeChild)
                .build();
    }

    protected String summaryName;
    protected String outNamePattern;
    protected LinkedList<Path> dirs;
    protected int nextIndex;

    public void visitTree(BirchMessages.VisitTree m) {
        Path dir = dirs.peek();
        CsvWriter summary = new CsvWriter();
        summary.withOutputFile(dir.resolve(summaryName));
        summary.writeLine("#index", "type", "childSize", "CF.entries", "CF.squareSum", "CF.centroid", "id");
        summary.writeLine(summary(-1, true, m.childSize, m.cf, m.id));
        summary.close();

        Path file = parent(dir).resolve(summaryName);
        if (Files.exists(file)) {
            try (CsvWriter summaryParent = openForAppend(file)) {
                summaryParent.writeLine(summary(nextIndex, true, m.childSize, m.cf, m.id));
            } catch (Exception ex) {
                ex.printStackTrace();
            }
        }

        dirs.push(dir);
    }

    private CsvWriter openForAppend(Path file) {
        try {
            return new CsvWriter().withOutputChannel(FileChannel.open(file,
                    StandardOpenOption.WRITE, StandardOpenOption.APPEND,
                    StandardOpenOption.CREATE));
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    public static Path parent(Path p) {
        return p == null ? Paths.get(".") :
                (p.getParent() == null ? Paths.get(".") :
                        p.getParent());
    }

    public void visitEntry(BirchMessages.VisitEntry m) {
        Path dir = dirs.peek();
        Path fileParent = parent(dir).resolve(summaryName);
        if (Files.exists(fileParent)) {
            try (CsvWriter summary = openForAppend(fileParent)) {
                summary.writeLine(summary(nextIndex, false, 0, m.cf, m.id));
            } catch (Exception ex) {
                ex.printStackTrace();
            }
        }


        Path file = dir.resolve(String.format(outNamePattern, nextIndex));

        try (CsvWriter writer = new CsvWriter().withOutputFile(file)) {
            for (int d = 0, dl = m.indexes.size(); d < dl; ++d) {
                long offset = m.indexes.get(d);

                List<String> cols = new ArrayList<>(2);
                cols.add(Long.toString(offset));

                if (input != null) {
                    BirchNodeInput in = input.makeDataPoint(input.getInput().readNext(offset));
                    if (in instanceof BirchNodeInput.BirchNodeInputLabeled) {
                        cols.add(Integer.toString(((BirchNodeInput.BirchNodeInputLabeled) in).getLabel()));
                    }
                }

                writer.writeLine(cols);
            }
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    public List<String> summary(int index, boolean tree, int childSize, ClusteringFeature cf, long id) {
        List<String> data = new ArrayList<>();
        try {

            if (index < 0) {
                data.add("");
            } else {
                data.add(Integer.toString(index));
            }
            if (tree) {
                data.add("n");
                data.add(Integer.toString(childSize));
            } else {
                data.add("e");
                data.add("");
            }
            data.add(Integer.toString(cf.getEntries()));
            data.add(Double.toString(cf.getSquareSum()));
            for (double d : cf.getCentroid()) {
                data.add(Double.toString(d));
            }
            data.add(Long.toString(id));
        } catch (Exception ex) {
            ex.printStackTrace();
        }
        return data;
    }

    public void visitAfterTree(BirchMessages.VisitAfter m) {
        dirs.pop();
    }

    public void visitTreeChild(BirchMessages.VisitChild m) {
        dirs.pop();
        Path subDir = dirs.peek().resolve(Integer.toString(m.i));
        dirs.push(subDir);

        if (!Files.exists(subDir)) {
            try {
                Files.createDirectory(subDir);
            } catch (Exception ex) {
                ex.printStackTrace();
            }
        }
        nextIndex = m.i;
    }

    @Override
    public void end(TraversalMessages.TraversalEventEnd<SaveSendVisitor> e) {
        super.end(e);
        if (input != null) {
            input.close();
        }
    }

    public static class IndexedSaveVisitor extends BirchBatch.SaveVisitor {

        public IndexedSaveVisitor(String summaryName, String outNamePattern) {
            super(summaryName, outNamePattern);
        }

        @Override
        public List<String> summary(int index, BirchNode e) {
            if (index < 0 && e instanceof ActorBirchConfig.Indexed) {
                index = (int) (((ActorBirchConfig.Indexed) e).getChildIndex()
                        % StatSystem.MAX_INSTANCES);
            }
            return super.summary(index, e);
        }

        public LinkedList<Path> getDirs() {
            return dirs;
        }
    }

    public static class SaveSendVisitor extends BirchNode.BirchVisitor implements Serializable {
        public ActorRef ref;

        @Override
        public boolean visitTree(BirchNodeCFTree t) {
            long id = -1L;
            if (t instanceof ActorBirchConfig.Indexed) {
                id = ((ActorBirchConfig.Indexed) t).getChildIndex();
            }
            ref.tell(new BirchMessages.VisitTree(t.getChildSize(), id, t.getClusteringFeature()), ActorRef.noSender());
            return true;
        }

        public void log(String str) {
            System.err.println(str);
        }

        @Override
        public void visitAfterTree(BirchNodeCFTree t) {
            ref.tell(new BirchMessages.VisitAfter(), ActorRef.noSender());
        }

        @Override
        public void visitTreeChild(BirchNodeCFTree tree, int i, BirchNodeCFTree.NodeHolder nodeHolder) {
            ref.tell(new BirchMessages.VisitChild(i), ActorRef.noSender());
        }

        @Override
        public boolean visitEntry(BirchNodeCFEntry e) {
            long id = -1L;
            if (e instanceof ActorBirchConfig.Indexed) {
                id = ((ActorBirchConfig.Indexed) e).getChildIndex();
            }
            ref.tell(new BirchMessages.VisitEntry(id, e.getClusteringFeature(), e.getIndexes()), ActorRef.noSender());
            return true;
        }
    }
}
