package csl.dataproc.actor.birch;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import csl.dataproc.actor.visit.TraversalMessages;
import csl.dataproc.actor.visit.TraversalVisitActor;
import csl.dataproc.birch.*;
import csl.dataproc.csv.CsvWriter;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.concurrent.CompletableFuture;

public class ActorBirchVisitorSaveSingleFile extends TraversalVisitActor<BirchNode, ActorBirchVisitorSaveSingleFile.SaveSingleFileSendVisitor> {
    protected String file;
    protected LinkedList<Long> nodeStack = new LinkedList<>();
    protected CsvWriter entryWriter; //childIndex1,childIndex2,childIndex3,...,"L"entrySize,entryIndex1,entryIndex2,...,entryIndex_entrySize
    protected CsvWriter nodeCfWriter; //childIndex1,...,"CF",CF.entries,CF.squareSum,CF.linearSum[0],CF.linearSum[1],... : file-cf.csv

    protected long entryCount;
    protected long nodeCount;

    public static void run(ActorSystem system, BirchNode root, String file) {
        try {
            TraversalVisitActor.start(system, ActorBirchVisitorSaveSingleFile.class, root, file).get();
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    public ActorBirchVisitorSaveSingleFile(BirchNode root, String file, CompletableFuture<SaveSingleFileSendVisitor> finisher) {
        super(root, new BirchMessages.BirchTraversalState<>(new SaveSingleFileSendVisitor()), finisher);
        this.file = file;
    }

    @Override
    public void preStart() throws Exception {
        super.preStart();
        this.initState.visitor.ref = self();

        Path filePath = Paths.get(file);
        Path dirPath = filePath.getParent();
        if (dirPath != null && !dirPath.toString().isEmpty() && !Files.exists(dirPath)) {
            Files.createDirectories(dirPath);
        }
        entryWriter = new CsvWriter().withOutputFile(filePath);
        System.err.println("Entry-file: " + filePath);

        openNodeCfWriter(filePath);
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
    public Receive createReceive() {
        return builder()
                .match(BirchMessages.VisitTree.class, this::visitTree)
                .match(BirchMessages.VisitEntry.class, this::visitEntry)
                .match(BirchMessages.VisitAfter.class, this::visitAfterTree)
                .match(BirchMessages.VisitChild.class, this::visitTreeChild)
                .build();
    }

    public void visitEntry(BirchMessages.VisitEntry m) {
        ++entryCount;
        LongList ll = m.indexes;
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
        writeNodeCf(m.cf);
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

    public void visitTree(BirchMessages.VisitTree m) {
        writeNodeCf(m.cf);
        ++nodeCount;
        nodeStack.addLast(-1L); //start: if the node has a child, then there is a visitTreeChild
    }

    public void visitTreeChild(BirchMessages.VisitChild m) {
        nodeStack.removeLast();
        nodeStack.addLast((long) m.i); //next child
    }

    public void visitAfterTree(BirchMessages.VisitAfter m) {
        nodeStack.removeLast(); //exit
    }

    @Override
    public void end(TraversalMessages.TraversalEventEnd<SaveSingleFileSendVisitor> e) {
        super.end(e);
        System.err.println(String.format("%,d entries, %,d nodes", entryCount, nodeCount));
        if (entryWriter != null) {
            this.entryWriter.close();
        }
        if (nodeCfWriter != null) {
            nodeCfWriter.close();
        }
    }

    static class SaveSingleFileSendVisitor extends BirchNode.BirchVisitor {
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
