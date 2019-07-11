package csl.dataproc.actor.vfdt;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import csl.dataproc.actor.visit.ActorVisitor;
import csl.dataproc.actor.visit.TraversalVisitActor;
import csl.dataproc.csv.CsvWriter;
import csl.dataproc.tgls.util.JsonWriter;
import csl.dataproc.vfdt.VfdtBatch;
import csl.dataproc.vfdt.VfdtNode;
import csl.dataproc.vfdt.VfdtVisitorStat;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.LinkedList;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

public class ActorVfdtVisitorStat extends TraversalVisitActor<VfdtNode, ActorVfdtVisitorStat.TraversalStatVisitor> {

    public static Map<Long,VfdtBatch.AnswerStat> run(ActorSystem system, Path dir, VfdtNode node) {
        CompletableFuture<TraversalStatVisitor> f = TraversalVisitActor.start(system, ActorVfdtVisitorStat.class, node, dir);
        try {
            return f.get().answerStats;
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    public static Map<Long,VfdtBatch.AnswerStat> runExperiment(ActorSystem system, Path dir, VfdtNode node) {
        CompletableFuture<TraversalStatVisitor> f = TraversalVisitActor.start(system, ActorVfdtVisitorStat.ActorVfdtVisitorStatForExperiment.class, node, dir);
        try {
            return f.get().answerStats;
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }


    protected Path dir;

    public ActorVfdtVisitorStat(VfdtNode node, Path dir, CompletableFuture<TraversalStatVisitor> f) {
        super(node, new VfdtMessages.VfdtTraversalState<>(new TraversalStatVisitor()), f);
        this.dir = dir;
    }

    @Override
    public void preStart() throws Exception {
        super.preStart();
        initState.visitor.setRef(dir, self());
    }

    @Override
    public Receive createReceive() {
        return builder()
                .match(VfdtMessages.WriteCsv.class, this::writeCsv)
                .match(VfdtMessages.WriteJson.class, this::writeJson)
                .match(VfdtMessages.CreateDirectories.class, this::createDirectories)
                .match(String.class, this::ask)
                .build();
    }

    public void writeCsv(VfdtMessages.WriteCsv m) {
        try {
            Files.write(Paths.get(m.path), m.data.getBytes(StandardCharsets.UTF_8));
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }
    public void writeJson(VfdtMessages.WriteJson m) {
        JsonWriter.write(m.json, Paths.get(m.path));
    }

    public void ask(String s) {
        sender().tell(s, self());
    }


    public void createDirectories(VfdtMessages.CreateDirectories d) {
        try {
            Files.createDirectories(Paths.get(d.path));
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    public static class ActorVfdtVisitorStatForExperiment extends ActorVfdtVisitorStat {
        public ActorVfdtVisitorStatForExperiment(VfdtNode node, Path dir, CompletableFuture<TraversalStatVisitor> f) {
            super(node, dir, f);
        }

        @Override
        public void writeCsv(VfdtMessages.WriteCsv m) {
            //skip
        }

        @Override
        public void writeJson(VfdtMessages.WriteJson m) {
            //skip
        }

        @Override
        public void createDirectories(VfdtMessages.CreateDirectories d) {
            //skip
        }
    }

    public static class TraversalStatVisitor extends VfdtVisitorStat implements Serializable, ActorVisitor<ActorVfdtNodeActor> {
        protected ActorRef ref;
        protected Map<Long,VfdtBatch.AnswerStat> answerStats = new TreeMap<>();

        private static final long serialVersionUID = 1L;

        public TraversalStatVisitor() {
            dirStack.push(".");
        }

        public TraversalStatVisitor(Path dir, ActorRef ref) {
            dirStack.push(".");
            dirStack.push(dir.toString());
            this.ref = ref;
        }

        public void setRef(Path path, ActorRef ref) {
            dirStack.push(path.toString());
            this.ref = ref;
        }

        @Override
        protected void write(Path path, Consumer<CsvWriter> f) {
            StringWriter sw = new StringWriter();
            try (CsvWriter w = new CsvWriter().withOutputWriter(sw)) {
                f.accept(w);
            }
            ref.tell(new VfdtMessages.WriteCsv(path.toString(), sw.getBuffer().toString()), ActorRef.noSender());
        }

        @Override
        protected void writeJson(Object json, String fileName) {
            Path p = Paths.get(dirStack.peek()).resolve(fileName);
            ref.tell(new VfdtMessages.WriteJson(p.toString(), json), ActorRef.noSender());
        }

        @Override
        protected void createDirectories(Path p) {
            ref.tell(new VfdtMessages.CreateDirectories(p.toString()), ActorRef.noSender());
        }

        @Override
        public String toString() {
            return "TraversalStatVisitor{" +
                    "ref=" + ref +
                    ", dirStack=" + dirStack +
                    ", totalCount=" + totalCount +
                    '}';
        }

        //without kryo+artery cause problem?
        private void writeObject(ObjectOutputStream out) throws IOException {
//            out.defaultWriteObject();
            out.writeObject(dirStack);
            out.writeLong(totalCount);
            out.writeObject(ref);
        }
        @SuppressWarnings("unchecked")
        private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
//            in.defaultReadObject();
            dirStack = (LinkedList<String>) in.readObject();
            totalCount = in.readLong();
            ref = (ActorRef) in.readObject();
        }

        @Override
        public void visitActor(ActorVfdtNodeActor actor) {
            actor.getAnswerStatMap().forEach((k,v) -> {
                merge(answerStats.computeIfAbsent(k, VfdtBatch.AnswerStat::new), v);
            });
        }


    }

    public static void merge(VfdtBatch.AnswerStat left, VfdtBatch.AnswerStat right) {
        left.classValue = right.classValue;
        left.count += right.count;
        left.success += right.success;
        right.failClassToCount.forEach((k,v) -> {
            left.failClassToCount.put(k, left.failClassToCount.getOrDefault(k, 0L) + v);
        });
    }

}
