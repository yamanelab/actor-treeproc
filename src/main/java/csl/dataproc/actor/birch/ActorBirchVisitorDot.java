package csl.dataproc.actor.birch;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import csl.dataproc.actor.stat.StatVm;
import csl.dataproc.actor.visit.ActorVisitor;
import csl.dataproc.actor.visit.TraversalMessages;
import csl.dataproc.actor.visit.TraversalVisitActor;
import csl.dataproc.birch.BirchBatch;
import csl.dataproc.birch.BirchNode;
import csl.dataproc.birch.BirchNodeCFTree;
import csl.dataproc.birch.ClusteringFeature;
import csl.dataproc.dot.DotAttribute;
import csl.dataproc.dot.DotGraph;
import csl.dataproc.dot.DotNode;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.nio.file.Path;
import java.util.concurrent.CompletableFuture;

public class ActorBirchVisitorDot extends BirchBatch.DotVisitor {
    protected Path dotFile;
    public CompletableFuture<DotGraph> graphResult;
    protected StatVm.GraphImageSetter imageSetter;

    protected boolean image;

    public ActorBirchVisitorDot() {
        this(true);
    }

    public ActorBirchVisitorDot(boolean image) {
        this.image = image;
    }

    public DotGraph run(ActorSystem system, BirchNode root, Path dotFile) {
        init(system, dotFile);
        TraversalVisitActor.startWithoutFuture(system, DotActor.class, this, root);
        return result(system);
    }

    protected void init(ActorSystem system, Path dotFile) {
        this.dotFile = dotFile;
        if (image) {
            imageSetter = new StatVm.GraphImageSetter(system, dotFile);
        }
        initGraph();
        graphResult = new CompletableFuture<>();
    }

    protected DotGraph result(ActorSystem system) {
        try {
            DotGraph g = graphResult.get();
            if (imageSetter != null) {
                imageSetter.saveVms(g);
            }
            return g;
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    public DotNode add(int childIndex, long id, String label, boolean push, boolean box) {
        this.childIndex = childIndex;
        return add(id, label, push, box);
    }

    public DotNode add(long id, String label, boolean push, boolean box) {
        DotNode dn = this.graph.add(label);
        if (!this.stack.isEmpty()) {
            this.graph.addEdge(this.stack.peek(), dn).getAttrs().add(DotAttribute.labelQuote("" + this.childIndex));
        }

        //sid

        if (!box) {
            dn.getAttrs().add(DotAttribute.styleRounded());
        }
        dn.getAttrs().add(DotAttribute.shapeBox());

        if (push) {
            this.stack.push(dn);
        }
        //img
        if (imageSetter != null) {
            imageSetter.setNodeImage(dn, id, false);
        }

        return dn;
    }

    public static class DotActor extends TraversalVisitActor<BirchNode, DotTraversalVisitor> {
        protected ActorBirchVisitorDot host;

        public DotActor(ActorBirchVisitorDot host, BirchNode root) {
            this(host, root, new DotTraversalVisitor());
        }

        public DotActor(ActorBirchVisitorDot host, BirchNode root, DotTraversalVisitor v) {
            super(root, new BirchMessages.BirchTraversalState<>(v), null);
            this.host = host;
        }

        @Override
        public void preStart() throws Exception {
            super.preStart();
            initState.visitor.ref = self();
        }

        @Override
        public Receive createReceive() {
            return builder()
                    .match(BirchMessages.DotAdd.class, this::add)
                    .match(BirchMessages.DotPop.class, this::pop)
                    .match(String.class, this::ask)
                    .build();
        }

        public void add(BirchMessages.DotAdd m) {
            host.add(m.childIndex, m.id, m.label, m.push, m.box);
        }

        public void pop(BirchMessages.DotPop m) {
            host.visitAfterTree(null);
        }

        public void ask(String s) {
            sender().tell(s, self());
        }

        @Override
        public void end(TraversalMessages.TraversalEventEnd<DotTraversalVisitor> e) {
            super.end(e);
            try {
                host.graphResult.complete(host.graph);
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }
    }

    public static class DotTraversalVisitor extends BirchBatch.DotVisitor
        implements Serializable, ActorVisitor<ActorBirchNodeActor> {
        protected ActorRef ref;
        protected long lastId = -1;

        public DotTraversalVisitor() { }

        public DotTraversalVisitor(ActorRef ref) {
            this.ref = ref;
        }

        @Override
        protected void add(String label, boolean push, boolean box) {
            ref.tell(new BirchMessages.DotAdd(lastId, label, push, box, childIndex), ActorRef.noSender());
        }

        @Override
        public void visitAfterTree(BirchNodeCFTree t) {
            ref.tell(new BirchMessages.DotPop(), ActorRef.noSender());
        }

        @Override
        public void visitActor(ActorBirchNodeActor actor) {
            lastId = actor.getNodeId();
        }

        private void writeObject(ObjectOutputStream out) throws IOException {
            out.writeInt(this.childIndex);
            out.writeLong(lastId);
            out.writeObject(ref);
        }
        private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
            childIndex = in.readInt();
            lastId = in.readLong();
            ref = (ActorRef) in.readObject();
        }

        @Override
        public String toString() {
            return "DotTraversalVisitor{" +
                    "ref=" + ref +
                    ", lastId=" + lastId +
                    '}';
        }

        @Override
        public String cfString(int index, BirchNode e) {
            ClusteringFeature cf = e.getClusteringFeature();
            return String.format("[%d] CF[N=%d, SS=%.4f]",
                    index, cf.getEntries(), cf.getSquareSum()/*, this.toStr(cf.getCentroid())*/);
        }
    }
}
