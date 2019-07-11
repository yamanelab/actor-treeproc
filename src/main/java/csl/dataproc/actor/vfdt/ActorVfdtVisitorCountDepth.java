package csl.dataproc.actor.vfdt;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import csl.dataproc.actor.visit.TraversalMessages;
import csl.dataproc.actor.visit.TraversalVisitActor;
import csl.dataproc.vfdt.VfdtNode;
import csl.dataproc.vfdt.VfdtNodeSplit;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

public class ActorVfdtVisitorCountDepth extends TraversalVisitActor<VfdtNode, ActorVfdtVisitorCountDepth.TraversalCountVisitor> {
    protected Map<Integer,Long> depthToNodes = new HashMap<>();
    protected CompletableFuture<Map<Integer,Long>> result;

    public static Map<Integer,Long> run(ActorSystem system, VfdtNode node) {
        try {
            CompletableFuture<Map<Integer,Long>> c = new CompletableFuture<>();
            TraversalVisitActor.startWithoutFuture(system, ActorVfdtVisitorCountDepth.class, node, c);
            return c.get();
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }



    public ActorVfdtVisitorCountDepth(VfdtNode root, CompletableFuture<Map<Integer,Long>> finisher) {
        super(root, new VfdtMessages.VfdtTraversalState<>(new TraversalCountVisitor()), null);
        this.result = finisher;
    }

    @Override
    public void preStart() throws Exception {
        super.preStart();
        initState.visitor.setCountActor(self());
    }


    @Override
    public Receive createReceive() {
        return builder()
                .match(CountUp.class, this::countUp)
                .match(String.class, this::ask)
                .build();
    }

    public void countUp(CountUp m) {
        add(m.depth, m.count);
    }

    public void ask(String s) {
        sender().tell(s, self());
    }

    public void add(int depth, long count) {
        depthToNodes.compute(depth, (k,v) -> v == null ? count : v + count);
    }

    @Override
    public void end(TraversalMessages.TraversalEventEnd<TraversalCountVisitor> e) {
        super.end(e);
        result.complete(this.depthToNodes);
    }

    public static class TraversalCountVisitor extends VfdtNode.NodeVisitor implements Serializable {
        protected ActorRef countActor;
        protected int depth;

        public TraversalCountVisitor() {}

        public TraversalCountVisitor(ActorRef countActor) {
            this.countActor = countActor;
        }

        public void setCountActor(ActorRef countActor) {
            this.countActor = countActor;
        }

        @Override
        public boolean visit(VfdtNode node) {
            countActor.tell(new CountUp(depth, 1L), ActorRef.noSender());
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

        @Override
        public String toString() {
            return "TraversalVisitor{" +
                    "countActor=" + countActor +
                    ", depth=" + depth +
                    '}';
        }
    }

    public static class CountUp implements Serializable {
        public int depth;
        public long count;

        public CountUp() {}

        public CountUp(int depth, long count) {
            this.depth = depth;
            this.count = count;
        }
    }
}
