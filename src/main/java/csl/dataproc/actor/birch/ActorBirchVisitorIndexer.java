package csl.dataproc.actor.birch;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import csl.dataproc.actor.visit.ActorVisitor;
import csl.dataproc.actor.visit.TraversalMessages;
import csl.dataproc.actor.visit.TraversalVisitActor;
import csl.dataproc.birch.BirchNode;
import csl.dataproc.birch.BirchNodeCFTree;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

public class ActorBirchVisitorIndexer extends TraversalVisitActor<BirchNode, ActorBirchVisitorIndexer.TravIndexer> {
    protected List<long[]> paths = new ArrayList<>();

    public static List<long[]> run(ActorSystem system, BirchNode node, boolean debug) {
        CompletableFuture<TravIndexer> f = TraversalVisitActor.start(system, ActorBirchVisitorIndexer.class, node, debug);
        try {
            return f.get().paths;
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    public ActorBirchVisitorIndexer(BirchNode root, boolean debug, CompletableFuture<TravIndexer> f) {
        super(root, new BirchMessages.BirchTraversalState<>(new TravIndexer(debug)), f);
    }

    @Override
    public void preStart() throws Exception {
        super.preStart();
        initState.visitor.pathReceiver = self();
    }

    @Override
    public Receive createReceive() {
        return builder()
                .match(long[].class, this::addPath)
                .build();
    }

    public void addPath(long[] path) {
        paths.add(path);
    }

    @Override
    public void end(TraversalMessages.TraversalEventEnd<TravIndexer> e) {
        e.visitor.paths = paths;
        super.end(e);
    }

    public static class TravIndexer extends BirchNode.BirchVisitor implements Serializable, ActorVisitor<ActorBirchNodeActor> {
        protected long index;
        protected LinkedList<Long> stack = new LinkedList<>();
        public boolean debug;
        public ActorRef pathReceiver;

        public List<long[]> paths; //for result

        public TravIndexer() {}

        public TravIndexer(boolean debug) {
            this.debug = debug;
        }

        public TravIndexer(boolean debug, long index, ActorRef pathReceiver) {
            this.debug = debug;
            this.index = index;
            this.pathReceiver = pathReceiver;
        }

        @Override
        public void visitActor(ActorBirchNodeActor actor) { }

        @Override
        public boolean visit(BirchNode node) {
            if (node instanceof ActorBirchConfig.Indexed) {
                if (debug) {
                    System.err.printf("#index %,d : %s\n", index, node);
                }
                ((ActorBirchConfig.Indexed) node).setChildIndex(index);
                ++index;

                if (pathReceiver != null) {
                    pathReceiver.tell(getCurrentPath(), ActorRef.noSender());
                }
            }
            return true;
        }

        @Override
        public boolean visitTree(BirchNodeCFTree t) {
            super.visitTree(t);
            if (t instanceof ActorBirchConfig.Indexed) {
                stack.addFirst(((ActorBirchConfig.Indexed) t).getChildIndex());
            }
            return true;
        }

        @Override
        public void visitAfterTree(BirchNodeCFTree t) {
            if (t instanceof ActorBirchConfig.Indexed) {
                stack.removeFirst();
            }
        }

        @Override
        public void visitTreeChild(BirchNodeCFTree tree, int i, BirchNodeCFTree.NodeHolder nodeHolder) {
            if (nodeHolder.node instanceof ActorBirchRefNode) {
                if (debug) {
                    System.err.printf("#index %,d * : %s\n", index, nodeHolder.node);
                }
                ((ActorBirchRefNode) nodeHolder.node).setChildIndex(index);
                ++index;

                if (pathReceiver != null) {
                    pathReceiver.tell(getCurrentPath(), ActorRef.noSender());
                }
            }
        }

        public long[] getCurrentPath() {
            long[] path = new long[stack.size()];
            int i = 0;
            for (Long n : stack) {
                path[i] = n;
                ++i;
            }
            return path;
        }
    }

}
