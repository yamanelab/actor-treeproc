package csl.dataproc.actor.vfdt;

import akka.actor.ActorSystem;
import csl.dataproc.actor.visit.ActorVisitor;
import csl.dataproc.actor.visit.TraversalVisitActor;
import csl.dataproc.vfdt.VfdtNode;

import java.util.Map;
import java.util.TreeMap;

public class ActorVfdtVisitorLeafMsgCount {

    public static Map<String,ActorVfdtNodeActor.LeafMessageCount> run(ActorSystem system, VfdtNode root) {
        try {
            return TraversalVisitActor.start(system, root, new VfdtMessages.VfdtTraversalState<>(new MsgVisitor()))
                    .get()
                    .getCounts();
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    public static class MsgVisitor extends VfdtNode.NodeVisitor implements ActorVisitor<ActorVfdtNodeActor> {
        protected Map<String,ActorVfdtNodeActor.LeafMessageCount> counts = new TreeMap<>();

        public Map<String, ActorVfdtNodeActor.LeafMessageCount> getCounts() {
            return counts;
        }

        @Override
        public void visitActor(ActorVfdtNodeActor actor) {
            actor.counts.forEach((k,v) ->
                    counts.computeIfAbsent(k, ActorVfdtNodeActor.LeafMessageCount::new)
                        .add(v));
        }

        @Override
        public String toString() {
            return "V(" + counts.values()  +")@" + Integer.toHexString(System.identityHashCode(this));
        }
    }
}
