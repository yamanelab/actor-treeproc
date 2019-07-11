package csl.dataproc.actor.vfdt.replica;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.pattern.PatternsCS;
import akka.util.Timeout;
import csl.dataproc.actor.vfdt.ActorVfdtNodeActor;
import csl.dataproc.actor.vfdt.ActorVfdtVisitorDot;
import csl.dataproc.actor.vfdt.VfdtMessages;
import csl.dataproc.actor.visit.TraversalVisitActor;
import csl.dataproc.dot.DotGraph;
import csl.dataproc.vfdt.VfdtNode;

import java.nio.file.Path;
import java.util.concurrent.TimeUnit;

public class ReplicaVfdtVisitorDot extends ActorVfdtVisitorDot {
    public DotGraph run(ActorSystem system, VfdtNode root, Path dotFile) {
        try {
            init(system, dotFile);
            TraversalVisitActor.startWithoutFuture(system, ReplicaDotActor.class, this, root);
            return result(system);
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    public static class ReplicaDotActor extends DotActor {
        public ReplicaDotActor(ActorVfdtVisitorDot host, VfdtNode root) {
            this(host, root, new ReplicaDotVisitor());
        }

        public ReplicaDotActor(ActorVfdtVisitorDot host, VfdtNode root, DotTraversalVisitor visitor) {
            super(host, root, visitor);
        }

        @Override
        public Receive createReceive() {
            return receiveBuilder()
                    .match(ReplicaMessages.AddPair.class, this::addPair)
                    .build()
                    .orElse(super.createReceive());
        }

        public void addPair(ReplicaMessages.AddPair add) {
            VfdtMessages.Add m = add.add;
            System.err.println("dot id=" + m.id + " pair=" + add.pair);
            host.add(m.childIndex, m.id, m.label, true, m.box);

            host.add(-1, add.pairId, "id=" + add.pairId + "(pair=" + m.id + ")", false, m.box);

            if (!m.push) {
                host.visitAfterSplit(null);
            }
        }
    }

    public static class ReplicaDotVisitor extends DotTraversalVisitor {
        protected long pairId = -1;
        protected ActorRef pair;

        public ReplicaDotVisitor() { }

        public ReplicaDotVisitor(ActorRef ref) {
            super(ref);
        }

        @Override
        public void visitActor(ActorVfdtNodeActor actor) {
            super.visitActor(actor);
            pairId = -1;
            pair = null;
            if (actor instanceof ReplicaVfdtNodeActor) {
                pair = ((ReplicaVfdtNodeActor) actor).pair;
                if (pair != null) {
                    try {
                        pairId = (Long) PatternsCS.ask(pair, new VfdtMessages.GetId(), new Timeout(5, TimeUnit.MINUTES))
                                .toCompletableFuture().get();
                    } catch (Exception ex) {
                        throw new RuntimeException(ex);
                    }
                }
            }
        }

        @Override
        protected void add(String label, boolean push, boolean box) {
            VfdtMessages.Add add = new VfdtMessages.Add(lastId, "id=" + lastId + (pairId != -1 ? ",pair=" + pairId : "") + "," + label, push, box, childIndex);
            if (pair != null) {
                ref.tell(new ReplicaMessages.AddPair(add, pair, pairId), ActorRef.noSender());
            } else {
                ref.tell(add, ActorRef.noSender());
            }
        }
    }
}
