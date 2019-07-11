package csl.dataproc.actor.birch;

import akka.actor.*;
import akka.pattern.PatternsCS;
import akka.util.Timeout;
import csl.dataproc.actor.visit.TraversalVisitActor;
import csl.dataproc.birch.BirchNode;
import csl.dataproc.birch.BirchNodeCFEntry;
import csl.dataproc.birch.DendrogramPurityUnit;

import java.io.PrintWriter;
import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

public class ActorDendrogramPurityUnit extends DendrogramPurityUnit {
    ActorSystem system;
    PrintWriter additionalOutput;

    public ActorDendrogramPurityUnit(ActorSystem system, PurityInput input) {
        super(input);
        this.system = system;
    }

    public void setAdditionalOutput(PrintWriter additionalOutput) {
        this.additionalOutput = additionalOutput;
    }

    public PrintWriter getAdditionalOutput() {
        return additionalOutput;
    }

    @Override
    public void log(String s) {
        super.log(s);
        if (additionalOutput != null) {
            additionalOutput.println(s);
        }
    }

    @Override
    public void constructTable(BirchNode root) {
        idToTable = new HashMap<>();

        PurityConstructionHostRef hostRef = new PurityConstructionHostRef(
                system.actorOf(Props.create(PurityConstructionHostActor.class, this))); //local actor with local reference "this"

        try {
            TraversalVisitActor.start(system, TableConstructionActor.class, root, hostRef, classSize, selectedClasses).get(); //can be remote
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }

        hostRef.getRef().tell(PoisonPill.getInstance(), ActorRef.noSender());
    }

    public static class PurityConstructionHostActor extends AbstractActor {
        protected DendrogramPurityUnit unit;

        public PurityConstructionHostActor(DendrogramPurityUnit unit) {
            this.unit = unit;
        }

        @Override
        public Receive createReceive() {
            return receiveBuilder()
                    .match(DendrogramPurityUnit.SubTreeTable.class, this::put)
                    .match(BirchMessages.PurityConstructLoad.class, this::load)
                    .match(BirchMessages.PurityFinishNode.class, this::finishNode)
                    .build();
        }

        public void put(DendrogramPurityUnit.SubTreeTable s) {
            unit.constructTableNode(s.getId(), s);
        }

        public void load(BirchMessages.PurityConstructLoad m) {
            List<DataPoint> dps = unit.constructDataPoints(m.id, m.entry);
            sender().tell(dps, self());
        }

        public void finishNode(BirchMessages.PurityFinishNode m) {
            unit.constructFinishNode(m.id);
        }
    }

    public static class PurityConstructionHostRef implements DendrogramPurityUnit.TableConstructionHost {
        protected ActorRef ref;

        public PurityConstructionHostRef(ActorRef ref) {
            this.ref = ref;
        }

        @Override
        public void put(long l, SubTreeTable subTreeTable) {
            ref.tell(subTreeTable, ActorRef.noSender());
        }

        @Override
        @SuppressWarnings("unchecked")
        public List<DataPoint> load(long id, BirchNodeCFEntry e) {
            Timeout t = new Timeout(5, TimeUnit.MINUTES);
            try {
                return (List<DataPoint>) PatternsCS.ask(ref, new BirchMessages.PurityConstructLoad(id, e), t)
                        .toCompletableFuture().get();
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }

        @Override
        public void finishNode(long l) {
            ref.tell(new BirchMessages.PurityFinishNode(l), ActorRef.noSender());
        }

        public ActorRef getRef() {
            return ref;
        }
    }

    public static class TableConstructionActor extends TraversalVisitActor<BirchNode, ActorBirchVisitorTableConstruction> {
        public TableConstructionActor(BirchNode root, PurityConstructionHostRef hostRef, int classSize, Set<Integer> selectedClasses, CompletableFuture<ActorBirchVisitorTableConstruction> finisher) {
            super(root, new BirchMessages.BirchTraversalState<>(new ActorBirchVisitorTableConstruction(hostRef, classSize, selectedClasses)), finisher);
        }
    }


    public static class ActorBirchVisitorTableConstruction extends BirchVisitorTableConstruction
        implements Serializable {
        public ActorBirchVisitorTableConstruction(TableConstructionHost host, int classSize, Set<Integer> selectedClasses) {
            super(host, classSize, selectedClasses);
        }
    }
}
