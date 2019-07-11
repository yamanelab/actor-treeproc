package csl.dataproc.actor.vfdt.replica;

import akka.actor.ActorRef;
import akka.dispatch.ControlMessage;
import csl.dataproc.actor.vfdt.VfdtMessages;
import csl.dataproc.vfdt.VfdtNode;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class ReplicaMessages {
    public static List<Class<?>> getTypes() {
        List<Class<?>> cls = new ArrayList<>();
        cls.addAll(Arrays.asList(ReplicaMessages.class.getClasses()));
        cls.add(ReplicaNodeGrowing.class);
        cls.add(ReplicaVfdtConfig.class);
        cls.add(ReplicaVfdtConfig.RootCreator.class);
        cls.add(ReplicaVfdtConfig.ActorAttrAndTotalToNode.class);
        return cls;
    }

    public static class Pair implements Serializable, ControlMessage {
        public ActorRef pair;

        public Pair() { }

        public Pair(ActorRef pair) {
            this.pair = pair;
        }
    }

    public static class UpdateSplitResponse implements Serializable, ControlMessage { }

    public static UpdateSplitFail splitFailCountCheck = new UpdateSplitFail(false);
    public static UpdateSplitFail splitFailMerge = new UpdateSplitFail(true);
    public static UpdateSplitFail splitFailIndexCheck = new UpdateSplitFail(true);

    public static class UpdateSplitFail extends UpdateSplitResponse implements Serializable, ControlMessage {
        public boolean clearCount;

        public UpdateSplitFail() { }

        public UpdateSplitFail(boolean clearCount) {
            this.clearCount = clearCount;
        }
    }

    public static class UpdateSet extends UpdateSplitResponse implements Serializable, ControlMessage {
        public VfdtNode state;

        public UpdateSet() { }

        public UpdateSet(VfdtNode state) {
            this.state = state;
        }
    }

    public static class UpdateSplit implements Serializable, ControlMessage {
        public VfdtNode state;

        public UpdateSplit() { }

        public UpdateSplit(VfdtNode state) {
            this.state = state;
        }
    }

    public static class UpdateMerge implements Serializable, ControlMessage {
        public VfdtNode state;

        public UpdateMerge() { }

        public UpdateMerge(VfdtNode state) {
            this.state = state;
        }
    }

    public static class AddPair implements Serializable {
        public VfdtMessages.Add add;
        public ActorRef pair;
        public long pairId;

        public AddPair() { }

        public AddPair(VfdtMessages.Add add, ActorRef pair, long pairId) {
            this.add = add;
            this.pair = pair;
            this.pairId = pairId;
        }
    }
//
//    public static class DotPair implements Serializable {
//        public ActorRef dotActor;
//
//        public DotPair() { }
//
//        public DotPair(ActorRef dotActor) {
//            this.dotActor = dotActor;
//        }
//    }

}
