package csl.dataproc.actor.vfdt.replica;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.japi.Creator;
import csl.dataproc.actor.vfdt.ActorVfdtConfig;
import csl.dataproc.actor.vfdt.ActorVfdtRefNode;
import csl.dataproc.csv.arff.ArffAttributeType;
import csl.dataproc.vfdt.VfdtNode;
import csl.dataproc.vfdt.VfdtNodeSplitDiscrete;

import java.io.Serializable;
import java.util.List;

public class ReplicaVfdtConfig extends ActorVfdtConfig {
    public double splitMergeSizeFactor = 1;

    public ReplicaVfdtConfig() { }

    public ReplicaVfdtConfig(ActorSystem system) {
        super(system);
    }

    @Override
    public VfdtNode newGrowing(GrowingParameters p) {
        return new ReplicaNodeGrowing(this, p.getTypes(), p.getInstanceCountClassTotal(), p.getParentClass(), p.getPathActiveAttributes(), p.getParentErrorRate(), p.getDepth());
    }


    public ActorRef createRoot(List<ArffAttributeType> types) {
        return create(ReplicaVfdtNodeActor.class,
                new ReplicaRootCreator(this, types));
    }

    @Override
    public VfdtNode newRoot(List<ArffAttributeType> types) {
        return new ActorVfdtRefNode(create(ReplicaVfdtNodeActor.class,
                new ReplicaRootCreator(this, types)), 0);
    }


//    @Override
//    public VfdtNodeSplitDiscrete.AttrAndTotalToNode wrapGenerator(long defaultDiscreteAttr, long instanceCount, int splitAttributeIndex,
//                                                                  int depth, VfdtNodeSplitDiscrete.AttrAndTotalToNode generator) {
//        return new ReplicaAttrAndTotalToNode(defaultDiscreteAttr, instanceCount, depth, this, generator);
//    }

    public static class ReplicaRootCreator implements Serializable, Creator<ReplicaVfdtNodeActor> {
        protected transient ActorVfdtConfig config;
        protected List<ArffAttributeType> types;

        public ReplicaRootCreator() {
            config = ActorVfdtConfig.instance;
        }

        public ReplicaRootCreator(ActorVfdtConfig config, List<ArffAttributeType> types) {
            this.config = config;
            this.types = types;
        }

        @Override
        public ReplicaVfdtNodeActor create() {
            return new ReplicaVfdtNodeActor(new ReplicaNodeGrowing(config, types), 0);
        }
    }

    public static class ReplicaAttrAndTotalToNode implements VfdtNodeSplitDiscrete.AttrAndTotalToNode, Serializable, Creator<ReplicaVfdtNodeActor> {
        protected long discreteAttr; //optional
        protected long instanceCount;
        protected int depth;
        protected transient ActorVfdtConfig config;
        protected VfdtNodeSplitDiscrete.AttrAndTotalToNode generator;

        public ReplicaAttrAndTotalToNode() {
            config = ActorVfdtConfig.instance;
        }

        public ReplicaAttrAndTotalToNode(long defaultDiscreteAttr, long defaultInstanceCount, int depth, ActorVfdtConfig config, VfdtNodeSplitDiscrete.AttrAndTotalToNode generator) {
            this.discreteAttr = defaultDiscreteAttr;
            this.instanceCount = defaultInstanceCount;
            this.depth = depth;
            this.config = config;
            this.generator = generator;
        }

        @Override
        public VfdtNode create(long attr, long total) {
            this.discreteAttr = attr;
            this.instanceCount = total;
            return new ActorVfdtRefNode(config.create(ReplicaVfdtNodeActor.class, this), depth);
        }

        @Override
        public ReplicaVfdtNodeActor create() {
            return new ReplicaVfdtNodeActor(createOriginal(), depth);
        }

        public VfdtNode createOriginal() {
            return generator.create(discreteAttr, instanceCount);
        }
    }
}
