package csl.dataproc.actor.vfdt;

import akka.actor.Actor;
import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.japi.Creator;
import csl.dataproc.actor.remote.RemoteManager;
import csl.dataproc.csv.arff.ArffAttributeType;
import csl.dataproc.vfdt.*;

import java.io.Serializable;
import java.util.List;

public class ActorVfdtConfig extends VfdtConfig implements Serializable, RemoteManager.SharedConfig {
    protected transient ActorSystem system;
    public static ActorVfdtConfig instance;

    public static ActorSystem globalSystem;

    public ActorVfdtConfig() {
        checkSystem();
    }

    public ActorVfdtConfig(ActorSystem system) {
        this.system = system;
        if (instance == null) {
            instance = this;
        }
    }

    @Override
    public void arrivedAtRemoteNode() {
        checkSystem();
        instance = this;
    }

    private void checkSystem() {
        if (system == null) {
            system = globalSystem;
        }
    }

    public ActorSystem getSystem() {
        checkSystem();
        return system;
    }

    public <T extends Actor> ActorRef create(Class<T> actorCls, Creator<T> props) {
        try {
            return RemoteManager.create(system, actorCls, props);
        } catch (Exception ex) {
            System.err.println("create local actor: " + ex);
            return system.actorOf(Props.create(props));
        }
    }

    @Override
    public VfdtNode newRoot(List<ArffAttributeType> types) {
        return new ActorVfdtRefNode(create(ActorVfdtNodeActor.class,
                new RootCreator(this, types)), 0); //root always becomes an actor
    }


    @Override
    public VfdtNode newSplitContinuous(SplitContinuousParameters p) {
        VfdtNodeGrowingSplitter.SplitterAttrAndTotalToNode leftGen = new VfdtNodeGrowingSplitter.SplitterAttrAndTotalToNode(this,
                1, 1, p.getParentClass(), p.getParentErrorRate(), p.getPathActiveAttributes(), p.getTypes(), p.getDepth() + 1);
        VfdtNodeGrowingSplitter.SplitterAttrAndTotalToNode rightGen = new VfdtNodeGrowingSplitter.SplitterAttrAndTotalToNode(this,
                1, 1, p.getParentClass(), p.getParentErrorRate(), p.getPathActiveAttributes(), p.getTypes(), p.getDepth() + 1);

        return new VfdtNodeSplitContinuous(p.getInstanceCount(), p.getSplitAttributeIndex(), p.getDepth(), p.getThreshold(),
                wrapGenerator(0, p.getLeftCount(), p.getSplitAttributeIndex(), p.getDepth() + 1, leftGen)
                        .create(p.getSplitAttributeIndex(), p.getLeftCount()),
                wrapGenerator(1, p.getRightCount(), p.getSplitAttributeIndex(), p.getDepth() + 1, rightGen)
                        .create(p.getSplitAttributeIndex(), p.getRightCount()));
    }

    public static class RootCreator implements Serializable, Creator<ActorVfdtNodeActor> {
        protected transient ActorVfdtConfig config;
        protected List<ArffAttributeType> types;

        public RootCreator() {
            config = ActorVfdtConfig.instance;
        }

        public RootCreator(ActorVfdtConfig config, List<ArffAttributeType> types) {
            this.config = config;
            this.types = types;
        }

        @Override
        public ActorVfdtNodeActor create() {
            return new ActorVfdtNodeActor(new VfdtNodeGrowing(config, types), 0);
        }
    }

    @Override
    public VfdtNode newSplitDiscrete(SplitDiscreteParameters p) {
        VfdtNodeSplitDiscrete.AttrAndTotalToNode gen2 = wrapGenerator(0, p.getInstanceCount(), p.getSplitAttributeIndex(),
                p.getDepth() + 1, p.getGenerator());
        return new VfdtNodeSplitDiscrete(p.getInstanceCount(), p.getSplitAttributeIndex(), p.getDepth(), gen2,
                VfdtNodeSplitDiscrete.create(p.getDiscreteClassCount(), gen2));
    }


    public VfdtNodeSplitDiscrete.AttrAndTotalToNode wrapGenerator(long defaultDiscreteAttr, long instanceCount, int splitAttributeIndex,
                                                                  int depth, VfdtNodeSplitDiscrete.AttrAndTotalToNode generator) {
        return new ActorAttrAndTotalToNode(defaultDiscreteAttr, instanceCount, depth, this, generator);
    }

    public static class ActorAttrAndTotalToNode implements VfdtNodeSplitDiscrete.AttrAndTotalToNode, Serializable, Creator<ActorVfdtNodeActor> {
        protected long discreteAttr; //optional
        protected long instanceCount;
        protected int depth;
        protected transient ActorVfdtConfig config;
        protected VfdtNodeSplitDiscrete.AttrAndTotalToNode generator;

        public ActorAttrAndTotalToNode() {
            config = ActorVfdtConfig.instance;
        }

        public ActorAttrAndTotalToNode(long defaultDiscreteAttr, long defaultInstanceCount, int depth, ActorVfdtConfig config, VfdtNodeSplitDiscrete.AttrAndTotalToNode generator) {
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
            return new ActorVfdtRefNode(config.create(ActorVfdtNodeActor.class, this), depth);
        }

        @Override
        public ActorVfdtNodeActor create() {
            return new ActorVfdtNodeActor(createOriginal(), depth);
        }

        public VfdtNode createOriginal() {
            return generator.create(discreteAttr, instanceCount);
        }
    }
}
