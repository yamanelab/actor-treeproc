package csl.dataproc.actor.vfdt;

import akka.actor.ActorRef;
import akka.dispatch.ControlMessage;
import csl.dataproc.actor.visit.TraversalMessages;
import csl.dataproc.actor.visit.TraversalStepTask;
import csl.dataproc.vfdt.*;
import csl.dataproc.vfdt.count.*;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class VfdtMessages {

    public static class GetInstanceCount implements Serializable {
    }

    public static class PutDataLine implements Serializable {
        public ActorVfdtDataLine dataLine;

        public PutDataLine() {}

        public PutDataLine(ActorVfdtDataLine dataLine) {
            this.dataLine = dataLine;
        }
    }

    public static class LearnReturn implements Serializable {
        public VfdtNode.DataLine line;

        public LearnReturn() { }

        public LearnReturn(VfdtNode.DataLine line) {
            this.line = line;
        }
    }

    public static class LearnEnd implements Serializable {
        public long count;

        public LearnEnd() { }

        public LearnEnd(long count) {
            this.count = count;
        }
    }

    public static class StepClassify implements Serializable {
        public ActorVfdtDataLine dataLine;

        public StepClassify() {}

        public StepClassify(ActorVfdtDataLine dataLine) {
            this.dataLine = dataLine;
        }
    }

    public static class ClassifyReturn implements Serializable {
        public VfdtNode.DataLine line;
        public long classValue;

        public ClassifyReturn() {}

        public ClassifyReturn(VfdtNode.DataLine line, long classValue) {
            this.line = line;
            this.classValue = classValue;
        }
    }

    public static class ClassifyEnd implements Serializable {
        public long count;

        public ClassifyEnd() {}

        public ClassifyEnd(long count) {
            this.count = count;
        }
    }

    public static class EndCheck implements Serializable, ControlMessage {

    }

    public static class Accept implements Serializable {
        public VfdtNode.NodeVisitor visitor;

        public Accept() {}

        public Accept(VfdtNode.NodeVisitor visitor) {
            this.visitor = visitor;
        }
    }

    public static class End implements Serializable { }

    public static class GetId implements Serializable { }

    public static class Add implements Serializable {
        public long id;
        public String label;
        public boolean push;
        public boolean box;
        public int childIndex;

        public Add() {}

        public Add(long id, String label, boolean push, boolean box, int childIndex) {
            this.id = id;
            this.label = label;
            this.push = push;
            this.box = box;
            this.childIndex = childIndex;
        }
    }

    public static class Pop implements Serializable {

    }

    public static class WriteCsv implements Serializable {
        public String path;
        public String data;

        public WriteCsv() {
        }

        public WriteCsv(String path, String data) {
            this.path = path;
            this.data = data;
        }
    }

    public static class WriteJson implements Serializable {
        public String path;
        public Object json;

        public WriteJson() {
        }

        public WriteJson(String path, Object json) {
            this.path = path;
            this.json = json;
        }
    }

    public static class CreateDirectories implements Serializable {
        public String path;

        public CreateDirectories() {}

        public CreateDirectories(String path) {
            this.path = path;
        }
    }

    public static class VfdtTraversalState<VisitorType extends VfdtNode.NodeVisitor> extends TraversalMessages.TraversalState<VfdtNode, VisitorType> {

        public VfdtTraversalState() { }

        public VfdtTraversalState(VisitorType visitor) {
            super(visitor);
        }

        @Override
        public TraversalStepTask<VfdtNode, VisitorType> getNextStepTaskNode(VfdtNode root) {
            List<Integer> idx = getNonActorIndexSubPath();
            VfdtNodeSplit parent = null;
            VfdtNode child = root;
            int index = 0;
            if (child != null) {
                for (int i : idx) {
                    index = i;
                    if (child instanceof VfdtNodeSplit) {
                        parent = ((VfdtNodeSplit) child);
                        List<VfdtNode> ns = parent.getChildNodes();
                        if (i < ns.size()) {
                            child = ns.get(i);
                        } else {
                            return getNextStepTaskNode(parent, null, index);
                        }
                    } else {
                        //error
                    }
                }
            }
            return getNextStepTaskNode(parent, child, index);
        }

        @Override
        public boolean visitEnter(VfdtNode child) {
            if (child instanceof VfdtNodeSplitDiscrete) {
                return visitor.visitSplitDiscrete((VfdtNodeSplitDiscrete) child);
            } else if (child instanceof VfdtNodeSplitContinuous) {
                return visitor.visitSplitContinuous((VfdtNodeSplitContinuous) child);
            } else {
                return child.accept(visitor); //leaf
            }
        }

        @Override
        public boolean visitChild(VfdtNode parent, int index, VfdtNode child) {
            visitor.visitSplitChild((VfdtNodeSplit) parent, index, child);
            return true;
        }

        @Override
        public TraversalMessages.TraversalNodeType getType(VfdtNode node) {
            if (node instanceof ActorVfdtRefNode) {
                return TraversalMessages.TraversalNodeType.Actor;
            } else if (node instanceof VfdtNodeSplit) {
                return TraversalMessages.TraversalNodeType.Parent;
            } else {
                return TraversalMessages.TraversalNodeType.Leaf;
            }
        }

        @Override
        public ActorRef getActor(VfdtNode refNode) {
            return ((ActorVfdtRefNode) refNode).getRef();
        }

        @Override
        public boolean visitExit(VfdtNode parent, VfdtNode.NodeVisitor visitor, boolean error) {
            if (parent != null) {
                if (parent instanceof VfdtNodeSplitDiscrete) {
                    visitor.visitAfterSplitDiscrete((VfdtNodeSplitDiscrete) parent);
                } else if (parent instanceof VfdtNodeSplitContinuous) {
                    visitor.visitAfterSplitContinuous((VfdtNodeSplitContinuous) parent);
                }
            }
            return true;
        }
    }

    public static List<Class<?>> getTypes() {
        List<Class<?>> cls = new ArrayList<>();
        cls.addAll(Arrays.asList(
                VfdtConfig.class,
                VfdtConfig.GrowingParameters.class,
                VfdtConfig.LeafParameters.class,
                VfdtConfig.SplitContinuousParameters.class,
                VfdtConfig.SplitDiscreteParameters.class,

                VfdtNodeGrowing.class,
                VfdtNodeGrowingSplitter.class,
                VfdtNodeGrowingSplitter.SplitterAttrAndTotalToNode.class,
                VfdtNodeGrowingSplitter.CheckSplitResultIgnores.class,
                VfdtNodeGrowingSplitter.CheckSplitResultSimple.class,
                VfdtNodeGrowingSplitter.CheckSplitResultSplit.class,
                VfdtNodeGrowingSplitter.LeafifyIndex.class,

                VfdtNodeLeaf.class,
                VfdtNodeSplitContinuous.class,
                VfdtNodeSplitDiscrete.class,

                VfdtVisitorStat.class,
                VfdtBatch.AnswerStat.class,

                VfdtNode.ArrayDataLine.class,
                SparseDataLine.class,
                SparseDataLine.SparseDataColumn.class,

                AttributeContinuousClassCount.class,
                AttributeContinuousSplit.class,
                AttributeDiscreteClassCountArray.class,
                AttributeDiscreteClassCountHash.class,
                AttributeClassCount.AttributeIgnoreClassCount.class,
                AttributeClassCount.GiniIndex.class,
                AttributeStringClassCount.class,
                LongCount.class,
                LongCount.DoubleCount.class
        ));
        cls.addAll(Arrays.asList(
                ActorVfdtConfig.class,
                ActorVfdtConfig.ActorAttrAndTotalToNode.class,
                ActorVfdtConfig.RootCreator.class,
                ActorVfdtDataLine.class,
                ActorVfdtVisitorCountDepth.CountUp.class,
                ActorVfdtVisitorDot.DotTraversalVisitor.class,
                ActorVfdtVisitorCountDepth.TraversalCountVisitor.class,
                ActorVfdtVisitorStat.TraversalStatVisitor.class,
                ActorVfdtVisitorLeafMsgCount.MsgVisitor.class,
                ActorVfdtNodeActor.class,
                ActorVfdtBatch.VfdtTestSession.class
        ));

        cls.addAll(Arrays.asList(VfdtMessages.class.getClasses()));
        return cls;
    }
}
