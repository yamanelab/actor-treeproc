package csl.dataproc.actor.vfdt;

import akka.actor.AbstractActor;
import akka.actor.ActorPath;
import akka.actor.PoisonPill;
import csl.dataproc.actor.remote.RemoteManager;
import csl.dataproc.actor.stat.StatMailbox;
import csl.dataproc.actor.stat.StatMessages;
import csl.dataproc.actor.stat.StatReceive;
import csl.dataproc.actor.stat.StatSystem;
import csl.dataproc.actor.visit.TraversalMessages;
import csl.dataproc.vfdt.*;
import csl.dataproc.vfdt.count.*;
import scala.PartialFunction;
import scala.runtime.BoxedUnit;

import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Map;
import java.util.TreeMap;

public class ActorVfdtNodeActor extends AbstractActor {
    protected VfdtNode state;
    protected StatMailbox.StatQueue queue;
    protected StatReceive receiveStat;
    protected int depth;

    protected Map<String, LeafMessageCount> counts = new TreeMap<>();
    protected Map<Long,VfdtBatch.AnswerStat> answerStatMap = new TreeMap<>();

    public ActorVfdtNodeActor(VfdtNode state) {
        this(state, 0);
    }

    public ActorVfdtNodeActor(VfdtNode state, int depth) {
        this.depth = depth;
        this.state = state;
        StatSystem s = StatSystem.system();
        if (s.isEnabled()) {
            initStatReceive(s, -1L, null);
        }
    }

    @Override
    public void preStart() throws Exception {
        StatSystem s = StatSystem.system();
        if (s.isEnabled()) {
            StatReceive ex = receiveStat;
            initStatReceive(s, s.getId(context().system(), self().path().toStringWithoutAddress()), self().path());
            if (ex != null && receiveStat != null) {
                receiveStat.add(ex);
            }
        }
    }

    protected void initStatReceive(StatSystem s, long id, ActorPath path) {
        receiveStat = new StatReceive(id, path);
    }


    @Override
    public void aroundReceive(PartialFunction<Object, BoxedUnit> receive, Object msg) {
        try {
            if (receiveStat != null) {
                receiveStat.receiveBefore(msg);
            }
            super.aroundReceive(receive, msg);
        } finally {
            if (receiveStat != null) {
                receiveStat.receiveAfter(msg, state.getClass().getName());
            }
        }
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(VfdtMessages.GetInstanceCount.class, this::getInstanceCount)
                .match(VfdtMessages.PutDataLine.class, this::putDataLine)
                .match(VfdtMessages.StepClassify.class, this::stepClassify)
                .match(VfdtMessages.Accept.class, this::accept)
                .match(StatMessages.ActorId.class, this::actorId)
                .match(VfdtMessages.End.class, this::end)
                .match(VfdtMessages.GetId.class, this::getId)
                .match(TraversalMessages.TraversalState.class, this::traverse)
                .build();
    }

    public void getInstanceCount(VfdtMessages.GetInstanceCount m) {
        sender().tell(state.getInstanceCount(), self());
    }

    public void putDataLine(VfdtMessages.PutDataLine m) {
        boolean leaf = isLeaf();
        //currently VfdtNodeGrowing only supports SparseDataLine and ArrayDataLine
        VfdtNode pre = state;
        state = state.put(leaf ? m.dataLine.line : m.dataLine);
        if (state != pre && depth == 0) {
            //root state change
            saveStatChangeState("root", pre, state);
        }
        if (leaf) {
            countUp(m);
            if (m.dataLine.target != null) {
                m.dataLine.target.tell(new VfdtMessages.LearnReturn(m.dataLine), self());
            }
        }
    }

    public boolean isLeaf() {
        return !(state instanceof VfdtNodeSplit);
    }

    public void stepClassify(VfdtMessages.StepClassify m) {
        VfdtNode next = state.stepClassify(m.dataLine);
        if (next == null || next == state) {
            countUp(m);
            long cls = state.getClassValue();
            answerStat(m.dataLine.line, cls);
            if (m.dataLine.target != null) {
                m.dataLine.target.tell(new VfdtMessages.ClassifyReturn(m.dataLine, cls), self());
            }
        } else if (next instanceof ActorVfdtRefNode) {
            next.stepClassify(m.dataLine);
        }
    }

    public void answerStat(VfdtNode.DataLine dataLine, long cls) {
        if (ActorVfdtBatch.testSession != null) {
            long ans = dataLine.getLong(ActorVfdtBatch.testSession.types.size() - 1);
            answerStatMap.computeIfAbsent(ans, VfdtBatch.AnswerStat::new).put(cls);
        }
    }

    public void countUp(Object msg) {
        counts.computeIfAbsent(msg.getClass().getName(), LeafMessageCount::new)
                .increment();
    }

    public void accept(VfdtMessages.Accept m) {
        sender().tell(state.accept(m.visitor), self());
    }

    public void actorId(StatMessages.ActorId m) {
        this.queue = m.queue;
        if (queue != null && queue.getId() == -1 && receiveStat != null) {
            queue.setId(receiveStat.getId());
        }
    }

    public void end(VfdtMessages.End t) {
        if (queue != null) {
            log("end actor: id=" + queue.getId() + " : " + self().path());
            this.queue.checkAndSave(true);
        }
        if (receiveStat != null) {
            this.receiveStat.saveTotal();
        }

        ActorVfdtBatch.end(state);

        //sender().tell(t, self());
        self().tell(PoisonPill.getInstance(), self());
    }

    public void log(String str) {
        RemoteManager m = RemoteManager.instance;
        if (m != null) {
            m.log(str);
        } else {
            System.err.println("[" + LocalDateTime.now() + "] " + str);
        }
    }

    public void getId(VfdtMessages.GetId m) {
        sender().tell(receiveStat == null ? -1 : receiveStat.getId(), self());
    }

    public long getId() {
        return receiveStat != null ? receiveStat.getId() : -1;
    }

    public Map<Long, VfdtBatch.AnswerStat> getAnswerStatMap() {
        return answerStatMap;
    }

    public static class LeafMessageCount implements Serializable {
        public String type;
        public long count;
        public Instant time;

        public LeafMessageCount() { }

        public LeafMessageCount(String type) {
            this.type = type;
            time = Instant.EPOCH;
        }

        public LeafMessageCount(String type, long count, Instant time) {
            this.type = type;
            this.count = count;
            this.time = time;
        }

        public void increment() {
            count++;
            time = Instant.now();
        }

        public void add(LeafMessageCount c) {
            this.count += c.count;
            if (c.time.isAfter(time)) {
                this.time = c.time;
            }
        }

        @Override
        public String toString() {
            String s = type;
            int d = s.lastIndexOf('.');
            if (d != -1) {
                int dol = s.lastIndexOf('$');
                if (dol > d) {
                    d = dol;
                }
                s = s.substring(d + 1);
            }
            return s + ":" + count + ":" +
                    LocalDateTime.ofInstant(time, ZoneId.systemDefault()).toLocalTime();
        }
    }


    ///////////////

    public void traverse(TraversalMessages.TraversalState<VfdtNode,VfdtNode.NodeVisitor> s) {
        s.accept(this, state);
    }

    public void saveStatChangeState(String msg, VfdtNode pre, VfdtNode post) {
        StatSystem s = StatSystem.system();
        if (s.isEnabled()) {
            try {
                Files.write(s.getFile("root-change-%d.txt", getId()),
                        toStrChangeState(msg, pre, post).getBytes(StandardCharsets.UTF_8));
            } catch (Exception ex) {
                log("error root change write: " + self() + " " + ex);
            }
        }
    }

    public String toStrChangeState(String msg, VfdtNode pre, VfdtNode post) {
        StringBuilder buf = new StringBuilder();
        buf.append(msg).append(" state change: id=").append(getId()).append(", ").append(counts.values()).append("\n");
        if (pre != null && pre instanceof VfdtNodeGrowing) {
            VfdtNodeGrowing g = (VfdtNodeGrowing) pre;
            long ic = g.getInstanceCount();
            //recompute splitting
            VfdtNodeGrowingSplitter.CheckSplitResult r = g.getSplitter().checkSplit(g, false);

            buf.append(" change state pre: ").append(g)
                    .append(String.format(", count=%,d, hoeff=%.6f", g.getInstanceCount(), g.hoeffdingBound(!g.getConfig().useGini)));
            buf.append("\n   result: ").append(toStr(g, r));
            int i = 0;
            for (AttributeClassCount count : g.getAttributeCounts()) {
                AttributeClassCount.GiniIndex[] is = count.giniIndex(ic, g.getClasses(), !g.getConfig().useGini);
                buf.append("\n   attr[").append(i).append("] ------------------------");
                if (is.length > 0) {
                    buf.append("\n    1st: ").append(toStr(g, is[0]));
                    if (is.length > 1) {
                        buf.append("\n    2nd: ").append(toStr(g, is[1]));
                    }
                }
                buf.append("\n    ").append(toStr(count));
                ++i;
            }
        }

        buf.append("\n   => ").append(post);
        if (post instanceof VfdtNodeSplitContinuous) {
            int i = 0;
            buf.append(" thr=").append(String.format("%.6f", ((VfdtNodeSplitContinuous) post).getThreshold()));
            for (VfdtNode cn : ((VfdtNodeSplit) post).getChildNodes()) {
                buf.append("\n   [").append(i).append("] ").append(cn);
                ++i;
            }
        } else if (post instanceof VfdtNodeSplitDiscrete) {
            VfdtNodeSplitDiscrete d = (VfdtNodeSplitDiscrete) post;
            int i = 0;
            for (VfdtNodeSplitDiscrete.NodeHolder h : d.getChildren()) {
                buf.append("\n  [").append(i).append("]  attr[").append(h.attrValue).append("] ").append(h.node);
                ++i;
            }
        }

        return buf.toString();
    }

    public static String toStr(VfdtNodeGrowing node, VfdtNodeGrowingSplitter.CheckSplitResult result) {
        if (result instanceof VfdtNodeGrowingSplitter.CheckSplitResultSplit) {
            AttributeClassCount.GiniIndex gi =((VfdtNodeGrowingSplitter.CheckSplitResultSplit) result).selected;
            return toStr(node, gi);
        } else if (result instanceof VfdtNodeGrowingSplitter.CheckSplitResultSimple) {
            return String.format("<%s>", ((VfdtNodeGrowingSplitter.CheckSplitResultSimple) result).name());
        } else if (result instanceof VfdtNodeGrowingSplitter.CheckSplitResultIgnores) {
            VfdtNodeGrowingSplitter.CheckSplitResultIgnores ignores = (VfdtNodeGrowingSplitter.CheckSplitResultIgnores) result;
            return String.format("<ignore: fstIdx+hoeff=%f ignoreAttrs=%s ignoreContSplits=%s",
                    ignores.firstAndHoeffdingBound, ignores.ignoreAttributes, ignores.ignoreContinuousSplit);
        } else {
            return result.toString();
        }
    }

    public static String toStr(VfdtNodeGrowing node, AttributeClassCount.GiniIndex gi) {
        int idx = node.getAttributeCountIndex(gi.sourceAttribute);
        return String.format("<attr[%d]: idx=%.6f, thr=%.6f>", idx, gi.index, gi.threshold);
    }

    public static String toStr(AttributeClassCount c) {
        if (c instanceof AttributeContinuousClassCount) {
            return toStr((AttributeContinuousClassCount) c);
        } else if (c instanceof AttributeDiscreteClassCount) {
            return toStr((AttributeDiscreteClassCount) c);
        } else if (c instanceof AttributeClassCount.AttributeIgnoreClassCount) {
            return "ignored";
        } else {
            return "" + c;
        }
    }

    public static String toStr(AttributeDiscreteClassCount count) {
        StringBuilder buf = new StringBuilder();
        buf.append("type=").append(count.getClass().getSimpleName());
        for (LongCount.LongKeyValueIterator att = count.attributeToClassesTotal();
                att.hasNext(); ) {
            long attr = att.getKey();
            long total = att.nextValue();
            buf.append("\n a=").append(attr).append("[").append(String.format(",%d", total)).append("]: ");
            for (LongCount.LongKeyValueIterator atc = count.classToCount(attr);
                 atc.hasNext(); ) {
                long cls = atc.getKey();
                long c = atc.nextValue();
                buf.append(" (").append(cls).append(":").append(String.format("%,d", c)).append(")");
            }
        }
        return buf.toString();
    }

    public static String toStr(AttributeContinuousClassCount count) {
        StringBuilder buf = new StringBuilder();
        buf.append("type=").append(count.getClass().getSimpleName());
        buf.append(", cTblSize=").append(count.getClassTableSize());
        buf.append(", addNew=").append(count.isAddingNewSplits());
        buf.append(", total=").append(toStr(count.getTotal()));
        buf.append(", splits=");
        int i = 0;
        for (AttributeContinuousSplit split : count.getSplits()) {
            buf.append(String.format("\n    %2d: ", i)).append(toStr(split));
            ++i;
        }
        return buf.toString();
    }
    public static String toStr(AttributeContinuousSplit split) {
        StringBuilder buf = new StringBuilder();
        buf.append(String.format("[l=%.3f,u=%.3f ", split.getLower(), split.getUpper()));
        buf.append(String.format("c=%,f,bc=%d,bcc=%,d,cc=", split.getCount(), split.getBoundaryClass(), split.getBoundaryClassCount()));
        for (LongCount.LongKeyDoubleValueIterator i = split.getClassCount().iterator(); i.hasNext(); ) {
            long k = i.getKey();
            double v = i.nextValueDouble();
            buf.append(String.format("(%d:%.6f)", k, v));
        }
        buf.append(String.format("] %h", System.identityHashCode(split)));
        return buf.toString();
    }
}
