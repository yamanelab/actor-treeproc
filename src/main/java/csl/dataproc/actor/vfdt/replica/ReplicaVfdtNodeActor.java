package csl.dataproc.actor.vfdt.replica;

import akka.actor.ActorPath;
import akka.actor.ActorRef;
import akka.pattern.PatternsCS;
import akka.util.Timeout;
import csl.dataproc.actor.stat.StatReceive;
import csl.dataproc.actor.stat.StatSystem;
import csl.dataproc.actor.vfdt.ActorVfdtNodeActor;
import csl.dataproc.actor.vfdt.VfdtMessages;
import csl.dataproc.vfdt.SparseDataLine;
import csl.dataproc.vfdt.VfdtNode;
import csl.dataproc.vfdt.VfdtNodeGrowing;
import csl.dataproc.vfdt.VfdtNodeGrowingSplitter;
import csl.dataproc.vfdt.count.AttributeClassCount;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class ReplicaVfdtNodeActor extends ActorVfdtNodeActor {
    protected ActorRef pair;

    protected boolean merging = false;
    protected List<VfdtMessages.PutDataLine> buffer = new ArrayList<>();

    public static int replicas = 2;

    public static boolean DEBUG = true;

    public ReplicaVfdtNodeActor(VfdtNode state, int depth) {
        super(state, depth);
    }

    @Override
    protected void initStatReceive(StatSystem s, long id, ActorPath path) {
        receiveStat = new ReplicaStatReceive(id, path);
    }

    @Override
    public Receive createReceive() {
        return super.createReceive().orElse(
                receiveBuilder()
                        .match(ReplicaMessages.Pair.class, this::pair)
                        .match(ReplicaMessages.UpdateSet.class, this::updateSet)
                        .match(ReplicaMessages.UpdateMerge.class, this::updateMerge)
                        .match(ReplicaMessages.UpdateSplit.class, this::updateSplit)
                        .match(ReplicaMessages.UpdateSplitFail.class, this::updateSplitFail)
                        .build());
    }

    public void pair(ReplicaMessages.Pair pair) {
        this.pair = pair.pair;
        log("pair set: " + self() + " : " + pair.pair);
        sender().tell(pair, self());
    }

    @Override
    public void putDataLine(VfdtMessages.PutDataLine m) {
        if (pair == null) {
            super.putDataLine(m);
            addPutStat();
        } else {
            if (merging) {
                buffer.add(m);
            } else {
                List<VfdtMessages.PutDataLine> bufferCopy = new ArrayList<>(buffer);
                buffer.clear();
                for (VfdtMessages.PutDataLine line : bufferCopy) {
                    putDataLine(line);
                }
                putDataLineWithPair(m);
                addPutStat();
            }
        }
    }
    protected void addPutStat() {
        if (receiveStat != null) {
            ((ReplicaStatReceive) receiveStat).addPutCount(pair != null);
        }
    }

    public void putDataLineWithPair(VfdtMessages.PutDataLine m) {
        boolean leaf = isLeaf();
        VfdtNode.DataLine line = m.dataLine.getLine();
        if (state instanceof VfdtNodeGrowing) {
            if (splitIncrementLocal(line)) {
                //merge replica data
                if (DEBUG) {
                    log("[SPLIT] * send updateSplit: count=" +
                            state.getInstanceCount() + "/" +
                                ((VfdtNodeGrowing) state).getSplitter().getSinceLastProcess() + " : " + self() + " -> " + pair);
                }
                merging = true;
                tellPair(new ReplicaMessages.UpdateSplit(state), false);
                //note: both paired actors frequently update same time. then blocked, it will be dead lock
            }
        } else {
            VfdtNode preState = state;
            state = state.put(leaf ? m.dataLine.getLine() : m.dataLine);
            if (state != preState) {
                //changed
                tellPair(new ReplicaMessages.UpdateSet(state), false);
            }
        }

        if (leaf) {
            countUp(m);
            if (m.dataLine.getTarget() != null) {
                m.dataLine.getTarget().tell(new VfdtMessages.LearnReturn(m.dataLine), self());
            }
        }
    }

    protected boolean splitIncrementLocal(VfdtNode.DataLine line) {
        VfdtNodeGrowing nodeGrowing = (VfdtNodeGrowing) state;
        if (line instanceof VfdtNode.ArrayDataLine) {
            nodeGrowing.putArrayLineWithoutSplit((VfdtNode.ArrayDataLine) line);
        } else if (line instanceof SparseDataLine) {
            nodeGrowing.putSparseLineWithoutSplit((SparseDataLine) line);
        }
        VfdtNodeGrowingSplitter splitter = nodeGrowing.getSplitter();
        return splitter.splitIncrement(splitter.getConfig().growingNodeProcessChunkSize / replicas);
    }

    protected static Timeout timeout = new Timeout(1, TimeUnit.SECONDS);

    public Object tellPair(Object msg, boolean block) {
        if (block) {
            try {
                return PatternsCS.ask(pair, msg, timeout)
                        .toCompletableFuture().get();
            } catch (Exception ex) {
                log("blockPair error: " + msg + " : " + ex);
                return null;
            }
        } else {
            pair.tell(msg, self());
            return null;
        }
    }

    public void updateSplit(ReplicaMessages.UpdateSplit m) {
        //pair is blocking
        if (DEBUG) {
            log("[SPLIT] ** receive split: " + self() + " : merging=" + merging);
        }
        //it allows doubly independent splitting processes: later set will win
        ///   a1(g1).put -> UpdateSplit(g1), UpdateSplit(g2) <- a2(g2).put
        ///                                  UpdateSet(g2)
        // in such case,
        ReplicaMessages.UpdateSplitResponse response = ReplicaMessages.splitFailCountCheck;
        long c = getMergedSplitCount(state, m.state);
        if (c >= ReplicaVfdtConfig.instance.growingNodeProcessChunkSize) {
            ReplicaNodeGrowing merged = merge(state, m.state);
            if (DEBUG) {
                log("[SPLIT]     split merged count=" +
                        merged.getInstanceCount() +"/" + merged.getSplitter().getSinceLastProcess() + " : " + self());
            }
            if (merged != null) {
                if (state instanceof VfdtNodeGrowing) {
                    ((VfdtNodeGrowing) state).getSplitter().setSinceLastProcess(0);
                }

                if (!merged.isPure()) {
                    VfdtNodeGrowingSplitter splitter = merged.getSplitter();
                    VfdtNode split = splitter.split(merged, splitter.checkSplit(merged, false));
                    if (split != merged) { //split
                        saveStatChangeState("replica " + counts.values() + " " + self() + " : " +
                                "\n" + toAttrStr(" [left]  ", state) +
                                "\n" + toAttrStr(" [right] ", m.state) + " ", merged, split);
                        response = new ReplicaMessages.UpdateSet(split);
                        self().tell(response, self());
                        log("[SPLIT]       split success : " + split + " : " + self());
                    } else {
                        //split fail: sinceLastProcess=0
                        response = ReplicaMessages.splitFailIndexCheck;
                    }
                } else {
                    //split fail: sinceLastProcess=0
                    response = ReplicaMessages.splitFailIndexCheck;
                }
            } else {
                //merge fail
                response = ReplicaMessages.splitFailMerge;
            }
        } else {
            //splitIncrement fail
            if (DEBUG) {
                log("[SPLIT]      split failed: not yet count=" + c +" : " + self());
            }
            response = ReplicaMessages.splitFailCountCheck;
        }
        tellPair(response, false);
    }

    public String toAttrStr(String head, VfdtNode state) {
        if (state instanceof VfdtNodeGrowing) {
            VfdtNodeGrowing g = (VfdtNodeGrowing) state;
            StringBuilder buf = new StringBuilder();
            buf.append(head).append(" count=").append(g.getInstanceCount());
            int i = 0;
            for (AttributeClassCount count : g.getAttributeCounts()) {
                buf.append("\n").append(head).append("   ").append("attr[").append(i).append("] ").append(toStr(count));
                ++i;
            }
            return buf.toString();
        } else {
            return head + "" + state;
        }
    }

    public long getMergedSplitCount(VfdtNode left, VfdtNode right) {
        if (left instanceof VfdtNodeGrowing && right instanceof VfdtNodeGrowing) {
            return ((VfdtNodeGrowing) left).getSplitter().getSinceLastProcess() +
                    ((VfdtNodeGrowing) right).getSplitter().getSinceLastProcess();
        } else {
            return -1;
        }
    }

    public void updateSet(ReplicaMessages.UpdateSet m) {
        if (m.state != null) {
            log("[SPLIT] *** state set : " + self() + " " + m.state + " <- " + state + " : " + counts.values() + " buffered=" + buffer.size());
            state = m.state;
        }
        finishMerging();
    }

    public void updateSplitFail(ReplicaMessages.UpdateSplitFail m) {
        if (m.clearCount && state instanceof VfdtNodeGrowing) {
            ((VfdtNodeGrowing) state).getSplitter().setSinceLastProcess(0);
        }
        if (DEBUG) {
            log("[SPLIT] *** split fail: clearCount=" + m.clearCount + " buffered=" + buffer.size() + " : " + self());
        }
        finishMerging();
    }

    public void finishMerging() {
        merging = false;
        if (!buffer.isEmpty()) {
            self().tell(buffer.remove(buffer.size() - 1), self());
        }
    }

    public void updateMerge(ReplicaMessages.UpdateMerge m) {
        if (m.state == null) {
            if (DEBUG) {
                log("send merge: " + state + " : " + self());
            }
            tellPair(new ReplicaMessages.UpdateMerge(state), false);
        } else {
            ReplicaNodeGrowing merged = merge(state, m.state);
            if (merged != null) {
                state = merged;
                pair.tell(new ReplicaMessages.UpdateSet(merged), self());
            }
        }
    }

    public ReplicaNodeGrowing merge(VfdtNode left, VfdtNode right) {
        if (left instanceof VfdtNodeGrowing && right instanceof VfdtNodeGrowing) {
            Instant mergeStart = Instant.now();
            VfdtNodeGrowing leftGrow = (VfdtNodeGrowing) left;
            VfdtNodeGrowing rightGrow = (VfdtNodeGrowing) right;
            ReplicaNodeGrowing merged = new ReplicaNodeGrowing(leftGrow.getConfig(), leftGrow.getTypes());
            merged.merge(leftGrow);
            merged.merge(rightGrow);

            if (receiveStat != null) {
                ((ReplicaStatReceive) receiveStat).addMergeTime(Duration.between(mergeStart, Instant.now()));
            }

            return merged;
        } else {
            log("merge error: " + left + " : " + right + " : " + self());
            return null;
        }
    }

    public static class ReplicaStatReceive extends StatReceive {
        public ReplicaStatReceive(long id, ActorPath path) {
            super(id, path);
        }

        @Override
        protected ReceiveCount newCountTotal(long id, String path) {
            return new ReplicaReceiveCount(id, path);
        }

        @Override
        protected ReceiveCount newCountDuration(long id) {
            return new ReplicaReceiveCount(id);
        }

        public void addPutCount(boolean withPair) {
            ((ReplicaReceiveCount) total).addPutCount(withPair);
            ((ReplicaReceiveCount) duration).addPutCount(withPair);
        }
        public void addMergeTime(Duration d) {
            ((ReplicaReceiveCount) total).addMergeTime(d);
            ((ReplicaReceiveCount) duration).addMergeTime(d);
        }
    }

    public static class ReplicaReceiveCount extends StatReceive.ReceiveCount {
        public long putWithPair;
        public long putWithoutPair;
        public Duration mergeTime = Duration.ZERO;

        public ReplicaReceiveCount(long id) {
            super(id);
        }

        public ReplicaReceiveCount(long id, String path) {
            super(id, path);
        }

        public ReplicaReceiveCount(Object json) {
            super(json);
        }

        @Override
        public void add(StatReceive.ReceiveCount count) {
            super.add(count);
            ReplicaReceiveCount c = (ReplicaReceiveCount) count;
            putWithoutPair += c.putWithoutPair;
            putWithPair += c.putWithPair;
            mergeTime = mergeTime.plus(c.mergeTime);
        }

        public void addPutCount(boolean withPair) {
            if (withPair) {
                putWithPair++;
            } else {
                putWithoutPair++;
            }
        }

        public void addMergeTime(Duration d) {
            mergeTime = mergeTime.plus(d);
        }

        @Override
        public Map<String, Object> toJson() {
            Map<String,Object> json = super.toJson();
            json.put("putWithPair", putWithPair);
            json.put("putWithoutPair", putWithoutPair);
            json.put("mergeTime", mergeTime.toString());
            return json;
        }

        @SuppressWarnings("unchecked")
        @Override
        public void setJson(Object json) {
            super.setJson(json);
            Map<String,Object> map = (Map<String,Object>) json;
            putWithPair = longValue(map, "putWithPair");
            putWithoutPair = longValue(map, "putWithoutPair");
            mergeTime = Duration.parse((String) map.getOrDefault("mergeTime", "PT0S"));
        }
    }
}
