package csl.dataproc.actor.birch;

import akka.actor.*;
import akka.japi.Creator;
import csl.dataproc.actor.remote.RemoteConfig;
import csl.dataproc.actor.remote.RemoteManager;
import csl.dataproc.actor.remote.RemoteManagerMessages;
import csl.dataproc.actor.stat.StatMailbox;
import csl.dataproc.actor.stat.StatMessages;
import csl.dataproc.actor.stat.StatReceive;
import csl.dataproc.actor.stat.StatSystem;
import csl.dataproc.birch.*;
import scala.PartialFunction;
import scala.concurrent.duration.FiniteDuration;
import scala.runtime.BoxedUnit;

import java.io.Serializable;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class ActorBirchConfig extends BirchConfig implements RemoteManager.SharedConfig {
    protected transient ActorSystem system;
    protected transient ActorRef spool;
    protected ActorRef finishActor;
    protected boolean test;

    public Duration finishLogDuration = Duration.ofMillis(200);

    public static ActorSystem globalSystem;

    public static ActorBirchConfig instance;

    transient ThreadLocal<LinkedList<ActorCall>> caller =
            ThreadLocal.withInitial(() -> {
                LinkedList<ActorCall> stack = new LinkedList<>();
                return  stack;
            });

    public static class ActorCall {
        public Actor actor;
        public Object msg;

        public ActorCall(Actor actor, Object msg) {
            this.actor = actor;
            this.msg = msg;
        }

        @Override
        public String toString() {
            return "call(" + actor + ", " + msg + ")";
        }
    }

    protected Map<StackInfo,StackInfo> stackInfoHistory = new LinkedHashMap<>();

    public ActorBirchConfig() {
        checkSystem();
        if (instance == null){
            instance = this;
        }
    }


    @Override
    public void arrivedAtRemoteNode() {
        checkSystem();
        instance = this;
    }

    public ActorBirchConfig(ActorSystem system) {
        this.system = system;
        setTraceIfDebug();
        if (instance == null){
            instance = this;
        }
    }

    public ActorSystem getSystem() {
        return system;
    }

    private void checkSystem() {
        if (system == null){
            system = globalSystem;
            setTraceIfDebug();
        }
    }

    public void setTraceIfDebug() {
        if (debug) {
            BirchNodeCFTree.trace = new BirchNodeCFTree.PutTraceLog();
        }
    }

    public Map<String,Object> toJson() {
        Map<String,Object> map = new HashMap<>();
        map.put("maxNodeEntries", maxNodeEntries);
        map.put("maxDistance", maxDistance);
        map.put("test", isTest());
        if (distanceFunction instanceof BirchNode.DefaultDistanceFunction) {
            map.put("distanceFunction",
                    ((BirchNode.DefaultDistanceFunction) distanceFunction).getNum());
        }
        return map;
    }

    @Override
    public boolean isTreeNode(BirchNode node) {
        if (node instanceof BirchNodeCFTree) {
            return true;
        } else if (node instanceof ActorBirchRefNode) {
            return ((ActorBirchRefNode) node).isTreeNode();
        } else {
            return false;
        }
    }

    @Override
    public BirchNodeConcrete newEntry(BirchNodeInput entry) {
        return new IndexedEntry(entry);
    }

    @Override
    public BirchNode newRoot(int dim) {
        return newNode(new IndexedNode(this, dim), true);
    }

    @Override
    public BirchNode newNode(List<BirchNode> children) {
        return newNode(new IndexedNode(this, children), false);
    }

    public BirchNode newNode(IndexedNode node, boolean root) {
        checkSystem();
        long id = node.getNodeId();
        //EXT: return new ActorBirchRefNode(createNodeActor(node), this);
        ActorRef ref = createNodeActor(node, root, id);
        ActorBirchRefNode refNode = new ActorBirchRefNode(ref, this, node.getClusteringFeature().copy(), id);
        if (ref instanceof ActorRefScope && ((ActorRefScope) ref).isLocal()) { //only local actor's state can have cache
            node.setRefNode(refNode);
        }
        return refNode;
    }

    public ActorRef createNodeActor(IndexedNode node, boolean root, long id) {
        //EXT: return create(ActorBirchNodeActor.class, new NodeCreator(node, this));
        return create(ActorBirchNodeActor.class, new NodeCreator(node, this, root, id));
    }

    public <T extends Actor> ActorRef create(Class<T> actor, Creator<T> props) {
        return RemoteManager.create(system, actor, props);
    }

    public static class NodeCreator implements Creator<ActorBirchNodeActor>, Serializable {
        protected IndexedNode node;
        protected ActorBirchConfig config;
        protected ActorRef linkLeft;
        protected ActorRef linkRight;
        protected boolean root;
        protected int splitCount;
        protected long id;

        public NodeCreator(IndexedNode node, ActorBirchConfig config, boolean root, long id) {
            this.node = node;
            this.config = config;
            this.root = root;
            this.id = id;
        }

        public NodeCreator(IndexedNode node, ActorBirchConfig config,
                           ActorRef linkLeft, ActorRef linkRight, boolean root, int splitCount, long id) {
            this.node = node;
            this.config = config;
            this.linkLeft = linkLeft;
            this.linkRight = linkRight;
            this.root = root;
            this.splitCount = splitCount;
            this.id = id;
        }

        @Override
        public ActorBirchNodeActor create() {
            return new ActorBirchNodeActor(node, config, linkLeft, linkRight, root, splitCount, id);
        }
    }

    public ActorRef createRootChanger() {
        return create(ActorBirchBatch.RootChangeActor.class, new ActorBirchBatch.RootChangeCreator(this));
    }

    @Override
    public BirchConfig toTest() {
        test = true;
        return this;
    }

    @Override
    public boolean isTest() {
        return test;
    }

    @Override
    public BirchNodeInput newPoint(long id, double... data) {
        if (test) {
            int label = (int) data[data.length - 1];
            double[] attrs = Arrays.copyOf(data, data.length - 1);
            return new BirchNodeInput.BirchNodeInputLabeled(id, label, attrs);
        } else {
            return super.newPoint(id, data);
        }
    }

    public void pushCaller(ActorBirchNodeActor actor, Object msg) {
        LinkedList<ActorCall> stack = caller.get();
        int level = stack.size();
        stack.addLast(new ActorCall(actor, msg));
        if (debug) {
            System.err.println(indent(level) + "[" + Thread.currentThread().getId() + "]#push " + actor + " : " + msg
                    + " [" + Thread.currentThread() + "]");
        }
    }

    private String indent(int level) {
        return IntStream.range(0, level)
                .mapToObj(e -> "   ")
                .collect(Collectors.joining()) + Integer.toHexString(System.identityHashCode(caller.get())) + ": ";
    }

    public void popCaller(ActorBirchNodeActor actor) {
        LinkedList<ActorCall> stack = caller.get();
        ActorCall a = stack.removeLast();
        int level = stack.size();
        if (debug) {
            System.err.println(indent(level) + "[" + Thread.currentThread().getId() + "]#pop  " + a);
        }
    }

    public ActorBirchNodeActor getCaller(ActorRef ref) {
        LinkedList<ActorCall> stack = caller.get();
        ActorCall a = (stack.isEmpty() ? null : stack.getLast());
        if (a != null && a.actor.self().equals(ref)) {
            return (ActorBirchNodeActor) a.actor;
        } else {
            if (debug) {
                if (stack.stream()
                        .anyMatch(c -> c.actor.self().equals(ref))) {
                    int i = 0;
                    for (ActorCall c : stack) {
                        System.err.println("[" + Thread.currentThread().getId() + "]##### caller " + i + "/" + stack.size() + " : " + c
                            + (c.actor.self().equals(ref) ? "!!!!" : ""));
                        ++i;
                    }
                    throw new RuntimeException();
                }
            }
            return null;
        }
    }

    public boolean isCaller(ActorRef ref) {
        ActorBirchNodeActor a = getCaller(ref);
        if (a != null) {
            return a.getSelf().equals(ref);
        } else {
            return false;
        }
    }

    public void terminate() {
        if (spool != null) {
            spool.tell(new BirchMessages.End(), ActorRef.noSender());
        }
        if (finishActor != null) {
            finishActor.tell(PoisonPill.getInstance(), ActorRef.noSender());
        }
        system.terminate();
    }

    public ActorRef getSpool() {
         if (spool == null) {
             spool = RemoteManager.create(system, SpoolActor.class, new SpoolCreator());
         }
         return spool;
    }

    public void sendDelay(Serializable msg, ActorRef target, long timeMs) {
        getSpool().tell(new BirchMessages.DelayedMessage(msg, target, timeMs), ActorRef.noSender());
    }

    public ActorRef getFinishActor() {
        if (finishActor == null) {
            finishActor = RemoteManager.create(system, FinisherActor.class, new FinisherCreator(finishLogDuration));
        }
        return finishActor;
    }

    public static class FinisherCreator implements Creator<FinisherActor>, RemoteManagerMessages.DispatcherSpecifiedCreator {
        Duration logDuration;

        public FinisherCreator() {
            logDuration = Duration.ofMillis(200);
        }

        public FinisherCreator(Duration logDuration) {
            this.logDuration = logDuration;
        }

        @Override
        public FinisherActor create() throws Exception {
            return new FinisherActor(logDuration);
        }
        @Override
        public String getSpecifiedDispatcher() {
            return RemoteConfig.CONF_PINNED_DISPATCHER;
        }
    }


    public static class FinisherActor extends AbstractActor {
        protected long entryCount;
        protected Instant startTime = Instant.EPOCH;
        protected Instant lastFinishLogTime = Instant.EPOCH;
        protected Instant finishTime = Instant.EPOCH;

        protected Duration logDuration;

        public FinisherActor() {
            this(Duration.ofMillis(200));
        }

        public FinisherActor(Duration logDuration) {
            this.logDuration = logDuration;
        }

        @Override
        public void preStart() throws Exception {
            super.preStart();
            startTime = Instant.now();
        }

        @Override
        public Receive createReceive() {
            return receiveBuilder()
                    .match(BirchMessages.Put.class, this::finishPut)
                    .match(BirchMessages.GetFinishCount.class, this::getFinishCount)
                    .match(BirchMessages.GetFinishTime.class, this::getFinishTime)
                    .build();
        }

        public void finishPut(BirchMessages.Put m) {
            entryCount++;
            Instant now = Instant.now();
            if (Duration.between(lastFinishLogTime, now).compareTo(logDuration) > 0) {
                lastFinishLogTime = now;
                System.err.println(String.format("%s: finish input-id=%d count=%,d",
                        Duration.between(startTime, now), m.entry.getId(), entryCount));
            }
            finishTime = now;
        }

        public void getFinishCount(BirchMessages.GetFinishCount m) {
            sender().tell(entryCount, self());
        }

        public void getFinishTime(BirchMessages.GetFinishTime m) {
            sender().tell(finishTime, self());
        }

    }

    public static class SpoolCreator implements Creator<SpoolActor>, RemoteManagerMessages.DispatcherSpecifiedCreator {
        @Override
        public SpoolActor create() throws Exception {
            return new SpoolActor();
        }

        @Override
        public String getSpecifiedDispatcher() {
            return RemoteConfig.CONF_PINNED_DISPATCHER;
        }
    }

    public static class SpoolActor extends AbstractActor {
        protected StatReceive statReceive;
        protected StatMailbox.StatQueue queue;

        @Override
        public void preStart() throws Exception {
            super.preStart();
            StatSystem statSystem = StatSystem.system();
            if (statSystem.isEnabled()) {
                statReceive = new StatReceive(
                        statSystem.getId(context().system(), self().path().toStringWithoutAddress()),
                        self().path());
            }
        }

        @Override
        public void aroundReceive(PartialFunction<Object, BoxedUnit> receive, Object msg) {
            if (statReceive != null) {
                statReceive.receiveBefore(msg);;
            }
            try {
                super.aroundReceive(receive, msg);
            } finally {
                if (statReceive != null) {
                    statReceive.receiveAfter(msg, "spoolActor");
                }
            }
        }

        @Override
        public Receive createReceive() {
            return receiveBuilder()
                    .match(BirchMessages.DelayedMessage.class, this::run)
                    .match(BirchMessages.End.class, this::end)
                    .match(StatMessages.ActorId.class, this::actorId)
                    .build();
        }

        public void run(BirchMessages.DelayedMessage m) {
            Instant now = Instant.now();
            if (now.isAfter(m.time)) {
                m.target.tell(m.message, ActorRef.noSender());
            } else {
                Duration d = Duration.between(now, m.time);
                getContext().getSystem().scheduler()
                        .scheduleOnce(
                                FiniteDuration.apply(d.toMillis(), TimeUnit.MILLISECONDS),
                                m.target,
                                m.message,
                                getContext().dispatcher(),
                                ActorRef.noSender());
            }

        }

        public void actorId(StatMessages.ActorId m) {
            this.queue = m.queue;
            if (queue != null && queue.getId() != -1 && statReceive != null) {
                queue.setId(statReceive.getId());
            }
        }

        public void end(BirchMessages.End e) {
            if (statReceive != null) {
                statReceive.saveTotal();
            }
            self().tell(PoisonPill.getInstance(), self());
        }
    }



    public interface Indexed {
        long getChildIndex();
        void setChildIndex(long index);

        /**
         * @return cache for ref-node, set by {@link #setRefNode(ActorBirchRefNode)}. {@link ActorBirchRefNode#getRefNode()} always returns this
         */
        ActorBirchRefNode getRefNode();
        default void setRefNode(ActorBirchRefNode node) { }
    }

    public static class IndexedEntry extends BirchNodeCFEntry implements Indexed, BirchNodeConcrete {
        public long childIndex;
        protected ActorBirchRefNode refNode; //cache for serializable

        public IndexedEntry(ClusteringFeature clusteringFeature, LongList indexes) {
            super(clusteringFeature, indexes);
        }

        public IndexedEntry(BirchNodeInput input) {
            super(input);
        }

        @Override
        public void setChildIndex(long childIndex) {
            this.childIndex = childIndex;
        }

        @Override
        public long getChildIndex() {
            return childIndex;
        }

        @Override
        public void setRefNode(ActorBirchRefNode refNode) {
            this.refNode = refNode;
        }

        @Override
        public ActorBirchRefNode getRefNode() {
            return refNode;
        }

        @Override
        public String toString() {
            return "IndexedEntry(idx=" + childIndex + ",data=" + getIndexes().size() +",id=" + Integer.toHexString(System.identityHashCode(this)) + ")";
        }
    }

    public static class IndexedNode extends BirchNodeCFTree implements Indexed, BirchNodeConcrete {
        protected long childCount;
        public long childIndex;
        public long nodeId;
        protected ActorBirchRefNode refNode; //cache for serializable

        public IndexedNode(BirchConfig config, List<BirchNode> children) {
            super(config, children);
            nodeId = RemoteManager.incrementAndGetId();
        }

        public IndexedNode(BirchConfig config, int dimension) {
            super(config, dimension);
            nodeId = RemoteManager.incrementAndGetId();
        }

        public long getNodeId() {
            return nodeId;
        }

        @Override
        protected NodeHolder newNodeHolder(BirchNode child) {
            ((Indexed) child).setChildIndex(childCount);
            ++childCount;
            //No setParent here
            return new NodeHolder(child);
        }

        @Override
        public void setChildren(List<BirchNode> es) {
            childCount = 0;
            super.setChildren(es);
        }

        @Override
        public void setChildrenWithoutUpdatingCf(List<BirchNode> es) {
            childCount = 0;
            super.setChildrenWithoutUpdatingCf(es);
        }

        /** for manual setting of children*/
        public void setChildSize(int childSize) {
            this.childSize = childSize;
            childCount = childSize;
        }

        @Override
        public long getChildIndex() {
            return childIndex;
        }

        @Override
        public void setChildIndex(long childIndex) {
            this.childIndex = childIndex;
        }

        @Override
        public void setRefNode(ActorBirchRefNode refNode) {
            this.refNode = refNode;
        }

        @Override
        public ActorBirchRefNode getRefNode() {
            return refNode;
        }

        @Override
        public NodeHolder findNodeHolder(BirchNode n) {
            if (n == null) {
                return null;
            } else if (n instanceof Indexed) {
                long i = ((Indexed) n).getChildIndex();
                NodeHolder next = child;
                while (next != null && !match(n, i, next.node)) {
                    next = next.next;
                }
                if (next != null) {
                    return next;
                } else {
                    return null;
                }
            } else {
                return super.findNodeHolder(n);
            }
        }

        private boolean match(BirchNode n, long i, BirchNode next) {
            return next == n ||
                    (next instanceof Indexed && ((Indexed) next).getChildIndex() == i);
        }

        @Override
        public String toString() {
            return "IndexedNode(idx=" + childIndex + ",childSize=" + childSize + ",childCount=" + childCount +",id=" +
                    nodeId + ")";
        }
    }


    public static class StackInfo {
        protected int hash;
        protected ArrayList<String> path = new ArrayList<>();
        public int count = 1;

        public boolean contains(String p) {
            return path.contains(p);
        }

        public void insert(String p) {
            path.add(0, p);
            hash += p.hashCode();
        }

        public void add(String p) {
            path.add(p);
            hash += p.hashCode();
        }

        public void fix() {
            path.trimToSize();
        }

        @Override
        public int hashCode() {
            return hash;
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == this) {
                return true;
            } else if (obj.getClass().equals(getClass())) {
                return path.equals(((StackInfo) obj).path);
            } else {
                return false;
            }
        }

        public StackInfo countUp() {
            ++count;
            return this;
        }

        @Override
        public String toString() {
            String top = "";
            String internal = "";
            String end = "";
            int size = path.size();
            if (size > 0) {
                end = path.get(path.size() - 1);
                if (size > 1) {
                    top = path.get(0);
                    if (path.size() > 2) {
                        internal = String.join(",", path.subList(1, path.size() - 1));
                    }
                }
            }
            return "#STACK\t" + count + "\t" + top + "\t" + internal + "\t" + end;
        }
    }

    public static Set<String> stackInfoExcludedMethods = new HashSet<>(Arrays.asList(
            "getStackTrace", //Thread#getStackTrace()
            "recordStackInfo",   //ActorBirchConfig#recordStackInfo
            //ActorBirchNodeActor methods:
            "send",
            "sendOrInvokeDirect",
            "sendOrInvokeDirectWithInvalidPath",
            "invokeSelfState",
            "invokeNonActor",
            "afterGetChildrenCfsDirect",
            "getChildrenCfsDirect"
    ));

    public synchronized void recordStackInfo(String msg) {
        StackTraceElement[] stack = Thread.currentThread().getStackTrace();
        StackInfo info = new StackInfo();
        for (StackTraceElement frame : stack) {
            String mName = frame.getMethodName();
            if (frame.getClassName().startsWith("akka")) {
                break;
            } else if (!(stackInfoExcludedMethods.contains(mName) ||
                            info.contains(mName) || //recursion
                            mName.contains("$"))) { //lambda
                if (mName.equals("finishPut") && msg.equals("Put")) {
                    msg = "[FINISH]";
                } else {
                    info.insert(mName);
                }
            }
        }
        info.add(msg);
        info.fix();
        stackInfoHistory.compute(info,
                (k,v) -> v == null ? info : v.countUp());
    }

    public synchronized List<StackInfo> getStackInfoHistory() {
        return new ArrayList<>(stackInfoHistory.values());
    }
}
