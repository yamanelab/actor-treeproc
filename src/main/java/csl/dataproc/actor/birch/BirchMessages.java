package csl.dataproc.actor.birch;

import akka.actor.ActorRef;
import akka.actor.Props;
import akka.dispatch.ControlMessage;
import csl.dataproc.actor.birch.remote.ActorBirchServerConfig;
import csl.dataproc.actor.remote.RemoteManager;
import csl.dataproc.actor.stat.StatMailbox;
import csl.dataproc.actor.visit.TraversalMessages;
import csl.dataproc.actor.visit.TraversalStepTask;
import csl.dataproc.birch.*;

import java.io.Serializable;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

public class BirchMessages {
    public static class AskDirectResult<T> {
        public boolean direct;
        public T returnedValue;
    }

    public static class AddEntry implements Serializable {
        public BirchNodeInput entry;
        public AddEntry() {}
        public AddEntry(BirchNodeInput entry) {
            this.entry = entry;
        }

        @Override
        public String toString() {
            return "AddEntry(" + (entry == null ? -1 : entry.getId()) + ":" + entry + ")";
        }
    }

    public static class Put implements Serializable  {
        public List<BirchNode> path;
        public BirchNodeInput entry;
        public ActorRef rootChanger;
        /** max depth : the current approximate height of the tree */
        public int maxDepth;

        public int putCount = 0;
        public boolean toRoot = false;

        public Put() {}
        public Put(List<BirchNode> path, BirchNodeInput entry, ActorRef rootChanger) {
            this.path = path;
            this.entry = entry;
            this.rootChanger = rootChanger;
            maxDepth = 0;
        }

        public BirchNode getPathTop() {
            return path.get(0);
        }

        public void popPath() {
            path = new ArrayList<>(path.subList(1, path.size())); //SubList fails by serialization: (NPE: inner-object without enclosing-object)
        }

        public void pushPath(BirchNode b) {
            path.add(0, b);
            ++maxDepth;
        }

        public void replacePathTop(BirchNode b) {
            path.set(0, b);
        }

        public boolean isRootPath() {
            return path.size() == 1;
        }

        public int getMaxDepth() {
            return maxDepth;
        }

        @Override
        public String toString() {
            return String.format("Put(@%,d,path=%d,maxDepth=%d)", (entry == null ? -1 : entry.getId()),
                    path == null ? -1 : path.size(), maxDepth);
        }
    }

    public static class GetClusteringFeature extends AskDirectResult<ClusteringFeature>
            implements Serializable  {}

    public static class FindClosestChild extends AskDirectResult<BirchNode>
            implements Serializable  {
        public BirchNodeInput input;
        public ClusteringFeature feature;
        public FindClosestChild() {}
        public FindClosestChild(BirchNodeInput input, ClusteringFeature feature) {
            this.input = input;
            this.feature = feature;
        }
    }

    public static class ChangeRoot implements Serializable, ControlMessage {
        public BirchNode node;
        public int maxDepth = -1;
        public Put put;

        public ChangeRoot() {}
        public ChangeRoot(BirchNode node) {
            this.node = node;
        }

        public ChangeRoot(BirchNode node, int maxDepth, Put put) {
            this.node = node;
            this.maxDepth = maxDepth;
            this.put = put;
        }

        @Override
        public String toString() {
            return "ChangeRoot(" + node + "," + maxDepth + ")";
        }
    }

    public static class PutToRoot implements Serializable  {
        public BirchNodeInput entry;
        public int putCount;

        public PutToRoot() {}

        public PutToRoot(BirchNodeInput entry) {
            this.entry = entry;
        }

        public PutToRoot(BirchNodeInput entry, int putCount) {
            this.entry = entry;
            this.putCount = putCount;
        }

        @Override
        public String toString() {
            return String.format("PutToRoot(@%,d, %d)", entry == null ? -1 : entry.getId(), putCount);
        }
    }

    public static class GetRoot extends AskDirectResult<BirchNode> implements Serializable {}
    public static class GetChildSize extends AskDirectResult<Integer> implements Serializable {}
    public static class GetChildren extends AskDirectResult<List> implements Serializable {}

    public static class GetParent extends AskDirectResult<BirchNode> implements Serializable {}

    public static class SetParent implements Serializable, ControlMessage {
        public long clock;
        public BirchNode parent;
        public int splitCount = -1;

        public SetParent() {}

        public SetParent(long clock, BirchNode parent, int splitCount) {
            this.clock = clock;
            this.parent = parent;
            this.splitCount = splitCount;
        }

        @Override
        public String toString() {
            return "SetParent(" + parent + "," + splitCount + ")";
        }
    }

    public static class SetChildren implements Serializable {
        public List<BirchNode> children;
        public SetChildren() {}
        public SetChildren(List<BirchNode> children) {
            this.children = children;
        }

        @Override
        public String toString() {
            return "SetChildren(" + children + ")";
        }
    }
    public static class InsertAfterWithoutAddingCf implements Serializable {
        public BirchNode target;
        public BirchNode newChild;
        public InsertAfterWithoutAddingCf() {}
        public InsertAfterWithoutAddingCf(BirchNode target, BirchNode newChild) {
            this.target = target;
            this.newChild = newChild;
        }

        @Override
        public String toString() {
            return "InsertAfterWithoutAddingCf(target=" + target + ",newChild=" + newChild + ")";
        }
    }

    public static class IsTreeNode extends AskDirectResult<Boolean> implements Serializable {}

    public static class SetChildIndex implements Serializable {
        public long clock;
        public long index;

        public SetChildIndex(long clock, long index) {
            this.clock = clock;
            this.index = index;
        }

        @Override
        public String toString() {
            return "SetChildIndex(" + clock + "," + index + ")";
        }
    }
    public static class GetChildIndex extends AskDirectResult<Long> implements Serializable {}

    public enum SerializeCacheStrategy {
        Lazy, Immediate, None ;
    }

    static AtomicLong tempCount = new AtomicLong(0); //debug
    static AtomicLong debugCountGetParent = new AtomicLong(0);
    static AtomicLong debugCountGetChildren = new AtomicLong(0);
    static AtomicLong debugCountPutWithChildrenCfs = new AtomicLong(0);
    static AtomicLong debugCountPutToRoot = new AtomicLong(0);

    public static BirchNode toSerializable(ActorBirchConfig config, BirchNode node, SerializeCacheStrategy cacheStrategy) { //debug
        return toSerializable(config, node, cacheStrategy, null);
    }
    public static BirchNode toSerializable(ActorBirchConfig config, BirchNode node, SerializeCacheStrategy cacheStrategy, AtomicLong l) {
        if (node instanceof ActorBirchRefNode ||
            node instanceof BirchNodeInput ||
            node == null) {
            return node;
        } else {
            long id;
            if (node instanceof ActorBirchConfig.IndexedNode) {
                ActorBirchConfig.IndexedNode in = (ActorBirchConfig.IndexedNode) node;
                id = in.getNodeId();
            } else {
                id = -1L; //entry
            }

            ActorBirchRefNode refNodeCache = null;
            if (node instanceof ActorBirchConfig.Indexed) {
                refNodeCache = ((ActorBirchConfig.Indexed) node).getRefNode();
                if (refNodeCache != null &&
                        refNodeCache.getCfCacheStrategy().equals(cacheStrategy)) {
                    return refNodeCache;
                }
            }

            //debug
            if (l != null) {
                l.incrementAndGet();
            }
            long tmpCount = tempCount.incrementAndGet();
            ActorBirchNodeActor.log(config, "##### toSerializable %s: %,d", node, tmpCount);
            if (!config.debug && tmpCount % 1000 == 0) {
                System.err.println(String.format("#### toSerializable %s: %,d", node, tmpCount));
                System.err.println(String.format("####    putToRoot:%,d, getParent:%,d, getChildren:%,d, putWithChildrenCfs:%,d",
                        debugCountPutToRoot.get(), debugCountGetParent.get(), debugCountGetChildren.get(), debugCountPutWithChildrenCfs.get()));
            }

            ActorBirchRefNode refNode;
            ActorRef ref = config.getSystem().actorOf(Props.create(ActorBirchNodeActor.class, node, config, id)); //TODO memory management
            if (cacheStrategy.equals(SerializeCacheStrategy.Lazy)) {
                refNode = new ActorBirchRefNode(ref, config, true, id);
            } else if (cacheStrategy.equals(SerializeCacheStrategy.Immediate)) {
                refNode = new ActorBirchRefNode(ref, config, node.getClusteringFeature().copy(), id);
            } else {
                refNode = new ActorBirchRefNode(ref, config, null, id);
            }
            if (node instanceof ActorBirchConfig.Indexed &&
                    refNodeCache == null) { //do not overwrite the existing cache
                ((ActorBirchConfig.Indexed) node).setRefNode(refNode);
            }
            return refNode;
        }
    }

    public static class Accept extends AskDirectResult<Boolean> implements Serializable {
        public BirchNode.BirchVisitor visitor;

        public Accept() {}

        public Accept(BirchNode.BirchVisitor visitor) {
            this.visitor = visitor;
        }

    }

    public static class StatInconsistency implements Serializable {
        public BirchNode parent;
        public int childIndex;
        public ActorRef finisher;

        public StatInconsistency() {}

        public StatInconsistency(BirchNode parent, int childIndex, ActorRef finisher) {
            this.parent = parent;
            this.childIndex = childIndex;
            this.finisher = finisher;
        }
    }

    public static class StatInconsistencyReport implements Serializable {
        public boolean inconsistentParent;
        public boolean inconsistentChildIndex;
        public boolean refNode;
        public boolean actor;
        public boolean entry;
    }

    public static class FixChildIndex implements Serializable {
        public BirchNode parent;
        public int depth;
        public ActorRef finisher;

        public FixChildIndex() {}

        public FixChildIndex(BirchNode parent, int depth, ActorRef finisher) {
            this.parent = parent;
            this.depth = depth;
            this.finisher = finisher;
        }
    }

    public static class FixChildIndexReport implements Serializable {
        public int reducedChildSize;
        public boolean entry;
    }

    /////////////////////////////////////////////////////////

    public static class NodeAndCf implements Serializable {
        public int index;
        public BirchNode node;
        public ClusteringFeature cf;

        public NodeAndCf() { }

        public NodeAndCf(int index, BirchNode node, ClusteringFeature cf) {
            this.index = index;
            this.node = node;
            this.cf = cf;
        }

        @Override
        public String toString() {
            return index + ":" + node + ":" + cf;
        }

        public static List<BirchNode> nodes(List<NodeAndCf> ns) {
            return ns.stream().map(e -> e.node).collect(Collectors.toList());
        }
    }

    public enum GetChildrenCfsPostProcess {
        ProcessPut {
            @Override
            public Serializable newMessage(GetChildrenCfs g) {
                return new PutWithChildrenCfs(g.put, g.parent, g.collected);
            }
        },
        ProcessSplitByNewEntry {
            @Override
            public Serializable newMessage(GetChildrenCfs g) {
                return new SplitByNewEntry(g.put, g.parent, g.collected);
            }
        },
        ProcessSplitByNewNode {
            @Override
            public Serializable newMessage(GetChildrenCfs g) {
                return new SplitByNewNode(g.put, g.parent, g.collected, null);
            }
        };

        public abstract Serializable newMessage(GetChildrenCfs g);
    }


    public static class GetChildrenCfs implements Serializable {
        public Put put;
        public int index;
        public List<BirchNode> children;
        public List<NodeAndCf> collected;
        public BirchNode parent;
        public GetChildrenCfsPostProcess postProcess;

        public GetChildrenCfs() {}

        public GetChildrenCfs(Put put,
                              int index,
                              List<BirchNode> children,
                              List<NodeAndCf> collected,
                              BirchNode parent,
                              GetChildrenCfsPostProcess postProcess) {
            this.put = put;
            this.index = index;
            this.children = children;
            this.collected = collected;
            this.parent = parent;
            this.postProcess = postProcess;
        }

        @Override
        public String toString() {
            return "GetChildrenCfs{" +
                    "put=" + put +
                    ", index=" + index +
                    ", children=" + children +
                    ", collected=" + collected +
                    ", parent=" + parent +
                    ", postProcess=" + postProcess +
                    '}';
        }
    }

    public static class PutWithChildrenCfs implements Serializable {
        public Put put;
        public BirchNode parent;
        public List<NodeAndCf> collected;

        public PutWithChildrenCfs() {}

        public PutWithChildrenCfs(Put put, BirchNode parent, List<NodeAndCf> collected) {
            this.put = put;
            this.parent = parent;
            this.collected = collected;
        }

        @Override
        public String toString() {
            return "PutWithChildrenCfs{" +
                    "put=" + put +
                    ", parent=" + parent +
                    ", collected=" + collected +
                    '}';
        }
    }

    public static class SplitByNewEntry implements Serializable, BirchModificationMessage {
        public Put put;
        public BirchNode parent;
        public List<NodeAndCf> collected;
        /** collected already includes a new entry */
        public boolean includesNewEntry = false;

        public SplitByNewEntry() {
        }

        public SplitByNewEntry(Put put, BirchNode parent, List<NodeAndCf> collected) {
            this.put = put;
            this.parent = parent;
            this.collected = collected;
        }

        @Override
        public String toString() {
            return "SplitByNewEntry{" +
                    "put=" + put +
                    ", parent=" + parent +
                    ", collected=" + collected +
                    ", includesNewEntry=" + includesNewEntry +
                    '}';
        }
    }

    public static class AddNewNodeEntry implements Serializable, BirchModificationMessage {
        public Put put;
        public List<NodeAndCf> children; //optional

        public AddNewNodeEntry() { }

        public AddNewNodeEntry(Put put) {
            this.put = put;
        }

        public AddNewNodeEntry(Put put, List<NodeAndCf> children) {
            this.put = put;
            this.children = children;
        }

        @Override
        public String toString() {
            return "AddNewNodeEntry{" +
                    "put=" + put +
                    ", children=" + children +
                    '}';
        }
    }

    public static class AddNewNode implements Serializable, BirchModificationMessage {
        public Put put;
        public BirchNode node;
        public ClusteringFeature cf;

        //for extensions
        public BirchNode existingSplit;
        public ClusteringFeature existingSplitCfDiff;

        public AddNewNode() {}

        public AddNewNode(Put put, BirchNode node, ClusteringFeature cf,
                          BirchNode existingSplit, ClusteringFeature existingSplitCfDiff) {
            this.put = put;
            this.node = node;
            this.cf = cf;
            this.existingSplit = existingSplit;
            this.existingSplitCfDiff = existingSplitCfDiff;
        }

        @Override
        public String toString() {
            return "AddNewNode{" +
                    "put=" + put +
                    ", node=" + node +
                    ", cf=" + cf +
                    ", exSplit=" + existingSplit +
                    ", exCfDiff=" + existingSplitCfDiff +
                    '}';
        }
    }

    public static class SplitByNewNode implements Serializable, BirchModificationMessage {
        public Put put;
        public BirchNode parent;
        public List<NodeAndCf> collected;
        public BirchNode newNode;

        public SplitByNewNode() {}

        public SplitByNewNode(Put put, BirchNode parent, List<NodeAndCf> collected, BirchNode newNode) {
            this.put = put;
            this.parent = parent;
            this.collected = collected;
            this.newNode = newNode;
        }

        @Override
        public String toString() {
            return "SplitByNewNode{" +
                    "put=" + put +
                    ", parent=" + parent +
                    ", collected=" + collected +
                    ", newNode=" + newNode +
                    '}';
        }
    }

    public static class End implements Serializable {
        public End() {}
    }

    public static class AddNewNodeToLink implements Serializable, BirchModificationMessage {
        public AddNewNode newNode;
        public ActorRef nextLeft;
        public ActorRef nextRight;
        public int count;

        public AddNewNodeToLink() {
        }

        public AddNewNodeToLink(AddNewNode newNode, ActorRef nextLeft, ActorRef nextRight, int count) {
            this.newNode = newNode;
            this.nextLeft = nextLeft;
            this.nextRight = nextRight;
            this.count = count;
        }

        @Override
        public String toString() {
            return "AddNewNodeToLink{" +
                    "newNode=" + newNode +
                    ", nextLeft=" + nextLeft +
                    ", nextRight=" + nextRight +
                    ", count=" + count +
                    '}';
        }
    }

    public static class DelayedMessage implements Serializable {
        public Serializable message;
        public ActorRef target;
        public Instant time;

        public DelayedMessage() { }

        public DelayedMessage(Serializable message, ActorRef target, long timeMs) {
            this.message = message;
            this.target = target;
            this.time = Instant.now().plus(timeMs, ChronoUnit.MILLIS);
        }

        @Override
        public String toString() {
            return "DelayedMessage{" +
                    "message=" + message +
                    ", target=" + target +
                    ", time=" + time +
                    '}';
        }
    }

    public static class SplitByNewNodeDelayed implements Serializable, BirchModificationMessage {
        public BirchNode invalidPath;
        /** {@link SplitByNewNode} or {@link AddNewNode} */
        public Serializable message;
        public int count;

        public SplitByNewNodeDelayed() {}

        public SplitByNewNodeDelayed(BirchNode invalidPath, Serializable message, int count) {
            this.invalidPath = invalidPath;
            this.message = message;
            this.count = 0;
        }

        @Override
        public String toString() {
            return "SplitByNewNodeDelayed{" +
                    "invalidPath=" + invalidPath +
                    ",message=" + message +
                    ",count=" + count +
                    '}';
        }
    }

    public static class GetFinishCount implements Serializable { }

    public static class GetFinishTime implements Serializable { }

    public static class SetParentAsRoot implements Serializable {
        public Put put;
        public SetParent setParent;

        public SetParentAsRoot() {}

        public SetParentAsRoot(Put put, SetParent setParent) {
            this.put = put;
            this.setParent = setParent;
        }

        @Override
        public String toString() {
            return "SetParentAsRoot{" +
                    "put=" + put +
                    ",setParent=" + setParent +
                    '}';
        }
    }

    //////////////////////

    public static class DotAdd implements Serializable {
        public long id; //future use
        public String label;
        public boolean push;
        public boolean box;
        public int childIndex;

        public DotAdd() {}

        public DotAdd(long id, String label, boolean push, boolean box, int childIndex) {
            this.id = id;
            this.label = label;
            this.push = push;
            this.box = box;
            this.childIndex = childIndex;
        }
    }

    public static class DotPop implements Serializable {
        public DotPop() {}
    }

    public static class BirchTraversalState<VisitorType extends BirchNode.BirchVisitor>
        extends TraversalMessages.TraversalState<BirchNode, VisitorType> {
        public BirchTraversalState() {}

        public BirchTraversalState(VisitorType v) {
            super(v);
        }

        @Override
        public TraversalStepTask<BirchNode, VisitorType> getNextStepTaskNode(BirchNode root) {
            List<Integer> idx = getNonActorIndexSubPath();
            BirchNodeCFTree parent = null;
            BirchNode child = root;
            int index = 0;
            if (root != null) {
                for (int i : idx) {
                    index = i;
                    if (child instanceof BirchNodeCFTree) {
                        parent = (BirchNodeCFTree) child;
                        List<BirchNode> ns = parent.getChildren();
                        if (i < ns.size()) {
                            child = ns.get(i);
                        } else {
                            return getNextStepTaskNode(parent, null, index);
                        }
                    } else {
                        //
                    }
                }
            }
            return getNextStepTaskNode(parent, child, index);
        }

        @Override
        public boolean visitEnter(BirchNode child) {
            if (child instanceof BirchNodeCFTree) {
                return visitor.visitTree((BirchNodeCFTree) child);
            } else {
                return child.accept(visitor);
            }
        }

        @Override
        public boolean visitChild(BirchNode parent, int index, BirchNode child) {
            visitor.visitTreeChild((BirchNodeCFTree ) parent, index,
                    ((BirchNodeCFTree) parent).getNodeHolders().get(index));
            return true;
        }

        @Override
        public TraversalMessages.TraversalNodeType getType(BirchNode node) {
            if (node instanceof ActorBirchRefNode) {
                return TraversalMessages.TraversalNodeType.Actor;
            } else if (node instanceof BirchNodeCFTree) {
                return TraversalMessages.TraversalNodeType.Parent;
            } else {
                return TraversalMessages.TraversalNodeType.Leaf;
            }
        }

        @Override
        public ActorRef getActor(BirchNode refNode) {
            return ((ActorBirchRefNode) refNode).getRef();
        }

        @Override
        public boolean visitExit(BirchNode parent, VisitorType visitor, boolean error) {
            if (parent != null) {
                if (parent instanceof BirchNodeCFTree) {
                    visitor.visitAfterTree((BirchNodeCFTree) parent);
                }
            }
            return true;
        }
    }

    public static class PurityConstructLoad implements Serializable{
        public long id;
        public BirchNodeCFEntry entry;

        public PurityConstructLoad() {}

        public PurityConstructLoad(long id, BirchNodeCFEntry entry) {
            this.id = id;
            this.entry = entry;
        }
    }

    public static class PurityFinishNode implements Serializable {
        public long id;

        public PurityFinishNode() {}

        public PurityFinishNode(long id) {
            this.id = id;
        }
    }

    public static class VisitTree implements Serializable {
        public long id;
        public int childSize;
        public ClusteringFeature cf;

        public VisitTree() { }

        public VisitTree(int childSize, long id, ClusteringFeature cf) {
            this.id = id;
            this.childSize = childSize;
            this.cf = cf;
        }
    }

    public static class VisitEntry implements Serializable {
        public long id;
        public ClusteringFeature cf;
        public LongList indexes;

        public VisitEntry() {}

        public VisitEntry(long id, ClusteringFeature cf, LongList indexes) {
            this.id = id;
            this.cf = cf;
            this.indexes = indexes;
        }
    }

    public static class VisitAfter implements Serializable {
    }

    public static class VisitChild implements Serializable {
        public int i;

        public VisitChild() {}

        public VisitChild(int i) {
            this.i = i;
        }
    }

    public interface BirchModificationMessage { }

    public static class BirchEnvelopeComparator extends StatMailbox.EnvelopePriorityComparator {
        protected static Class<?>[] types = {
                ControlMessage.class,
                BirchModificationMessage.class
        };

        public BirchEnvelopeComparator() {
            super(types);
        }
    }

    /////////////////////////////////////

    public static class ServerTextInput implements Serializable {
        public String data;

        public ServerTextInput() {}

        public ServerTextInput(String data) {
            this.data = data;
        }
    }

    public static class ServerLoadInput implements Serializable {
        public String serverFile;

        public ServerLoadInput() {}

        public ServerLoadInput(String serverFile) {
            this.serverFile = serverFile;
        }
    }

    public static class ServerStatus implements Serializable { }

    public static class ServerStat implements Serializable { }

    public static class ServerSave implements Serializable {
        public String serverFile;

        public ServerSave() { }

        public ServerSave(String serverFile) {
            this.serverFile = serverFile;
        }
    }

    public static class ServerPurity implements Serializable { }

    /////////////////////////////////////

    public static List<Class<?>> getTypes() {
        List<Class<?>> cls = new ArrayList<>();
        cls.addAll(Arrays.asList(
                BirchConfig.class,
                BirchConfig.BirchConfigTest.class,

                BirchNode.DefaultDistanceFunction.class,
                BirchNodeCFEntry.class,
                BirchNodeCFTree.class,
                BirchNodeCFTree.NodeHolder.class,
                BirchNodeInput.class,
                BirchNodeInput.BirchNodeInputLabeled.class,
                ClusteringFeature.class,

                BirchBatch.CountEntriesVisitor.class,
                BirchBatch.CountEntryPointsVisitor.class,
                BirchBatch.DataField.class,
                BirchBatch.Extract.class,
                BirchBatch.ExtractLog.class,
                BirchBatch.Input.class,

                DendrogramPurityUnit.DataPoint.class,
                DendrogramPurityUnit.SubTreeTable.class
        ));

        cls.addAll(Arrays.asList(
                ActorBirchConfig.class,
                ActorBirchConfig.IndexedEntry.class,
                ActorBirchConfig.IndexedNode.class,
                ActorBirchConfig.NodeCreator.class,
                ActorBirchRefNode.class,
                ActorBirchBatch.BirchBatchEx.SerializableCountDepthVisitor.class,
                ActorBirchBatch.RootChangeCreator.class,

                ActorBirchNodeActor.PutEventType.class,
                ActorBirchNodeActor.BirchNodeInvalidPath.class,

                ActorBirchVisitorDot.DotTraversalVisitor.class,
                ActorBirchVisitorIndexer.TravIndexer.class,
                ActorBirchVisitorSave.IndexedSaveVisitor.class,

                ActorDendrogramPurityUnit.PurityConstructionHostRef.class,
                ActorDendrogramPurityUnit.ActorBirchVisitorTableConstruction.class
        ));

        cls.add(ActorBirchServerConfig.class);

        cls.addAll(Arrays.asList(BirchMessages.class.getClasses()));
        return cls;
    }

}
