package csl.dataproc.actor.birch;

import akka.actor.ActorRef;
import akka.actor.ActorRefScope;
import akka.pattern.PatternsCS;
import akka.util.Timeout;
import csl.dataproc.actor.remote.RemoteManager;
import csl.dataproc.birch.BirchNode;
import csl.dataproc.birch.BirchNodeInput;
import csl.dataproc.birch.ClusteringFeature;
import csl.dataproc.birch.LongList;

import java.io.Serializable;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class ActorBirchRefNode implements Serializable, BirchNode, ActorBirchConfig.Indexed {
    protected ActorRef ref;
    protected ActorBirchConfig config;
    public long childIndex = -1;
    protected long nodeId;


    /** CF cache */
    protected boolean cfCache;
    protected ClusteringFeature clusteringFeature;
    protected BirchNode parent;

    protected long clockChildIndex;
    protected long clockParent;

    public ActorBirchRefNode() {
    }

    public ActorBirchRefNode(ActorRef ref, ActorBirchConfig config, boolean cfCache, long nodeId) {
        this.ref = ref;
        this.config = config;
        this.nodeId = nodeId;
        this.cfCache = cfCache;
    }

    public ActorBirchRefNode(ActorRef ref, ActorBirchConfig config, ClusteringFeature clusteringFeature, long nodeId) {
        this.ref = ref;
        this.config = config;
        this.clusteringFeature = clusteringFeature;
        this.cfCache = (clusteringFeature != null);
        this.nodeId = nodeId;
    }

    @Override
    public ActorBirchRefNode getRefNode() {
        return this;
    }

    public BirchMessages.SerializeCacheStrategy getCfCacheStrategy() {
        return clusteringFeature == null ?
                BirchMessages.SerializeCacheStrategy.Immediate : //CF is already cached; thus cf-copying happens when transfer it to a remote node
                (cfCache ?
                        BirchMessages.SerializeCacheStrategy.Lazy :
                        BirchMessages.SerializeCacheStrategy.None);
    }

    public boolean isLocalRef() {
        return ref instanceof ActorRefScope && ((ActorRefScope) ref).isLocal();
    }

    /**
     * @return unique ID or -1 if state of the ref is an entry
     */
    public long getNodeId() {
        return nodeId;
    }

    @Override
    public BirchNode getParent() {
        return parent;
        //EXT: return ask(new BirchMessages.GetParent(), BirchNode.class);
    }

    @Override
    public void setParent(BirchNode parent) {
        setParentRef(parent); //EXT
        long clock = getClock(); //RemoteManager.incrementAndGetId();
        this.clockParent = clock;
        if (config.debug) {
            config.recordStackInfo(parent == null ? "parent(null)" :
                    "parent(" + parent.getClass().getSimpleName() + ")");

            //SET-RECEIVE-DEBUG: System.err.println(String.format("### send SetParent\t%s\t%s", parent, ref));
        }
        tell(new BirchMessages.SetParent(clock, parent, -1));
    }

    private long getClock() {
        Instant now = Instant.now();
        long micro = (now.getNano() / 1000L);
        return now.getEpochSecond() * 1000_000L + micro;
    }

    public void setParentRef(BirchNode parent) {
        this.parent = parent;
    }

    @Override
    public LongList getIndexes() {
        return null;
    }

    @Override
    public void addEntry(BirchNodeInput e) {
        tell(new BirchMessages.AddEntry(e));
    }

    /** never use original put(e) */
    public void put(BirchMessages.Put p) {
        tell(p);
    }

    @Override
    public ClusteringFeature getClusteringFeature() {
        if (clusteringFeature == null) {
            ClusteringFeature cf = ask(new BirchMessages.GetClusteringFeature(), ClusteringFeature.class);
            if (cfCache) {
                clusteringFeature = cf;
            }
            return cf;
        } else {
            return clusteringFeature;
        }
    }

    public boolean hasClusteringFeatureCache() {
        return clusteringFeature != null;
    }

    @Override
    public BirchNode findClosestChild(BirchNodeInput input, ClusteringFeature cf, DistanceFunction distanceFunction) {
        return ask(new BirchMessages.FindClosestChild(input, cf), BirchNode.class);
    }

    protected <R> R ask(BirchMessages.AskDirectResult<R> msg, Class<R> type) {
        return ask(msg, type, new Timeout(5, TimeUnit.HOURS));
    }

    protected <R> R ask(BirchMessages.AskDirectResult<R> msg, Class<R> type, Timeout timeout) {
        try {
            ActorBirchNodeActor actor = config.getCaller(ref);
            if (actor != null) {
                msg.direct = true;
                actor.receive().apply(msg);
                return msg.returnedValue;
            } else {
                msg.direct = false;
                if (config.debug) {
                    config.recordStackInfo(msg.getClass().getSimpleName());
                }
                return type.cast(PatternsCS.ask(ref, msg, timeout)
                        .toCompletableFuture().get());
            }
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    protected void tell(Object msg) {
        if (config.debug) {
            config.recordStackInfo(msg.getClass().getSimpleName());
        }
        ActorBirchNodeActor actor = config.getCaller(ref);
        if (actor != null) {//avoiding deadlock
            actor.receive().apply(msg);
        } else {
            ref.tell(msg, ActorRef.noSender());
        }
    }

    @Override
    public void setChildren(List<BirchNode> children) {
        tell(new BirchMessages.SetChildren(children));
    }

    @Override
    public void insertAfterWithoutAddingCf(BirchNode child, BirchNode newChildNext) {
        tell(new BirchMessages.InsertAfterWithoutAddingCf(child, newChildNext));
    }

    @Override
    public int getChildSize() {
        return ask(new BirchMessages.GetChildSize(), Integer.class);
    }

    @SuppressWarnings("unchecked")
    @Override
    public List<BirchNode> getChildren() {
        return ask(new BirchMessages.GetChildren(), List.class);
    }

    public boolean isTreeNode() {
        return ask(new BirchMessages.IsTreeNode(), Boolean.class);
    }

    @Override
    public void setChildIndex(long index) {
        this.childIndex = index;
        long clock = getClock();
        this.clockChildIndex = clock;
        tell(new BirchMessages.SetChildIndex(clock, index));
    }

    @Override
    public long getChildIndex() {
        if (childIndex == -1) {
            childIndex = ask(new BirchMessages.GetChildIndex(), Long.class);
        }
        return childIndex;
    }

    @Override
    public boolean accept(BirchVisitor birchVisitor) {
        return ask(new BirchMessages.Accept(birchVisitor), Boolean.class, new Timeout(5, TimeUnit.HOURS));
    }

    public ActorRef getRef() {
        return ref;
    }

    @Override
    public String toString() {
        return "ref(id=" + nodeId + "," + ref + ")";
    }


}
