package csl.dataproc.actor.birch;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.PoisonPill;
import akka.japi.pf.FI;
import akka.japi.pf.ReceiveBuilder;
import csl.dataproc.actor.remote.RemoteManager;
import csl.dataproc.actor.stat.StatMailbox;
import csl.dataproc.actor.stat.StatMessages;
import csl.dataproc.actor.stat.StatReceive;
import csl.dataproc.actor.stat.StatSystem;
import csl.dataproc.actor.visit.TraversalMessages;
import csl.dataproc.birch.*;
import csl.dataproc.tgls.util.ArraysAsList;
import scala.PartialFunction;
import scala.runtime.BoxedUnit;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.ListIterator;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class ActorBirchNodeActor extends AbstractActor {
    protected BirchNodeConcrete state;
    protected ActorBirchConfig config;
    protected StatReceive statReceive;
    protected StatMailbox.StatQueue queue;

    //EXT
    protected ActorRef linkLeft;
    protected ActorRef linkRight;
    protected boolean root;
    protected int splitCount;
    protected int parentSplitCount = -1;
    protected long clockParent;
    protected long clockChildIndex;
    protected long nodeId;

    public ActorBirchNodeActor(BirchNodeConcrete state, ActorBirchConfig config, long nodeId) {
        this.state = state;
        this.config = config;
        this.nodeId = nodeId;
    }

    public ActorBirchNodeActor(BirchNodeConcrete state, ActorBirchConfig config,
                               ActorRef linkLeft, ActorRef linkRight, boolean root, int splitCount, long nodeId) {
        this.state = state;
        this.config = config;
        this.linkLeft = linkLeft;
        this.linkRight = linkRight;
        this.root = root;
        this.splitCount = splitCount;
        this.nodeId = nodeId;
    }

    public long getNodeId() {
        return nodeId;
    }

    /// stat support ///////////////////////////////////////////////////////////////////////////////////////////////////

    public long getId() {
        return statReceive != null ? statReceive.getId() : -1L;
    }

    @Override
    public void preStart() throws Exception {
        super.preStart();
        StatSystem statSystem = StatSystem.system();
        if (statSystem.isEnabled()) {
            StatReceive ex = statReceive;
            statReceive = new StatReceive(
                    statSystem.getId(context().system(), self().path().toStringWithoutAddress()), self().path());
            if (ex != null){
                statReceive.add(ex);
            }
        }
    }

    @Override
    public void aroundReceive(PartialFunction<Object, BoxedUnit> receive, Object msg) {
        if (statReceive != null){
            statReceive.receiveBefore(msg);
        }
        try {
            super.aroundReceive(receive, msg);
        } finally {
            if (statReceive != null) {
                statReceive.receiveAfter(msg, "" + state);
            }
        }
    }

    /// logging ////////////////////////////////////////////////////////////////////////////////////////////////////////

    public void log(String fmt, Object... args) {
        log(config, fmt, args);
    }

    public static void log(ActorBirchConfig config, String fmt, Object... args) {
        if (config.debug) {
            System.err.println("[" + Thread.currentThread().getId() + "]#" + String.format(fmt, args)); //with newline
        }
    }

    @Override
    public String toString() {
        return "birchActor(" + state  +")";
    }

    /// receiver mapping  //////////////////////////////////////////////////////////////////////////////////////////////

    @Override
    public Receive createReceive() {
        return setReceiveBuilder(receiveBuilder())
                .build();
    }

    public ReceiveBuilder setReceiveBuilder(ReceiveBuilder builder) {
        return builder
                .match(BirchMessages.Put.class, withContext(this::putEntry))
                .match(BirchMessages.GetChildrenCfs.class, withContext(this::getChildrenCfs))
                .match(BirchMessages.SplitByNewEntry.class, withContext(this::splitByNewEntry))
                .match(BirchMessages.SplitByNewNode.class, withContext(this::splitByNewNode))
                .match(BirchMessages.PutWithChildrenCfs.class, withContext(this::putWithChildrenCfs))
                .match(BirchMessages.AddNewNodeEntry.class, withContext(this::addNewNodeEntry))
                .match(BirchMessages.AddNewNode.class, withContext(this::addNewNode))
                .match(BirchMessages.AddNewNodeToLink.class, withContext(this::addNewNodeToLink))
                .match(BirchMessages.SplitByNewNodeDelayed.class, withContext(this::splitByNewNodeDelayed))
                .match(BirchMessages.SetParentAsRoot.class, withContext(this::setParentAsRoot))
                .match(BirchMessages.End.class, this::end)
                .match(StatMessages.ActorId.class, this::actorId)

                .match(TraversalMessages.TraversalState.class, this::traverse)
                .match(BirchMessages.StatInconsistency.class, this::statInconsistency)
                .match(BirchMessages.FixChildIndex.class, this::fixChildIndex)

                /////////////////////////
                .match(BirchMessages.AddEntry.class, withContext(this::addEntry))
                .match(BirchMessages.GetParent.class, withContext(this::getParent))
                .match(BirchMessages.SetParent.class, withContext(this::setParent))
                .match(BirchMessages.GetClusteringFeature.class, withContext(this::getClusteringFeature))
                .match(BirchMessages.FindClosestChild.class, withContext(this::findClosestChild))
                .match(BirchMessages.GetChildSize.class, withContext(this::getChildSize))
                .match(BirchMessages.GetChildren.class, withContext(this::getChildren))
                .match(BirchMessages.SetChildren.class, withContext(this::setChildren))
                .match(BirchMessages.InsertAfterWithoutAddingCf.class, withContext(this::insertAfterWithoutAddingCf))
                .match(BirchMessages.IsTreeNode.class, withContext(this::isTreeNode))
                .match(BirchMessages.SetChildIndex.class, withContext(this::setChildIndex))
                .match(BirchMessages.GetChildIndex.class, withContext(this::getChildIndex));

    }

    public <MessageType> FI.UnitApply<MessageType> withContext(FI.UnitApply<MessageType> handler) {
        //EXT
        return handler;
//        return m -> {
//            try {
//                config.pushCaller(this, m);
//                handler.apply(m);
//            } finally {
//                config.popCaller(this);
//            }
//        };
    }

    /// BirchNode support (most of them are not used) //////////////////////////////////////////////////////////////////

    public <T> void returnToSender(BirchMessages.AskDirectResult<T> m, T msg) {
        if (m.direct) {
            m.returnedValue = msg;
        } else {
            sender().tell(msg, self());
        }
    }

    public void getParent(BirchMessages.GetParent m) {
        returnToSender(m, BirchMessages.toSerializable(config, state.getParent(), BirchMessages.SerializeCacheStrategy.Lazy, BirchMessages.debugCountGetParent));
    }

    public void setParent(BirchMessages.SetParent m) {
        //SET-RECEIVE-DEBUG: log("### receive SetParent\t%s\t%s", m.parent, self());
        if ((m.splitCount < 0 || m.splitCount >= this.parentSplitCount) && clockParent <= m.clock) {
            clockParent = m.clock;
            if (m.splitCount > this.parentSplitCount) {
                this.parentSplitCount = m.splitCount;
            }

            state.setParent(m.parent);
            if (m.parent != null) { //EXT
                root = false;
            }
        } else {
            log("### error: SetParent ignored: %s, this.parentSplitCount=%d", m, this.parentSplitCount);
        }
    }

    public void addEntry(BirchMessages.AddEntry ae) {
        state.addEntry(ae.entry);
    }

    public void getClusteringFeature(BirchMessages.GetClusteringFeature m) {
        returnToSender(m, state.getClusteringFeature());
    }

    public void findClosestChild(BirchMessages.FindClosestChild m) {
        returnToSender(m, state.findClosestChild(m.input, m.feature, config.distanceFunction));
    }


    public void getChildSize(BirchMessages.GetChildSize m) {
        returnToSender(m, state.getChildSize());
    }

    public void getChildren(BirchMessages.GetChildren m) {
        returnToSender(m, state.getChildren().stream()
                .map(e -> BirchMessages.toSerializable(config, e, BirchMessages.SerializeCacheStrategy.Lazy, BirchMessages.debugCountGetChildren))
                .collect(toArrayList()));
    }

    private static Collector<BirchNode, ArrayList<BirchNode>, ArrayList<BirchNode>> toArrayList() {
        return Collector.of(ArrayList::new, ArrayList::add, (l,r) -> { //guarantee serializable
            l.addAll(r);
            return l;
        });
    }

    public void setChildren(BirchMessages.SetChildren m) {
        state.setChildren(m.children);
    }

    public void insertAfterWithoutAddingCf(BirchMessages.InsertAfterWithoutAddingCf m) {
        state.insertAfterWithoutAddingCf(m.target, m.newChild);
    }

    public void isTreeNode(BirchMessages.IsTreeNode m) {
        returnToSender(m, config.isTreeNode(state));
    }

    public void setChildIndex(BirchMessages.SetChildIndex m) {
        if (clockChildIndex <= m.clock) {
            ((ActorBirchConfig.Indexed) state).setChildIndex(m.index);
        } else {
            log("#skip setChildIndex: clockChildIndex=%,d < m.clock=%,d : %s", clockChildIndex, m.clock, this);
        }
    }

    public void getChildIndex(BirchMessages.GetChildIndex m) {
        returnToSender(m, ((ActorBirchConfig.Indexed) state).getChildIndex());
    }

    /// tracing for debugging //////////////////////////////////////////////////////////////////////////////////////////


    public void runTrace(Consumer<BirchNodeCFTree.PutTrace> e) {
        BirchNodeCFTree.PutTrace t = BirchNodeCFTree.trace;
        synchronized (t) {
            ActorBirchBatch.getTraceRootExt().setNextNodeActor(this);
            e.accept(t); //it will be the same instance returned by getTraceRootExt() if debug on
        }
    }

    public void runTraceEx(Consumer<ActorBirchBatch.PutTraceRootExtension> f) {
        ActorBirchBatch.PutTraceRootExtension e = ActorBirchBatch.getTraceRootExt();
        synchronized (e) {
            e.setNextNodeActor(this);
            f.accept(e);
        }
    }

    /// generic functions for invoking and sending messages ////////////////////////////////////////////////////////////

    /** checks the type of node and,
     *   if the node holds an actor ref,
     *     and also the ref is one of self actor, then use state instead of the node and directly invoke.
     *   if the node holds an actor ref and it does not relate to self,
     *      then it send the message via the ref.
     *   if the node is a simple object,
     *      then directly invoke with the node.
     * */
    public <MsgType> void sendOrInvokeDirect(BirchNode node, MsgType sentMessage, BiConsumer<BirchNodeConcrete,MsgType> directRunner) {
        if (node instanceof ActorBirchRefNode) {
            ActorBirchRefNode refNode = ((ActorBirchRefNode) node);
            ActorRef ref = refNode.getRef();
            if (ref.equals(getSelf())) {
                invokeSelfState(refNode, sentMessage, directRunner);
            } else {
                send(refNode, ref, sentMessage);
            }
        } else if (node instanceof BirchNodeConcrete) {
            invokeNonActor((BirchNodeConcrete) node, sentMessage, directRunner);
        } else {
            log("error: sendOrInvokeDirect %s, %s", node, sentMessage);
        }
    }

    public <MsgType> void invokeSelfState(ActorBirchRefNode refNode, MsgType sentMessage, BiConsumer<BirchNodeConcrete,MsgType> directRunner) {
        directRunner.accept(state, sentMessage);
    }

    public <MsgType> void send(ActorBirchRefNode refNode, ActorRef ref, MsgType sentMessage) {
        send(ref, sentMessage);
    }

    public <MsgType> void invokeNonActor(BirchNodeConcrete node, MsgType sentMessage, BiConsumer<BirchNodeConcrete,MsgType> directRunner) {
        directRunner.accept(node, sentMessage);
    }

    public <MsgType> void sendOrInvokeDirectWithInvalidPath(BirchNode node, MsgType sentMessage,
                                                            BiConsumer<BirchNodeConcrete,MsgType> directRunner,
                                                            BiConsumer<BirchNodeInvalidPath,MsgType> directRunnerForInvalidPath) {
        if (node instanceof BirchNodeInvalidPath) {
            directRunnerForInvalidPath.accept((BirchNodeInvalidPath) node, sentMessage);
        } else {
            sendOrInvokeDirect(node, sentMessage, directRunner);
        }
    }

    public void send(ActorRef ref, Object msg) {
        if (config.debug) {
            config.recordStackInfo(msg.getClass().getSimpleName());
        }
        ref.tell(msg, ActorRef.noSender());
    }

    /// putting an input: the entry point of the algorithm /////////////////////////////////////////////////////////////

    public void putEntry(BirchMessages.Put m) {

        if (m.isRootPath() && hasParent()) {//EXT: not a root
            //m.rootChanger.tell(new BirchMessages.PutToRoot(m.entry, m.putCount + 1), ActorRef.noSender());
            if ((m.putCount % 100) == 0 && m.putCount > 1 || m.putCount == 2) {
                log("retry put %d : %s : %s  state.parent=%s", m.putCount, m, getSelf(), state.getParent());
                if (!m.toRoot) {
                    log("error %s", "toRoot=false");
                }
            }
            //finishList(m.path);
            config.sendDelay(new BirchMessages.PutToRoot(m.entry, m.putCount + 1), m.rootChanger, 10 + m.putCount); //maybe REMOTE
        } else {
            m.toRoot = false;
            runTraceEx(t -> t.putEntry(this, m.path, m.entry));
            if (config.isTreeNode(state)) {
                if (m.path.size() <= 1) {
                    runTrace(t -> t.putStart(state, m.entry));
                }
                state.getClusteringFeature().add(m.entry.getClusteringFeature()); //DIFF

                //collects CFs of child nodes
                nextChildrenCfsDirect(newGetChildrenCfs(m.getPathTop(),  //the current path has self ActorBirchRefNode
                        state.getChildren(), m, BirchMessages.GetChildrenCfsPostProcess.ProcessPut)); //at the end of the collection, it will call PutWithChildrenCfs back to self
            } else {
                if (!putNonActor(state, m)) { //put an data point to the state entry, or returns false if overflow
                    m.popPath(); //pop the self node
                    BirchNode parent = m.getPathTop();
                    //send AddNewNodeEntry back to the parent. the message will create a new entry node with the input data
                    //or non actor node: it might never happen

                    addNewNodeEntryOrLink(parent, new BirchMessages.AddNewNodeEntry(m)); //maybe REMOTE
                }
            }
        }
    }

    public boolean hasParent() {
        return state.getParent() != null;
    }

    public void addNewNodeEntryOrLink(BirchNode node, BirchMessages.AddNewNodeEntry m) {
        //EXT: TODO horizontal link traversal for adding input to entries
//        int maxDepth = m.put.getMaxDepth();
//        log("addNewNodeEntryToLink : maxDepth %d, path %d: %s %s", maxDepth, m.put.path.size(),
//                node, getSelf());

        sendOrInvokeDirect(node, m, this::addNewNodeEntryDirect);
    }

    /// collecting children and their CFs  /////////////////////////////////////////////////////////////////////////////
    /// parent -> child0 -> child1 -> child2 ... -> childLast -> back to parent
    ///   if there is a cache-less ref node as a child, then it needs to send messages

    public BirchMessages.GetChildrenCfs newGetChildrenCfs(BirchNode refParent,
                                                          List<BirchNode> children,
                                                          BirchMessages.Put m,
                                                          BirchMessages.GetChildrenCfsPostProcess postProcess) {
        //EXT
//        if (children.stream()
//                .anyMatch(ActorBirchRefNode.class::isInstance)) {
//            //some nodes are ref node, and then, message passing will happen
//            children = children.stream()
//                    .map(c -> BirchMessages.toSerializable(config, c))
//                    .collect(Collectors.toList()); //so, convert all nodes to ref nodes
//            refParent = BirchMessages.toSerializable(config, refParent);
//        }
        return new BirchMessages.GetChildrenCfs(m,
                0, children,
                new ArrayList<>(children.size()), refParent, postProcess);
    }

    public void getChildrenCfs(BirchMessages.GetChildrenCfs g) {
        runTraceEx(t -> t.getChildrenCfs(this, g.index, g.children, g.put.entry, g.postProcess, g.parent));

        //self is a child of g.parent: add CF of state to the message, and proceed to the next
        getChildrenCfsDirect(state, g);
    }

    public void getChildrenCfsDirect(BirchNode n, BirchMessages.GetChildrenCfs g) { //n can take a ref with caching a CF
        g.collected.add(new BirchMessages.NodeAndCf(g.index, g.children.get(g.index),
                n.getClusteringFeature()));
        ++g.index;
        nextChildrenCfsDirect(g);
    }

    public void nextChildrenCfsDirect(BirchMessages.GetChildrenCfs g) {
        if (g.index < g.children.size()) {
            //it remains a child
            int i = g.index;
            BirchNode n = g.children.get(i);
            //run on the child actor's getChildrenCfs
            //or immediate processing and proceed to the next
            //EXT
            if (n instanceof ActorBirchRefNode &&
                    ((ActorBirchRefNode) n).hasClusteringFeatureCache()) {
                getChildrenCfsDirect(n, g); //using cache without sending the message g
            } else {
                if (n instanceof ActorBirchRefNode) {
                    log("error: non cache user : %s", n);
                }
                sendOrInvokeDirect(n, g, this::getChildrenCfsDirect); //maybe REMOTE
            }
        } else {
            //at the end of the collection, it sends back to the parent
            Object m = g.postProcess.newMessage(g);
            sendOrInvokeDirectWithInvalidPath(g.parent, m, //maybe REMOTE
                    (p, msg) -> afterGetChildrenCfsDirect(msg),
                    (ip, msg) -> sendSplitByNewNodeInvalidPath(ip, (BirchMessages.SplitByNewNode) msg));
        }
    }

    public void afterGetChildrenCfsDirect(Object m) {
        if (m instanceof BirchMessages.SplitByNewEntry) {
            splitByNewEntryDirect((BirchMessages.SplitByNewEntry) m);
        } else if (m instanceof BirchMessages.SplitByNewNode) {
            splitByNewNodeDirect((BirchMessages.SplitByNewNode) m);
        } else {
            putWithChildrenCfsDirect((BirchMessages.PutWithChildrenCfs) m);
        }
    }

    /// continue to put with collected CFs  ////////////////////////////////////////////////////////////////////////////

    public void putWithChildrenCfs(BirchMessages.PutWithChildrenCfs m) {
        runTraceEx(t -> t.putWithChildrenCfs(this, m.put.path, m.put.entry, m.collected));
        putWithChildrenCfsDirect(m);
    }

    public void putWithChildrenCfsDirect(BirchMessages.PutWithChildrenCfs m) {
        BirchMessages.NodeAndCf selected = findClosestChild(m);
        //runTrace().findClosestChild(m.parent, selected == null ? null : selected.node, selectedChildIndex, selectedD, m.put.entry);
        if (selected == null) {
            //no children: add a new entry node to the root
            addNewNodeEntryOrLink(m.parent, new BirchMessages.AddNewNodeEntry(m.put, m.collected));
        } else {
            //go down to the selected node
            m.put.pushPath(selected.node); //for toSerializable: if the node is not a ref-node, currently suppose the sub-tree does not contain any remote node

            if (selected.node instanceof ActorBirchRefNode &&
                    ((ActorBirchRefNode) selected.node).hasClusteringFeatureCache()) { //EXT
                selected.node.getClusteringFeature().add(m.put.entry.getClusteringFeature()); //add CF to the cache
            }

            sendOrInvokeDirect(selected.node, m.put, (n, msg) -> { //maybe REMOTE
                if (!putNonActor(n, msg)) { //immediate processing here : n is not an actor (n never become ref of self())
                    msg.popPath(); //cancel the addition
                    addNewNodeEntryOrLink(m.parent, new BirchMessages.AddNewNodeEntry(msg, m.collected));
                }
            });
        }
    }

    public BirchMessages.NodeAndCf findClosestChild(BirchMessages.PutWithChildrenCfs m) {
        BirchNode.DistanceFunction distanceFunction = config.distanceFunction;
        BirchMessages.NodeAndCf selected = null;
        double selectedD = 0;
        ClusteringFeature cf = m.put.entry.getClusteringFeature();
        for (BirchMessages.NodeAndCf ncf : m.collected) {
            double childD = distanceFunction.distance(ncf.cf, cf);
            if (selected == null || selectedD > childD) {
                selected = ncf;
                selectedD = childD;
            }
        }
        return selected;
    }


    public boolean putNonActor(BirchNodeConcrete node, BirchMessages.Put m) {
        if (node instanceof BirchNodeCFEntry) {
            BirchNodeCFEntry nodeEntry = (BirchNodeCFEntry) node;
            if (config.canBeMerged(nodeEntry, m.entry)) {
                nodeEntry.addEntry(m.entry); //node.getClusteringFeature().add(m.entry.getClusteringFeature());
                finishPut(m, PutEventType.AddToEntry, nodeEntry);
                runTrace(t -> t.finishWithAddEntry(nodeEntry, m.entry));
                return true;
            } else {
                return false;
            }
        } else if (node instanceof BirchNodeCFTree) {
            node.getClusteringFeature().add(m.entry.getClusteringFeature());
            //collects children and go down
            BirchNode parent;
            if (node == state) {
                parent = m.getPathTop(); //path-top of m is ref-node of the actor that holds node: thus following message sending will be local-only
            } else {
                parent = node;
            }
            nextChildrenCfsDirect(newGetChildrenCfs(parent, node.getChildren(), m,
                    BirchMessages.GetChildrenCfsPostProcess.ProcessPut));
            return true;
        } else {
            log("error putNonActor %s %s", node, m);
            return true;
        }
    }

    /// adding a new entry (bottom) node ///////////////////////////////////////////////////////////////////////////////

    public void addNewNodeEntry(BirchMessages.AddNewNodeEntry m) {
        runTraceEx(t -> t.addNewNodeEntry(this, m.put.path, m.put.entry, m.children));
        addNewNodeEntryDirect(state, m);
    }

    public void addNewNodeEntryDirect(BirchNodeConcrete targetNode, BirchMessages.AddNewNodeEntry m) {
        if (config.canBeInsertedNewEntry(targetNode)) {
            if (targetNode instanceof BirchNodeCFTree) {
                addNewNodeEntryNewEntry((BirchNodeCFTree) targetNode, m);
            } else if (targetNode instanceof BirchNodeCFEntry) { //it will never happen
                targetNode.getIndexes().add(m.put.entry.getId());
                log("addNewNodeEntryDirect %s, %s", targetNode, m);
                finishPut(m.put, PutEventType.AddToEntry, targetNode);
            } else {
                //error
                log("error addNewNodeEntryDirect: %s, %s", targetNode, m);
            }
        } else {
            //overflow : split
            if (m.children == null) {
                nextChildrenCfsDirect(newGetChildrenCfs(m.put.getPathTop(), targetNode.getChildren(), //maybe REMOTE
                        m.put, BirchMessages.GetChildrenCfsPostProcess.ProcessSplitByNewEntry)); //path.get(0): is this OK?
                //it will creates SplitByNewEntry -> splitByNewEntry
            } else {
                splitByNewEntryDirect(new BirchMessages.SplitByNewEntry(m.put, m.put.getPathTop(), m.children));
            }
        }
    }

    public void addNewNodeEntryNewEntry(BirchNodeCFTree targetNode, BirchMessages.AddNewNodeEntry m) {
        BirchNodeConcrete newEntry = config.newEntry(m.put.entry);
        targetNode.addChildWithoutAddingCf(newEntry); //DIFF
        //CF is already added by putEntry if necessary (tree node cases)
        setParent(m.put.getPathTop(), newEntry); //path top is self node
        finishPut(m.put, PutEventType.NewEntry, targetNode, newEntry);
        runTrace(t -> t.finishWithAddNodeEntry(targetNode, newEntry, m.put.entry));
    }

    /// splitting a entry (bottom) node ////////////////////////////////////////////////////////////////////////////////

    public void splitByNewEntry(BirchMessages.SplitByNewEntry m) {
        runTraceEx(t -> t.splitByNewEntry(this, m.put.path, m.put.entry, m.collected));
        splitByNewEntryDirect(m);
    }

    public void splitByNewEntryDirect(BirchMessages.SplitByNewEntry m) {
        if (!m.includesNewEntry) { //usually m.parent is a ref of self
            BirchNodeConcrete newEntry = config.newEntry(m.put.entry);
            newEntry.setParent(m.parent);  /*changed from setParent(state.getParent()): this would be a bug*/
            m.collected.add(new BirchMessages.NodeAndCf(m.collected.size(),
                    newEntry, m.put.entry.getClusteringFeature()));
            m.includesNewEntry = true; //the new entry is appended
        }
        List<List<BirchMessages.NodeAndCf>> pair = splitChildrenToFarthestPair(m.collected);
        List<BirchMessages.NodeAndCf> left = pair.get(0);
        List<BirchMessages.NodeAndCf> right = pair.get(1);

        //if the parent is ref node, "this" is the target of the ref
        //if (((ActorBirchRefNode) m.parent).getRef().equals(getSelf())) {
        //same process as the non actor with state
        sendOrInvokeDirect(m.parent, m, (p, msg)-> split(msg.put, p, left, right));
    }


    /// shared functions for setting children and parents //////////////////////////////////////////////////////////////

    public void setChildren(ActorBirchConfig.IndexedNode parent, List<BirchMessages.NodeAndCf> es) {
        parent.getClusteringFeature().clear();
        BirchNodeCFTree.NodeHolder prev = null;
        BirchNodeCFTree.NodeHolder top = null;
        int i = 0;
        for (BirchMessages.NodeAndCf ncf : es) {
            parent.getClusteringFeature().add(ncf.cf);
            ((ActorBirchConfig.Indexed) ncf.node).setChildIndex(i); // update the index
            BirchNodeCFTree.NodeHolder next = new BirchNodeCFTree.NodeHolder(ncf.node);
            if (prev == null) {
                top = next;
            } else {
                prev.next = next;
                next.prev = prev;
            }
            prev = next;
            ++i;
        }
        parent.setChildNodeHolder(top);
        parent.setChildTailNodeHolder(prev);

        parent.setChildSize(es.size());
        //at the construction process, it does not do child.setParent(parent): all parent nodes are obtained from path list.
    }


    public void setParent(BirchNode parent, BirchNode child) {
        //EXT
        if (child instanceof ActorBirchRefNode) {
            ActorBirchRefNode refChild = (ActorBirchRefNode) child;
            refChild.setParentRef(parent);
            if (refChild.getRef().equals(getSelf())) {
                state.setParent(parent);
                root = false;
            } else {
                refChild.getRef().tell(new BirchMessages.SetParent(RemoteManager.incrementAndGetId(),
                        parent, splitCount), ActorRef.noSender());
            }
        } else {
            child.setParent(parent);
        }
    }

    public void setParents(BirchNode parent, List<BirchMessages.NodeAndCf> es) {
        //EXt
        es.forEach(e -> setParent(parent, e.node));
    }

    /// splitting algorithm ////////////////////////////////////////////////////////////////////////////////////////////

    public List<List<BirchMessages.NodeAndCf>> splitChildrenToFarthestPair(List<BirchMessages.NodeAndCf> collected) {
        int l = collected.size();
        double maxD = 0;
        BirchMessages.NodeAndCf maxL = null;
        BirchMessages.NodeAndCf maxR = null;
        for (int i = 0; i < l; ++i) {
            BirchMessages.NodeAndCf ncf1 = collected.get(i);
            for (int j = i + 1; j < l; ++j) {
                BirchMessages.NodeAndCf ncf2 = collected.get(j);
                double d = config.distanceFunction.distance(ncf1.cf, ncf2.cf);
                if (maxL == null) {
                    maxL = ncf1;
                    maxR = ncf2;
                    maxD = d;
                } else if (maxD < d) {
                    maxL = ncf1;
                    maxR = ncf2;
                    maxD = d;
                }
            }
        }
        return splitChildrenToFarthestPairWithTopNodes(collected, maxL, maxR, config.distanceFunction);
    }

    public List<List<BirchMessages.NodeAndCf>> splitChildrenToFarthestPairWithTopNodes(List<BirchMessages.NodeAndCf> es,
                                                                                       BirchMessages.NodeAndCf l,
                                                                                       BirchMessages.NodeAndCf r,
                                                                                       BirchNode.DistanceFunction distanceFunction) {
        List<BirchMessages.NodeAndCf> left = new ArrayList<>(es.size());
        List<BirchMessages.NodeAndCf> right = new ArrayList<>(es.size());
        for (BirchMessages.NodeAndCf ncf : es) {
            if (ncf == l) {
                left.add(ncf);
            } else if (ncf == r) {
                right.add(ncf);
            } else if (distanceFunction.distance(ncf.cf, l.cf) <
                        distanceFunction.distance(ncf.cf, r.cf)) {
                left.add(ncf);
            } else {
                right.add(ncf);
            }
        }
        return ArraysAsList.get(left, right);
    }


    public void split(BirchMessages.Put put, BirchNodeConcrete exParent,
                      List<BirchMessages.NodeAndCf> left,
                      List<BirchMessages.NodeAndCf> right) {
        ClusteringFeature exCf = exParent.getClusteringFeature().copy();
        splitSetChildrenLeft(put, (ActorBirchConfig.IndexedNode) exParent, left);
        ClusteringFeature leftNodeCfDiff = diff(exCf, exParent.getClusteringFeature());
        splitAfterSetChildrenLeft(put, exParent, leftNodeCfDiff);

        BirchNode exParentRef = put.getPathTop();
        BirchMessages.AddNewNode n = splitSetChildrenRight(put, right, exParentRef, leftNodeCfDiff, exParent.getParent());

        BirchNode newParent = n.node;
        ClusteringFeature rightNodeCf = n.cf;

        runTrace(t -> t.split(exParent, newParent, BirchMessages.NodeAndCf.nodes(left), BirchMessages.NodeAndCf.nodes(right), put.entry));

        if (!n.put.path.isEmpty()) {
            BirchNode upper = n.put.getPathTop();
            sendOrInvokeDirectWithInvalidPath(upper, n, this::addNewNodeDirect,
                    this::sendAddNewNodeInvalidPath);
        } else {
            splitNewRoot(put, n, exParentRef, leftNodeCfDiff, newParent, rightNodeCf);
        }
    }

    /** suppose a is already copied. returns a */
    public ClusteringFeature diff(ClusteringFeature a, ClusteringFeature b) {
        a.remove(b);
        return a;
    }

    public void splitSetChildrenLeft(BirchMessages.Put put, ActorBirchConfig.IndexedNode exParent, List<BirchMessages.NodeAndCf> left) {
        setChildren(exParent, left);
        BirchNode parent = put.getPathTop();
        setParents(parent, left);
    }

    public void splitAfterSetChildrenLeft(BirchMessages.Put put, BirchNodeConcrete exParent, ClusteringFeature leftNodeCfDiff) {
        //EXT
        BirchNode exParentInPath = put.getPathTop();
        if (exParentInPath instanceof ActorBirchRefNode &&
                ((ActorBirchRefNode) exParentInPath).hasClusteringFeatureCache()) {
            exParentInPath.getClusteringFeature().remove(leftNodeCfDiff); //update the cache CF
            //the ref may be placed in another VM: updated by later moving up message
        }
    }

    public BirchMessages.AddNewNode splitSetChildrenRight(BirchMessages.Put put, List<BirchMessages.NodeAndCf> rightChildren,
                                                          BirchNode exParentRef, ClusteringFeature newLeftCfDiff,
                                                          BirchNode exParentOfParent) {
        int dim = put.entry.getClusteringFeature().getDimension();
        ActorBirchConfig.IndexedNode rightNode = new ActorBirchConfig.IndexedNode(config,
                dim);
        setChildren(rightNode, rightChildren);

        BirchNode newParent = newNode(rightNode, exParentOfParent);
          //set the initial parent of the new right node to the same parent of the left node
        setParents(newParent, rightChildren);

        put.popPath();
        return new BirchMessages.AddNewNode(put,
                newParent, rightNode.getClusteringFeature(),
                exParentRef, newLeftCfDiff);
    }

    public void splitNewRoot(BirchMessages.Put put, BirchMessages.AddNewNode n,
                             BirchNode exParentRef, ClusteringFeature leftNodeCf,
                             BirchNode newParent, ClusteringFeature rightNodeCf) {
        //EXT
        //the path might be wrong
        if (state.getParent() != null) { //if the state has a parent, the actor node is no longer the root
            BirchNode upper = state.getParent();

            //the method is called from split when the path is empty,
            // and it recognizes a new node in the path, so fix the depth
            if (upper instanceof ActorBirchRefNode) {
                n.put.pushPath(upper); //adding the upper with fixing the maxDepth
            }

            sendOrInvokeDirect(upper, n, this::addNewNodeDirect);
        } else if (!root) {
            log("splitByNewNodeDelayed from splitNewRoot: !root bug parent==null maxDepth=%d", n.put.getMaxDepth());
            config.sendDelay(new BirchMessages.SplitByNewNodeDelayed(null, n, 0), getSelf(), 10);
        } else {
            this.root = false;

            int dim = put.entry.getClusteringFeature().getDimension();
            //update root
            ActorBirchConfig.IndexedNode newRoot = new ActorBirchConfig.IndexedNode(config, dim);
            BirchMessages.NodeAndCf rootLeft = new BirchMessages.NodeAndCf(0, exParentRef, leftNodeCf);
            BirchMessages.NodeAndCf rootRight = new BirchMessages.NodeAndCf(1, newParent, rightNodeCf);
            List<BirchMessages.NodeAndCf> rootChildren = ArraysAsList.get(rootLeft, rootRight);
            setChildren(newRoot, rootChildren);
            BirchNode newRootRef = newNodeRoot(newRoot);
            setParents(newRootRef, rootChildren);

            setParent(newRootRef, exParentRef);

            runTrace(t -> t.newRoot(newRootRef, rootLeft.node, rootRight.node, put.entry));
            //the new right node has responsibility for sending the new root to the root holder
            sendOrInvokeDirect(newParent,
                    new BirchMessages.SetParentAsRoot(put, new BirchMessages.SetParent(RemoteManager.incrementAndGetId(), newRootRef, -1)),
                    this::setParentAsRootDirect);
        }
    }

    private BirchNode newNodeRoot(ActorBirchConfig.IndexedNode state) {
        return newNode(state, null);
    }

    public BirchNode newNode(ActorBirchConfig.IndexedNode state, BirchNode parent) {
        boolean root = (parent == null);
        //Note: a change of the parent always happens in the right hand side of the self node.
        //       Also it will not over the existing right link:
        //       e.g.  suppose c is a child of p1. denote p[c] where c is the counter of the node p.
        //           S1.  {split(p1); p1[0]->p2[1];               c!SetParent(p1,0) }
        //           S2.  {split(p1); p1[0]->p3[1]->p2[1];        c!SetParent(p3,1) }
        //           S3.  {split(p3); p1[0]->p3[1]->p4[2]->p2[1]; c!SetParent(p4,2) }
        //              SetParents of S1-S3 might happen simultaneously,
        //                but if c!SetParent(p2,1) in S1, S2 and S3 will never happen. //really? how about p4 -> p2 ?
        //
        //       So it will be sufficient to only pass monotonically incrementing counter to the right node.
        //       The counter always satisfies l.splitCount < r.splitCount for 2 nodes l and r,
        //              if r is the right side of l (transitively linked by l.linkRight)
        if (!root) {
            state.setParent(parent); //before passing to create
        }
        long id = state.getNodeId();
        ActorRef ref = config.create(ActorBirchNodeActor.class,
                new ActorBirchConfig.NodeCreator(state, config, getSelf(), linkRight, root, root ? 0 : (splitCount + 1), id));
        ActorBirchRefNode refNode = new ActorBirchRefNode(ref, config, state.getClusteringFeature().copy(), id);
        if (!root) { //if root, the node is the upper of self
            this.linkRight = ref;
            refNode.setParentRef(parent);
        }
        return refNode;
    }

    public void setParentAsRoot(BirchMessages.SetParentAsRoot m) {
        setParent(m.setParent);
        setParentAsRootDirect(state, m);
    }

    public void setParentAsRootDirect(BirchNodeConcrete target, BirchMessages.SetParentAsRoot m) {
        BirchNode newRootRef = m.setParent.parent;
        log("sendChangeRoot: %s : %s : state.parent=%s", newRootRef, m.put, state.getParent());
        send(m.put.rootChanger, new BirchMessages.ChangeRoot(newRootRef, 1 + m.put.getMaxDepth(), m.put));
        finishPut(m.put, PutEventType.NewRoot, target, newRootRef);
    }

    /// go up to the parent node in the tree ///////////////////////////////////////////////////////////////////////////

    public void addNewNode(BirchMessages.AddNewNode m) {
        runTraceEx(t -> t.addNewNode(this, m.put.path, m.put.entry, m.node, m.cf));

        boolean fixed = fixChildCf(m);
        if (!fixed && hasLinkRight()) { //EXT
            //  pathTop: this: state { children: m.existingRef }
            send(linkRight, new BirchMessages.AddNewNodeToLink(
                    m, linkLeft, linkRight, 0));
        } else {
            //BirchNode parent = m.put.getPathTop();
            //sendOrInvokeDirectWithInvalidPath(parent, m, this::addNewNodeDirect, //usually, parent is self
            //        this::sendAddNewNodeInvalidPath); //the case of an invalid path is considered by the caller
            addNewNodeDirect(state, m);
        }
    }

    //EXT
    public boolean hasLinkRight() {
        return linkRight != null;
    }

    //EXT
    public boolean fixChildCf(BirchMessages.AddNewNode m) {
        boolean found = true;
        if (m.existingSplit != null && m.existingSplit instanceof ActorBirchRefNode) {
            found = false;
            for (BirchNode node : state.getChildren()) {
                if (matchRef(m.existingSplit, node)) {
                    //if the node instance is m.existingSplit,
                    // it is already modified by the above splitSetChildrenLeft as in the same VM
                    found = true;
                    if (m.existingSplitCfDiff != null && node != m.existingSplit) {
                        //    existingSplitCfDiff = oldCf - newCf
                        // => newCfCache = oldCf - existingSplitCfDiff
                        ClusteringFeature cfCache = node.getClusteringFeature();
                        cfCache.remove(m.existingSplitCfDiff);
                    }
                    break;
                }
            }
        }
        return found;
    }


    public void addNewNodeDirect(BirchNodeConcrete target, BirchMessages.AddNewNode m) {
        if (config.canBeInsertedNewEntry(target)) {
            ActorBirchConfig.IndexedNode n = (ActorBirchConfig.IndexedNode) target;
            n.addChildWithoutAddingCf(m.node);
            BirchNode nRef = m.put.getPathTop();
            if (nRef instanceof BirchNodeInvalidPath) {
                nRef = newTemporaryRefNode(target);
                m.put.replacePathTop(nRef);
            }
            setParent(nRef, m.node);
            finishPut(m.put, PutEventType.NewNode, n);
            runTrace(t -> t.finishWithAddNodeTree(target, null, m.node, m.put.entry));
        } else {
            //split
            if (!m.put.path.isEmpty()) {
                BirchNode ref = m.put.getPathTop();
                BirchMessages.SplitByNewNode n = new BirchMessages.SplitByNewNode(m.put, ref, null, m.node);
                sendOrInvokeDirectWithInvalidPath(ref, n, this::splitByNewNodeDirect,
                        this::sendSplitByNewNodeInvalidPath);
                //as the initial state, the collected list is null
            } else {
                log("error addNewNodeDirect empty path: %s %s", target, m); //never
            }
        }
    }

    /// splitting an upper node ////////////////////////////////////////////////////////////////////////////////////////

    public void splitByNewNode(BirchMessages.SplitByNewNode m) {
        runTraceEx(t -> t.splitByNewNode(this, m.put.path, m.put.entry, m.collected, m.newNode));
        splitByNewNodeDirect(state, m);
    }

    public void splitByNewNodeDirect(BirchMessages.SplitByNewNode m) {
        //EXT
        //adding the input CF as one of a new entry, which will be part of summarized sub-CFs
        if (m.parent instanceof ActorBirchRefNode) {
            //m.parent is always self
            state.getClusteringFeature().add(m.put.entry.getClusteringFeature());
        } else {
            m.parent.getClusteringFeature().add(m.put.entry.getClusteringFeature());
        }

        splitByNewNodeDirect(state, m);
    }

    public void splitByNewNodeDirect(BirchNodeConcrete target, BirchMessages.SplitByNewNode m) {
        if (m.collected == null) {
            //re-run after collecting CFs
            List<BirchNode> cs = new ArrayList<>(target.getChildren());
            if (m.newNode != null) { //add the new node as an additional child
                cs.add(m.newNode);
            }
            nextChildrenCfsDirect(newGetChildrenCfs(m.parent, cs, m.put,
                    BirchMessages.GetChildrenCfsPostProcess.ProcessSplitByNewNode));
        } else {
            List<List<BirchMessages.NodeAndCf>> pair = splitChildrenToFarthestPair(m.collected);
            sendOrInvokeDirect(m.parent, m, (p, msg) -> split(msg.put, p, pair.get(0), pair.get(1)));
        }
    }

    /// EXT: horizontal traversing via links ///////////////////////////////////////////////////////////////////////////

    public void addNewNodeToLink(BirchMessages.AddNewNodeToLink toLink) {
        updatePathConditionally(toLink.newNode.put);
        // path: { invalid(ref) where ref!=self, state.parent, state.parent.parent, ... }

        runTraceEx(t -> t.addNewNodeToLink(this, toLink.newNode.put.path,
                toLink.newNode.put.entry, toLink.newNode.node, toLink.newNode.cf, toLink.count));
        boolean fixed = fixChildCf(toLink.newNode);
        if (!fixed && hasLinkRight() && toLink.count < 3) {
            toLink.nextRight = linkRight;
            toLink.count++;
            send(linkRight, toLink);
        } else {
            log("addNewNode: fixed=%s count=%d", "" + fixed, toLink.count);
            addNewNodeDirect(state, toLink.newNode);
        }
    }

    public void updatePathConditionally(BirchMessages.Put put) {
        BirchNode top = put.getPathTop();
        if ((top instanceof ActorBirchRefNode &&
                !((ActorBirchRefNode) top).getRef().equals(getSelf())) || //wrong path
                top instanceof ActorBirchNodeActor.BirchNodeInvalidPath) {
            updatePath(put);
        }
    }

    public void updatePath(BirchMessages.Put put) {
        put.path = new ArrayList<>(put.path);
        ListIterator<BirchNode> pi = put.path.listIterator();
        BirchNode n = pi.next();
        pi.set(invalid(n));
        BirchNode next = state;
        while (next != null) {
            next = next.getParent();
            if (next != null) {
                if (pi != null && pi.hasNext()) {
                    BirchNode ex = pi.next();
                    if (matchRef(next, ex)) {
                        break;
                    } else {
                        pi.set(next); //update an existing path element
                    }
                } else {
                    pi = null;
                    put.path.add(next); //append a deeper path element
                    put.maxDepth++; //
                }
            }
        }
        while (pi != null && pi.hasNext()) { //fill the rest of the path
            BirchNode o = pi.next();
            pi.set(invalid(o));
        }
    }

    private BirchNode invalid(BirchNode o) {
        if (o instanceof ActorBirchNodeActor.BirchNodeInvalidPath) {
            return o;
        } else {
            return new ActorBirchNodeActor.BirchNodeInvalidPath(o);
        }
    }

    public boolean matchRef(BirchNode l, BirchNode r) {
        if (l instanceof ActorBirchRefNode && r instanceof ActorBirchRefNode) {
            return ((ActorBirchRefNode) l).getRef().equals(((ActorBirchRefNode) r).getRef());
        } else {
            return l.equals(r);
        }
    }

    public static class BirchNodeInvalidPath extends BirchNodeInput implements Serializable {
        protected BirchNode original;

        public BirchNodeInvalidPath(BirchNode original) {
            this.original = original;
        }

        public BirchNode getOriginal() {
            return original;
        }

        @Override
        public String toString() {
            return "invalid(" + original + ")";
        }
    }

    /// EXT: handling an invalid path element //////////////////////////////////////////////////////////////////////////

    public void sendSplitByNewNodeInvalidPath(ActorBirchNodeActor.BirchNodeInvalidPath invalidPath, BirchMessages.SplitByNewNode m) {
        //create a temporarily ref for self and replace the invalidPath with it
        m.parent = newTemporaryRefNode(state);
        m.put.replacePathTop(m.parent);
        splitByNewNodeDirect(state, m);
        //TODO CF might be illegal?
    }

    public BirchNode newTemporaryRefNode(BirchNodeConcrete target) {
        if (target.equals(state)) {
            return new ActorBirchRefNode(getSelf(), config, true/*target.getClusteringFeature().copy()*/, nodeId);
        } else {
            return target;
        }
    }

    public void splitByNewNodeDelayed(BirchMessages.SplitByNewNodeDelayed d) {
        if (state.getParent() != null) {
            log("successfully delayed!: %s", d);
        }
        if (d.message instanceof BirchMessages.SplitByNewNode) {
            sendSplitByNewNodeInvalidPath((ActorBirchNodeActor.BirchNodeInvalidPath) d.invalidPath, (BirchMessages.SplitByNewNode) d.message);
        } else if (d.message instanceof BirchMessages.AddNewNode) {
            sendAddNewNodeInvalidPath((ActorBirchNodeActor.BirchNodeInvalidPath) d.invalidPath, (BirchMessages.AddNewNode) d.message, d.count);
        } else {
            log("error invalid message: %s", d);
        }
    }

    public void sendAddNewNodeInvalidPath(ActorBirchNodeActor.BirchNodeInvalidPath invalidPath, BirchMessages.AddNewNode m) {
        sendAddNewNodeInvalidPath(invalidPath, m, 0);
    }

    public void sendAddNewNodeInvalidPath(ActorBirchNodeActor.BirchNodeInvalidPath invalidPath, BirchMessages.AddNewNode m, int count) {
        //addNewNodeToLink: path={invalidPath1,invalidPath2,...}
        //  -> addNewNodeDirect(state, ...): //the branch for split
        //      sendOrInvokeDirect(invalidPath1, n, splitByNewNodeDirect) //n is a SplitByNewNode and includes the invalid path
        //      -> sendSplitByNewNodeInvalidPath(invalidPath1, n): //ext
        //          sendOrInvokeDirect(state.parent, n, splitByNewNodeDirect)
        //        -> splitByNewNodeDirect(state, n)
        //         ->  split(...)
        //             sendOrInvokeDirect(upper=invalidPath2, AddNewNode, addNewNodeDirect) ->
        BirchNode p = state.getParent();
        if (p != null) {
            //TODO parent is serializable?
            if (m.put.path.isEmpty()) { //the case of root=false in splitNewRoot: then the maxDepth is not fixed yet
                m.put.pushPath(p); //increasing the maxDepth
            } else {
                m.put.replacePathTop(p);
            }

            //TODO updating m.existingSplitDiff ?
            if (m.existingSplit instanceof BirchNodeInvalidPath) {
                m.existingSplit = p;
            }
            sendOrInvokeDirect(p, m, this::addNewNodeDirect);
        } else if (!root) {
            if (count > 2) {
                log("retry %d : %s", count, m);
            }
            config.sendDelay(new BirchMessages.SplitByNewNodeDelayed(invalidPath, m, count + 1), getSelf(), 10L + count * 10L);
        } else {
            log("error: the root needs to horizontal traverse from linkRight %s ?", m);
        }
    }

    /// finish process /////////////////////////////////////////////////////////////////////////////////////////////////


    public enum PutEventType {
        NewRoot,
        NewNode,
        NewEntry,
        AddToEntry,
    }

    public void finishPut(BirchMessages.Put put, PutEventType type, BirchNode target, Object... args) {
        send(config.getFinishActor(), put);
    }


    /// actor visiting support /////////////////////////////////////////////////////////////////////////////////////////

    public void traverse(TraversalMessages.TraversalState<BirchNode, BirchNode.BirchVisitor> t) {
        t.accept(this, state);
    }

    public void actorId(StatMessages.ActorId m) {
        this.queue = m.queue;
        if (queue != null && queue.getId() == -1 && statReceive != null){
            queue.setId(statReceive.getId());
        }
    }

    public void end(BirchMessages.End e) {
        if (queue != null) {
            this.queue.checkAndSave(true);
        }
        if (statReceive != null) {
            this.statReceive.saveTotal();
        }

        endTree(state);
        self().tell(PoisonPill.getInstance(), self());
    }

    public static void endTree(BirchNode n) {
        if (n instanceof ActorBirchRefNode) {
            ((ActorBirchRefNode) n).getRef().tell(new BirchMessages.End(), ActorRef.noSender());
        } else if (n instanceof BirchNodeCFTree) {
            n.getChildren().forEach(ActorBirchNodeActor::endTree);
        } //else : nothing to do
    }

    /// node fix after construction ////////////////////////////////////////////////////////////////////////////////////

    public void statInconsistency(BirchMessages.StatInconsistency m) {
        statInconsistency(config, state, m, true);
    }

    public static void statInconsistencyStart(ActorBirchConfig config, BirchNode child, BirchNode parent, int childIndex, ActorRef finisher) {
        BirchMessages.StatInconsistency m = new BirchMessages.StatInconsistency(parent, childIndex, finisher);
        statInconsistency(config, child, m, false);
        if (child instanceof ActorBirchRefNode) {
            m.parent = BirchMessages.toSerializable(config, m.parent, BirchMessages.SerializeCacheStrategy.Lazy);
            ((ActorBirchRefNode) child).tell(m);
        }
    }

    public static void statInconsistency(ActorBirchConfig config, BirchNode node, BirchMessages.StatInconsistency m, boolean actor) {
        BirchMessages.StatInconsistencyReport r = new BirchMessages.StatInconsistencyReport();
        r.actor = actor;
        r.refNode = (node instanceof ActorBirchRefNode);
        r.entry = (node instanceof BirchNodeCFEntry);
        BirchNode parent = node.getParent();
        long parentId = getNodeId(parent);
        long checkingId = getNodeId(m.parent);
        r.inconsistentParent = (checkingId != parentId);
        log(config, "statInconsistency %s: parent=%s (id=%d), m.parent=%s (id=%d) inconsistentParent=%s",
                node, parent, parentId, m.parent, checkingId, r.inconsistentParent);

        if (node instanceof ActorBirchConfig.Indexed) {
            long childIndex = ((ActorBirchConfig.Indexed) node).getChildIndex();
            r.inconsistentChildIndex = (childIndex != m.childIndex);
            log(config, "statInconsistency %s: childIndex=%d, m.childIndex=%d inconsistentChildIndex=%s",
                    node, childIndex, m.childIndex, r.inconsistentChildIndex);
        }

        m.finisher.tell(r, ActorRef.noSender());

        if (node instanceof BirchNodeCFTree) {
            int i = 0;
            List<BirchNode> cs = node.getChildren();
            if (config.debug) {
                log(config, "statInconsistency %s: childIndices=%s",
                        node,
                        IntStream.range(0, cs.size()).mapToObj(x -> {
                            BirchNode n = cs.get(x);
                            if (n instanceof ActorBirchConfig.Indexed) {
                                return x + ":" + ((ActorBirchConfig.Indexed) n).getChildIndex();
                            } else {
                                return x + ":?";
                            }
                        }).collect(Collectors.joining(", ", "[", "]")));
            }
            for (BirchNode c : cs) {
                statInconsistencyStart(config, c, node, i, m.finisher);
                ++i;
            }
        }
    }

    public static long getNodeId(BirchNode node) {
        if (node instanceof ActorBirchConfig.IndexedNode) {
            return ((ActorBirchConfig.IndexedNode) node).getNodeId();
        } else if (node instanceof ActorBirchRefNode) {
            return ((ActorBirchRefNode) node).getNodeId();
        } else {
            return -1L;
        }
    }

    public void fixChildIndex(BirchMessages.FixChildIndex m) {
        fixChildIndex(config, state, m);
    }

    public static void fixChildIndexStart(ActorBirchConfig config, BirchNode child, BirchNode parent, int depth, ActorRef finisher) {
        BirchMessages.FixChildIndex m = new BirchMessages.FixChildIndex(parent, depth, finisher);
        fixChildIndex(config, child, m);
    }

    public static boolean sameNode(BirchNode a, BirchNode b) {
        if (a == null || b == null) {
            return a == b;
        } else if (a == b || a.equals(b)) {
            return true;
        } else {
            long ai = getNodeId(a);
            long bi = getNodeId(b);
            return ai != -1L && ai == bi;
        }
    }

    public static void fixChildIndex(ActorBirchConfig config, BirchNode state, BirchMessages.FixChildIndex m) {
        if (state instanceof BirchNodeCFTree) {
            List<BirchNode> cs = state.getChildren();

            List<BirchNode> ncs = new ArrayList<>(cs.size());
            for (BirchNode c : cs) {
                c.setParent(m.parent); //reset for correcting parent inconsistency
                if (sameNode(c.getParent(), state)) {
                    ncs.add(c);
                }

                fixChildIndexStart(config, c, c,m.depth + 1, m.finisher); //maybe duplicated traversal, but precise messages for finisher
            }

            BirchMessages.FixChildIndexReport r = new BirchMessages.FixChildIndexReport();
            r.reducedChildSize = (cs.size() - ncs.size());
            m.finisher.tell(r, ActorRef.noSender());
            ((BirchNodeCFTree) state).setChildrenWithoutUpdatingCf(ncs); //childIndices of IndexedNode will be updated

        } else if (state instanceof ActorBirchRefNode) {
            ActorBirchRefNode ref = (ActorBirchRefNode) state;
            if (!ref.isLocalRef()) {
                m.parent = BirchMessages.toSerializable(config, m.parent, BirchMessages.SerializeCacheStrategy.Lazy);
            }
            ref.tell(m);
        } else if (state instanceof BirchNodeCFEntry) {
            BirchMessages.FixChildIndexReport r = new BirchMessages.FixChildIndexReport();
            r.entry = true;
            m.finisher.tell(r, ActorRef.noSender());
        }
    }
}
