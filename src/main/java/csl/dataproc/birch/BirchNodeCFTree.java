package csl.dataproc.birch;

import csl.dataproc.tgls.util.ArraysAsList;

import java.io.Serializable;
import java.util.*;

public class BirchNodeCFTree implements BirchNode, Iterable<BirchNode>, Serializable {
    protected BirchConfig config;
    protected ClusteringFeature clusteringFeature;
    protected NodeHolder child;
    protected NodeHolder childTail;
    protected int childSize;
    protected BirchNode parent;

    static final long serialVersionUID = 1L;

    public static class NodeHolder implements Serializable {
        public BirchNode node;
        public NodeHolder next;
        public NodeHolder prev;

        static final long serialVersionUID = 1L;

        public NodeHolder() {}

        public NodeHolder(BirchNode node) {
            this.node = node;
        }
    }

    public BirchNodeCFTree() {}

    public BirchNodeCFTree(BirchConfig config, int dimension) {
        this(config, new ClusteringFeature(dimension));
    }

    public BirchNodeCFTree(BirchConfig config, ClusteringFeature cf) {
        this.config = config;
        this.clusteringFeature = cf;
    }

    public BirchNodeCFTree(BirchConfig config, List<BirchNode> children) {
        this(config, children.get(0).getClusteringFeature().getDimension());
        setChildren(children);
    }

    public BirchNodeCFTree(BirchConfig config, ClusteringFeature cf, List<BirchNode> children) {
        this(config, cf);
        setChildrenWithoutUpdatingCf(children);
    }

    @Override
    public BirchNode getParent() {
        return parent;
    }

    @Override
    public void setParent(BirchNode parent) {
        this.parent = parent;
    }

    @Override
    public ClusteringFeature getClusteringFeature() {
        return clusteringFeature;
    }

    public BirchConfig getConfig() {
        return config;
    }

    public int getChildSize() {
        return childSize;
    }

    @Override
    public BirchNode findClosestChild(BirchNodeInput input, ClusteringFeature cf, DistanceFunction distanceFunction) {
        BirchNode selected = null;
        double selectedD = 0;
        int selectedChildIndex = -1;
        int i = 0;
        for (BirchNode child : this) {
            double childD = distanceFunction.distance(child.getClusteringFeature(), cf);
            if (selected == null || selectedD > childD) {
                selected = child;
                selectedD = childD;
                selectedChildIndex = i;
            }
            ++i;
        }
        getTrace().findClosestChild(this, selected, selectedChildIndex, selectedD, input);
        return selected;
    }

    /** if reverse == false: returnedList.get(0) == this */
    public List<BirchNode> findClosestPath(BirchNodeInput input, ClusteringFeature cf, DistanceFunction distanceFunction, boolean reverse) {
        ArrayList<BirchNode> path = new ArrayList<>();
        BirchNode e = this;
        while (e != null) {
            path.add(e);
            e = e.findClosestChild(input, cf, distanceFunction);
        }
        if (reverse) {
            Collections.reverse(path);
        }
        return path;
    }

    /** returns the root: usually this */
    public BirchNode put(BirchNodeInput e) {
        PutTrace trace = getTrace();
        trace.putStart(this, e);
        List<BirchNode> path = findClosestPath(e);

        Iterator<BirchNode> pathIter = path.iterator();
        BirchNode target = pathIter.next(); //a bottom node

        // not an entry (only the root yet), or inside of the diameter (usually) even an entry
        //  here is not a splitting process for bottom upping
        if (noSplit(target, e)) {
            target.addEntry(e);
            if (config.isTreeNode(target)) {
                trace.finishWithAddNodeEntry(target, null, e);
            } else {
                trace.finishWithAddEntry(target, e);
            }

            //special process for the initial state (only for the initial node and its children)
            while (pathIter.hasNext()) {
                pathIter.next().getClusteringFeature().add(e.getClusteringFeature());
            }
            return this;
        } else {
            //if an end-entry overs the diameter
            AddingToUpper adding = newAddingToUpper();
            adding.init(e);

            while (adding.stepPath(pathIter)) ;

            BirchNode ret = adding.stepPathEnd(pathIter, this);
             //if it has no root splitting, the root continues to be this
            return ret;
        }
    }



    public List<BirchNode> findClosestPath(BirchNodeInput e) {
        return findClosestPath(e, e.getClusteringFeature(), config.distanceFunction, true);
    }

    public boolean noSplit(BirchNode target, BirchNodeInput e) {
        return config.isTreeNode(target) || //this means there is no leaf node and then it is the initial state of root
                config.canBeMerged(target, e); //merging an existing entry
    }

    public AddingToUpper newAddingToUpper() {
        return new AddingToUpper(config);
    }

    public static class AddingToUpper {
        protected BirchConfig config;
        protected BirchNode targetChildEntry = null;
        protected BirchNode newChildEntry;
        protected ClusteringFeature inputCf;
        protected BirchNodeInput input;

        public AddingToUpper(BirchConfig config) {
            this.config = config;
        }

        public ClusteringFeature init(BirchNodeInput e) {
            this.input = e;
            newChildEntry = config.newEntry(e);
            inputCf = newChildEntry.getClusteringFeature();
            return inputCf;
        }

        public PutTrace getTrace() {
            return trace;
        }

        public boolean stepPath(Iterator<BirchNode> pathIter) {
            if (pathIter.hasNext()) {
                return step(pathIter.next());
            } else {
                return false;
            }
        }

        public BirchNode stepPathEnd(Iterator<BirchNode> pathIter, BirchNode root) {
            return stepPathEnd(pathIter, root, inputCf);
        }

        public BirchNode stepPathEnd(Iterator<BirchNode> pathIter, BirchNode root, ClusteringFeature cf) {
            if (!pathIter.hasNext()) {
                if (targetChildEntry == null) { //already added
                    return root;
                } else {
                    BirchNode newRoot = config.newNode(getAsRootChildren());
                    getTrace().newRoot(newRoot, targetChildEntry, newChildEntry, input);
                    return newRoot;
                }
            } else {
                addClusteringFeatureToUpperFrom(pathIter, cf);
                return root;
            }
        }

        public void addClusteringFeatureToUpperFrom(Iterator<BirchNode> pathIter, ClusteringFeature cf) {
            while (pathIter.hasNext()) {
                pathIter.next().getClusteringFeature().add(cf);
            }
        }

        public boolean step(BirchNode targetParent) {
            if (config.canBeInsertedNewEntry(targetParent)) {
                BirchNode targetChild = targetChildEntry;
                insertAfterWithoutAddingCf(targetParent);

                //due to do next a path iterator, only the absorbing node is not added CF (addition is limited for the input)
                targetParent.getClusteringFeature().add(inputCf);
                if (config.isTreeNode(this.newChildEntry)) {
                    getTrace().finishWithAddNodeTree(targetParent, targetChild, this.newChildEntry, this.input);
                } else {
                    getTrace().finishWithAddNodeEntry(targetParent, newChildEntry, this.input);
                }
                return false;
            } else {
                split(targetParent);
                return true;
            }
        }

        protected void insertAfterWithoutAddingCf(BirchNode targetParent) {
            targetParent.insertAfterWithoutAddingCf(targetChildEntry, newChildEntry);
            targetChildEntry = null;
        }

        protected void split(BirchNode targetParent) {
            List<BirchNode> es = targetParent.getChildren();
            BirchNode newCE = newChildEntry;
            es.add(newCE);

            List<List<BirchNode>> split = split(es, config.distanceFunction);
            List<BirchNode> left = split.get(0);
            List<BirchNode> right = split.get(1);

            setChildren(targetParent, left);

            newChildEntry = config.newNode(right);
             //a new sibling of targetParent (added as a targetParent's child = as a newChild)
            targetChildEntry = targetParent;

            getTrace().split(targetParent, newChildEntry, left, right, input);
        }

        protected void setChildren(BirchNode targetParent, List<BirchNode> cs) {
            targetParent.setChildren(cs);
        }

        public List<List<BirchNode>> split(List<BirchNode> es, DistanceFunction distanceFunction) {
            BirchNode[] pair = farthestPair(es, distanceFunction);
            BirchNode l = pair[0];
            BirchNode r = pair[1];
            List<BirchNode> left = new ArrayList<>(es.size());
            List<BirchNode> right = new ArrayList<>(es.size());
            for (BirchNode child : es) {
                if (child == l) {
                    left.add(child);
                } else if (child == r) {
                    right.add(child);
                } else if (distanceFunction.distance(child.getClusteringFeature(), l.getClusteringFeature()) <
                        distanceFunction.distance(child.getClusteringFeature(), r.getClusteringFeature())) {
                    left.add(child);
                } else {
                    right.add(child);
                }
            }
            return ArraysAsList.get(left, right);
        }

        public BirchNode[] farthestPair(Iterable<BirchNode> es, DistanceFunction distanceFunction) {
            BirchNode[] maxPair = null;
            double maxD = 0;

            for (BirchNode e1 : es) {
                ClusteringFeature e1cf = null;
                for (BirchNode e2 : es) {
                    if (e1 != e2) {
                        if (e1cf == null) {
                            e1cf = e1.getClusteringFeature();
                        }
                        double d = distanceFunction.distance(e1cf, e2.getClusteringFeature());
                        if (maxPair == null) {
                            maxPair = new BirchNode[] {e1, e2};
                            maxD = d;
                        } else if (maxD < d) {
                            maxPair[0] = e1;
                            maxPair[1] = e2;
                            maxD = d;
                        }
                    }
                }
            }
            return maxPair;
        }

        public List<BirchNode> getAsRootChildren() {
            return ArraysAsList.get(targetChildEntry, newChildEntry);
        }

        public BirchNode getTargetChildEntry() {
            return targetChildEntry;
        }
    }


    @Override
    public void addEntry(BirchNodeInput e) {
        BirchNode newChildEntry = config.newEntry(e);
        addChildWithoutAddingCf(newChildEntry);
        getClusteringFeature().add(newChildEntry.getClusteringFeature());
    }

    /**
     * insert newChildNext to next of child: this.child -next-> ... -next-> child -next-> newChildNext -next-> ...
     * @param child an existing node : if null, it means appending to the tail
     * @param newChildNext a new node
     */
    public void insertAfterWithoutAddingCf(BirchNode child, BirchNode newChildNext) {
        NodeHolder target = findNodeHolder(child);
        if (target == null) {
            addChildWithoutAddingCf(newChildNext);
        } else {
            NodeHolder newNode = newNodeHolder(newChildNext);
            newNode.next = target.next;
            if (target.next != null) {
                target.next.prev = newNode;
            }
            target.next = newNode;
            newNode.prev = target;
            childSize++;
        }
    }

    public NodeHolder findNodeHolder(BirchNode n) {
        if (n == null) {
            return null;
        } else {
            NodeHolder next = child;
            while (next != null && next.node != n) {
                next = next.next;
            }
            if (next != null) {
                return next;
            } else {
                return null;
            }
        }
    }

    public void addChildWithoutAddingCf(BirchNode e) {
        NodeHolder childTail = this.childTail;
        NodeHolder newTail = newNodeHolder(e);
        newTail.prev = childTail;
        if (childTail != null) {
            childTail.next = newTail;
        } else {
            this.child = newTail;
        }
        this.childTail = newTail;
        childSize++;
    }

    /**
     * @param e insert to top: e -next-> child -next-> ...
     */
    public void insertChildWithoutAddingCf(BirchNode e) {
        NodeHolder child = this.child;
        NodeHolder newTop = newNodeHolder(e);
        newTop.next = child;
        if (child != null) {
            child.prev = newTop;
        } else {
            this.childTail = newTop;
        }
        this.child = newTop;
        childSize++;
    }

    /** @return obtains children of the node: generates a list from linked chains
     *            [child, child.next, child.next.next, ...]
     * */
    public List<BirchNode> getChildren() {
        List<BirchNode> es = new ArrayList<>(childSize + 1);
        NodeHolder e = child;
        while (e != null) {
            es.add(e.node);
            e = e.next;
        }
        return es;
    }

    /**
     * @return obtains node holders as a list: [child, child.next, child.next.next, ...]
     */
    public List<NodeHolder> getNodeHolders() {
        List<NodeHolder> ns = new ArrayList<>(childSize + 1);
        NodeHolder e = child;
        while (e != null) {
            ns.add(e);
            e = e.next;
        }
        return ns;
    }


    public void setChildNodeHolder(NodeHolder holder) {
        this.child = holder;
    }

    public void setChildTailNodeHolder(NodeHolder childTail) {
        this.childTail = childTail;
    }

    public NodeHolder getChildNodeHolder() {
         return child;
    }

    /**
     * @param es constructs new holders for ths list: child=es[0] -next-> es[1] -next-> es[2] ...
     */
    public void setChildren(List<BirchNode> es) {
        clusteringFeature.clear();
        NodeHolder prev = null;
        NodeHolder top = null;
        for (BirchNode child : es) {
            clusteringFeature.add(child.getClusteringFeature());
            NodeHolder next = newNodeHolder(child);
            if (prev == null) {
                top = next;
            } else {
                prev.next = next;
                next.prev = prev;
            }
            prev = next;
        }
        this.child = top;
        this.childTail = prev;
        childSize = es.size();
    }

    /**
     * @param es constructs new holders for ths list: child=es[0] -next-> es[1] -next-> es[2] ...
     */
    public void setChildrenWithoutUpdatingCf(List<BirchNode> es) {
        NodeHolder prev = null;
        NodeHolder top = null;
        for (BirchNode child : es) {
            NodeHolder next = newNodeHolder(child);
            if (prev == null) {
                top = next;
            } else {
                prev.next = next;
                next.prev = prev;
            }
            prev = next;
        }
        this.child = top;
        this.childTail = prev;
        childSize = es.size();
    }

    protected NodeHolder newNodeHolder(BirchNode child) {
        child.setParent(this);
        return new NodeHolder(child);
    }

    @Override
    public Iterator<BirchNode> iterator() {
        return new NodeIterator(child);
    }

    public static class NodeIterator implements Iterator<BirchNode> {
        protected NodeHolder target;

        public NodeIterator(NodeHolder target) {
            this.target = target;
        }

        @Override
        public boolean hasNext() {
            return target != null;
        }

        @Override
        public BirchNode next() {
            BirchNode n = target.node;
            target = target.next;
            return n;
        }
    }

    @Override
    public boolean accept(BirchVisitor visitor) {
        if (visitor.visitTree(this)) {
            NodeHolder next = child;
            int i = 0;
            while (next != null) {
                visitor.visitTreeChild(this, i, next);
                if (!next.node.accept(visitor)) {
                    visitor.visitAfterTree(this);
                    return false;
                }
                next = next.next;
                ++i;
            }
            visitor.visitAfterTree(this);
            return true;
        } else {
            return false;
        }
    }

    public static class DebugVisitor extends BirchNode.BirchVisitor {
        protected int depth;
        protected Map<BirchNode,Long> id = new HashMap<>();
        protected long idCount = 0;

        public void run(BirchNode n) {
            println("---------------------->>>");
            n.accept(this);
            println(">>>----------------------");
            println("");
        }

        private void indent() {
            for (int i = 0; i < depth; ++i){
                print("--");
            }
        }
        private void print(String s) {
            System.err.print(s);
        }
        private void println(String s) {
            System.err.println(s);
        }

        @Override
        public boolean visitEntry(BirchNodeCFEntry e) {
            indent();
            println("entry " + e.getIndexes().size() + " <"   + getId(e)+ ">");
            return true;
        }

        @Override
        public boolean visitTree(BirchNodeCFTree t) {
            indent();
            println("node[" + t.getChildSize() + "] <" + getId(t) + ">");
            depth++;
            return true;
        }

        @Override
        public boolean visit(BirchNode node) {
            indent();
            println(node + " "  + getId(node));
            return true;
        }

        private Long getId(BirchNode n) {
            if (!id.containsKey(n)) {
                id.put(n, idCount);
                ++idCount;
            }
            return id.get(n);
        }

        @Override
        public void visitAfterTree(BirchNodeCFTree t) {
            depth--;
        }
    }

    public PutTrace getTrace() {
        return trace;
    }

    public static PutTrace trace = new PutTrace();

    public static class PutTrace {
        public void putStart(BirchNode node, BirchNodeInput input) { }
        public void findClosestChild(BirchNode node, BirchNode selected, int childIndex, double selectedD, BirchNodeInput input) { }
        public void finishWithAddEntry(BirchNode node, BirchNodeInput input) { }
        public void finishWithAddNodeTree(BirchNode node, BirchNode target, BirchNode treeNode, BirchNodeInput input) { }
        public void finishWithAddNodeEntry(BirchNode node, BirchNode target, BirchNodeInput input) { }
        public void split(BirchNode parent, BirchNode newNode, List<BirchNode> left, List<BirchNode> right, BirchNodeInput input) { }
        public void newRoot(BirchNode newRoot, BirchNode left, BirchNode right, BirchNodeInput input) { }
    }

    public static class PutTraceLog extends PutTrace {
        public void log(String fmt, Object... args) {
            System.err.printf("#!!! " + fmt + "\n", args);
        }

        @Override
        public void putStart(BirchNode node, BirchNodeInput input) {
            log("@%,d putStart: sqSum=%f", input.getId(), input.getClusteringFeature().squareSum);
        }

        @Override
        public void findClosestChild(BirchNode node, BirchNode selected, int childIndex, double selectedD, BirchNodeInput input) {
            log("@%,d findClosestChild: child=%d sel=%f", input.getId(), childIndex, selectedD);
        }

        @Override
        public void finishWithAddEntry(BirchNode node, BirchNodeInput input) {
            log("@%,d addEntry", input.getId());
        }

        @Override
        public void finishWithAddNodeTree(BirchNode node, BirchNode target, BirchNode entryNode, BirchNodeInput input) {
            log("@%,d finishWithAddNode", input.getId());
        }

        @Override
        public void finishWithAddNodeEntry(BirchNode node, BirchNode target, BirchNodeInput input) {
            log("@%,d finishWithAddEntryNode", input.getId());
        }

        @Override
        public void split(BirchNode parent, BirchNode newNode, List<BirchNode> left, List<BirchNode> right, BirchNodeInput input) {
            log("@%,d split : left=%d, right=%d", input.getId(), left.size(), right.size());
        }

        @Override
        public void newRoot(BirchNode newRoot, BirchNode left, BirchNode right, BirchNodeInput input) {
            log("@%,d newRoot", input.getId());
        }
    }
}
