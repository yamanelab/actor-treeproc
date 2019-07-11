package csl.dataproc.actor.visit;

import akka.actor.Actor;
import akka.actor.ActorRef;
import akka.actor.ActorSystem;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class TraversalMessages {

    public static String TraversalStart = "start";

    public static List<Class<?>> getTypes() {
        return Arrays.asList(TraversalMessages.class.getClasses());
    }

    /**
     * describe a step of tree-visiting:
     *  the tree is constructed by both regular node-objects and node-actors.
     *  This state object is a message and passed by {@link TraversalVisitActor} with serialization.
     * <ul>
     *   <li>stack: a path of the visiting. the first element is a stack top.
     *      it does not directly refer a node-object because of serialization.
     *      it consists of a reverse list of {@link TraversalEvent}:
     *       <ul>
     *           <li>ref(ActorRef) {@link TraversalEventPushActor} :
     *                requesting the root({@link TraversalVisitActor}) or
     *                entering an actor node</li>
     *           <li>i(int) {@link TraversalEventPushIndex}: entering a child node with its index</li>
     *           <li>pop {@link TraversalEventPop}: leaving the node of the rest of the stack</li>
     *           <li>end(visitor,reqRoot,error) {@link TraversalEventEnd}: end of visiting.
     *                if reqRoot, it needs to explicitly do a leaving process.
     *                if error, the visiting process unexpectedly stopped.</li>
     *       </ul>
     *       examples:
     *       <pre>
     *           class Node { }
     *           class NodeObj extends Node {
     *               List&lt;Node&gt; children;
     *           }
     *           class NodeRef extends Node {
     *               ActorRef nodeActor;
     *           }
     *           class NodeActor extends AbstractActor { //the actor must respond to TraversalState
     *               Node state;
     *               public Receive createReceive() {
     *                  return receiveBuilder()
     *                      ...
     *                      .match(TraversalMessages.TraversalState.class, this::traverse)
     *                      .build();
     *               }
     *
     *               public void traverse(TraversalState s) {
     *                   s.accept(this, state);
     *               }
     *           }
     *           class NodeVisitor implements Serializable {
     *               void visitObj(NodeObj n) {}
     *               void visitLeaf(Node n) {}
     *           }
     *
     *           class NodeTravState extends TraversalState&lt;Node,NodeVisitor&gt; {
     *               ... //described later
     *           }
     *       </pre>
     *       <pre>
     *           NodeObj root = new NodeObj();
     *           NodeObj c0 = new NodeObj();
     *           root.children.add(c0);
     *           NodeObj n0 = new NodeObj();
     *           NodeObj n0_0 = new NodeObj();
     *           NodeObj n0_1 = new NodeObj();
     *           c0.children.add(new NodeRef(system.actorOf(Props.create(NodeActor.class, n0))));
     *           n0.children.add(n0_0);
     *           n0.children.add(n0_1);
     *
     *           CompletableFuture&lt;NodeVisitor&gt; f = new CompletableFuture&gt;&lt;();
     *           ActorRef visitActor = system.actorOf(Props.create(TraversalVisitActor.class,
     *                             root,
     *                             new TraversalState&lt;Node,NodeVisitor&gt;(),
     *                             f));
     *           visitActor.tell(TraversalStart, noSender());
     *
     *           NodeVisitor v = f.get(); //wait for completion
     *           ... //obtains results from v
     *       </pre>
     *        <ul>
     *            <li> [i(0), i(0), ref(visitActor)] : the visitActor enters the child 0 of the root node (0 = c0).
     *                   in this case, the root directly holds child node (non-actor) object </li>
     *            <li> =&gt;      [i(0), ref(n0), i(0), i(0), ref(visitActor)] : enters the child 0 of the child 0 of the root (0 = NodeRef of n0).
     *                  and the child is an actor node (NodeRef(n0)) and the actor has the actual child node 0 (n0) </li>
     *            <li> =&gt; [pop, i(0), ref(n0), i(0), i(0), ref(visitActor)] : after visiting the child 0, it leaves the node</li>
     *            <li> =&gt;      [i(1), ref(n0), i(0), i(0), ref(visitActor)] : next, it tries to enter a sibling node (0+1)</li>
     *            <li> =&gt;      [pop , ref(n0), i(0), i(0), ref(visitActor)] : after visiting all child nodes, it leaves the parent node</li>
     *        </ul>
     *        actually, you can use {@link TraversalVisitActor#start(ActorSystem, Class, Object...)} for starting the visiting.
     *   </li>
     *   <li>visitor: a visitor object. it must be Serializable</li>
     *   <li>finisher: optional. the actor referred by the ActorRef will receive {@link TraversalEventEnd}</li>
     * </ul>
     *
     * How to implements your own state? :
     * <ul>
     *     <li> implement {@link #getNextStepTaskNode(Object)} by using
     *        {@link #getNonActorIndexSubPath()} and {@link #getNextStepTaskNode(Object, Object, int)}. </li>
     *     <li> implement {@link #getType(Object)} for classifying a node. </li>
     *     <li> implement {@link #visitEnter(Object)} for running actual visiting process. </li>
     *     <li> implement {@link #getActor(Object)} for extracting a reference of an actor node. </li>
     *     <li> also you can implement {@link #visitLeaf(Object)}, {@link #visitChild(Object, int, Object)},
     *             and {@link #visitExit(Object, Object, boolean)} </li>
     * </ul>
     * example:
     * <pre>
     *      //usually, a visitor is customized by subclassing. so you can specify the type of the visitor by a type arg.
     *     class NodeTravState&lt;V extends NodeVisitor&gt; extends TraversalState&lt;Node,V&gt; {
     *         public NodeTravState() { } //serializable default constructor
     *         public NodeTravState(V v) { super(v); }
     *
     *         //obtains task with a node followed by a index sub-path of the parent.
     *           //those indexes are children of local (non-actor) nodes
     *         public TraversalStepTask&lt;Node,V&gt; getNextStepTaskNode(Node parent) {
     *              List&lt;Integer&gt; idx = getNonActorIndexSubPath(); //child indexes obtained from the stack, top to bottom
     *              Node child = parent;
     *              parent = null;
     *              int index = 0;
     *              if (child != null) {
     *                  for (int i : idx) {
     *                      index = i;
     *                      parent = child;
     *                      if (parent instanceof NodeObj) {
     *                          List&lt;Node&gt; l = ((NodeObj) parent).children;
     *                          if (i &lt; l.size()) {
     *                              child = l.get(i);
     *                          } else {
     *                              return getNextStepTask(parent, null, index);
     *                          }
     *                      }
     *                  }
     *              }
     *              return getNextStepTask(parent, child, index);
     *         }
     *
     *         //each node is classified by 3 types: Actor, Parent and Leaf
     *         public TraversalMessages.TraversalNodeType getType(Node node) {
     *              if (node instanceof NodeRef) {
     *                  return TraversalMessages.TraversalNodeType.Actor;
     *              } else if (node instanceof NodeObj) {
     *                  return TraversalMessages.TraversalNodeType.Parent;
     *              } else {
     *                  return TraversalMessages.TraversalNodeType.Leaf;
     *              }
     *          }
     *
     *          //for Actor nodes
     *          public ActorRef getActor(Node node) {
     *              return ((NodeRef) node).nodeActor;
     *          }
     *
     *          //for Parent nodes
     *          public boolean visitEnter(Node node) {
     *              if (node instanceof NodeObj) {
     *                  this.visitor.visitObj((NodeObj) node); //call the visitor method
     *              } else { //by default, also visitLeaf calls the method for Leaf nodes
     *                  this.visitor.visitLeaf(node);
     *              }
     *              return true; //you can also return false for stopping the process
     *          }
     *     }
     * </pre>
     */
    public static abstract class TraversalState<NodeType, VisitorType> implements Serializable, Cloneable {
        public List<TraversalEvent> stack;
        public VisitorType visitor;
        public ActorRef finisher;

        public TraversalState() {
            stack = new ArrayList<>();
        }

        public TraversalState(VisitorType visitor) {
            this.visitor = visitor;
            stack = new ArrayList<>();
        }

        public TraversalState<NodeType, VisitorType> withFinisher(ActorRef finisher) {
            TraversalState<NodeType, VisitorType> t = next(finisher);
            t.finisher = finisher;
            return t;
        }

        public TraversalState<NodeType, VisitorType> next(ActorRef ref) {
            TraversalState<NodeType, VisitorType> t = create();
            t.stack.add(new TraversalEventPushActor(ref));
            t.stack.addAll(stack);
            return t;
        }
        public TraversalState<NodeType, VisitorType> next(int index) {
            TraversalState<NodeType, VisitorType> t = create();
            t.stack.add(new TraversalEventPushIndex(index));
            t.stack.addAll(stack);
            return t;
        }

        public TraversalState<NodeType, VisitorType> nextPop() {
            TraversalState<NodeType, VisitorType> t = create();
            t.stack.add(new TraversalEventPop());
            t.stack.addAll(stack);
            return t;
        }
        @SuppressWarnings("unchecked")
        protected TraversalState<NodeType, VisitorType> create() {
            try {
                TraversalState<NodeType, VisitorType> t = (TraversalState<NodeType, VisitorType>) super.clone();
                t.stack = new ArrayList<>(stack.size());
                return t;
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }

        public TraversalState<NodeType, VisitorType> nextIndex() {
            ((TraversalEventPushIndex) stack.get(0)).index++;
            return this;
        }

        public TraversalEventType getTopType() {
            return stack.isEmpty() ? TraversalEventType.Empty : stack.get(0).getType();
        }

        public ActorRef topRef() {
            for (TraversalEvent e : stack) {
                if (e instanceof TraversalEventPushActor) {
                    return ((TraversalEventPushActor) e).ref;
                }
            }
            return null;
        }

        /** upper node to lower node */
        public List<Integer> getNonActorIndexSubPath() {
            List<Integer> idx = new ArrayList<>();
            for (TraversalMessages.TraversalEvent e : stack) {
                if (e instanceof TraversalMessages.TraversalEventPushIndex) {
                    idx.add(((TraversalMessages.TraversalEventPushIndex) e).index);
                } else if (!e.getType().equals(TraversalEventType.Pop)) { //skip pop, stop at actor
                    break;
                }
            }
            Collections.reverse(idx);
            return idx;
        }

        public void end(boolean requireRootAfter, boolean error) {
            if (finisher != null) {
                finisher.tell(new TraversalEventEnd<>(visitor, requireRootAfter, error), ActorRef.noSender());
            }
        }

        @Override
        public String toString() {
            return getClass().getSimpleName() + "(visitor=" + this.visitor + ", stack=" + stack + ", finisher=" + finisher + ")";
        }

        public TraversalStepTask<NodeType, VisitorType> getNextStepTask(NodeType root) {
            switch (getTopType()) {
                case Actor:
                case Index:
                case Empty:
                    return getNextStepTaskNode(root);
                case Pop:
                    return getNextStepTaskPop();
                case End:
                default:
                    return getNextStepTaskEnd(root == null);
            }
        }

        public TraversalStepTask<NodeType, VisitorType> getNextStepTaskPop() {
            return new TraversalStepTask.TraversalStepTaskPop<>();
        }

        public TraversalStepTask<NodeType, VisitorType> getNextStepTaskEnd(boolean requireRootAfter) {
            return new TraversalStepTask.TraversalStepTaskEnd<>(requireRootAfter);
        }

        /** obtains a target node task from the root with (non-actor) index path ({@link #getNonActorIndexSubPath()}.
         *   each index of the path might be over the size of children,
         *     then it can break and specify null as child.
         *      The null child will cause {@link #visitExit(Object, Object, boolean)} and {@link #nextPop()}.
         *   example:
         *  <pre>
         *      Node n = root;
         *      Node p = null;
         *      int  ni = 0;
         *      for (int i : getNonActorIndexSubPath()) {
         *          p = n;
         *          ni = i;
         *          List&lt;Node&gt; cs = ((NodeObj) n).children;
         *          if (i &lt; cs.size()) {
         *              n = cs.get(i);
         *          } else {
         *              n = null;
         *              break;
         *          }
         *      }
         *      return getNextStepTaskNode(p, n, ni);
         *  </pre>
         * */
        public abstract TraversalStepTask<NodeType, VisitorType> getNextStepTaskNode(NodeType parent);

        public TraversalStepTask<NodeType, VisitorType> getNextStepTaskNode(NodeType parent, NodeType child, int index) {
            return new TraversalStepTask.TraversalStepTaskNode<>(parent, child, index);
        }

        /**
         * a node actor calls the method by receiving TraversalState
         *  <pre>
         *   class NodeActor extends AbstractActor {
         *     Node state;
         *     public Receive createReceive() {
         *        ...
         *        .mach(TraversalState.class, this::traverse)
         *        ...
         *     }
         *     ...
         *     void traverse(TraversalState s) { s.accept(this, state); }
         *   }
         *  </pre>
         * @param actor the receiver
         * @param actorNode the node object held by the actor
         */
        @SuppressWarnings("unchecked")
        public void accept(Actor actor, NodeType actorNode) {
            if (getTopType().equals(TraversalEventType.Actor) && visitor instanceof ActorVisitor<?>) {
                ((ActorVisitor<Actor>) visitor).visitActor(actor);
            }
            TraversalStepTask<NodeType,VisitorType> step = getNextStepTask(actorNode);
            TraversalState<NodeType, VisitorType> nextState = step.step(this);
            if (nextState != null) {
                ActorRef targetRef = nextState.topRef();
                ActorRef selfRef = actor.self();
                if (targetRef != null) {
                    targetRef.tell(nextState, selfRef);
                } else {
                    traverseStepOnSystem(actorNode, actor.context().system());
                }
            }
        }

        public void traverseStepOnSystem(NodeType root, ActorSystem system) {
            system.dispatcher().execute(() -> {
                TraversalState<NodeType,VisitorType> nextState = getNextStepTask(root).step(this);
                if (nextState != null) {
                    nextState.traverseStepOnSystem(root, system);
                }
            });
        }

        /** call entering method to the visitor with child and return true if it continues visiting */
        public abstract boolean visitEnter(NodeType child);

        /** call intermediate entering method to the visitor with parent, child and the child index,
         *   and return true if it continues visiting.
         *   both parent and child are non-null */
        public boolean visitChild(NodeType parent, int index, NodeType child) {
            return true;
        }

        /** call entering method to the visitor with the leaf child and return true if it continues visiting */
        public boolean visitLeaf(NodeType child) {
            return visitEnter(child);
        }

        /** cal exiting method to the visitor with the parent node. the parent might be null.
         *   the method might be call with an initial state with another visitor instance as the final step. */
        public boolean visitExit(NodeType parent, VisitorType visitor, boolean error) {
            return true;
        }

        /** return the type of the node:
         * <ul>
         *   <li>Actor: a reference node to an actor: {@link #getActor(Object)} </li>
         *   <li>Parent: a parent node which might contain children: {@link #visitEnter(Object)} </li>
         *   <li>Leaf: a leaf node: {@link #visitLeaf(Object)}</li>
         * </ul>
         * */
        public abstract TraversalNodeType getType(NodeType node);

        public abstract ActorRef getActor(NodeType refNode);
    }

    public enum TraversalNodeType {
        Actor, Parent, Leaf
    }

    public enum TraversalEventType {
        Empty, End, Index, Actor, Pop
    }

    public static class TraversalEvent implements Serializable {
        public TraversalEventType getType() {
            return TraversalEventType.Empty;
        }
    }

    public static class TraversalEventPushActor extends TraversalEvent {
        public ActorRef ref;

        public TraversalEventPushActor() { }

        public TraversalEventPushActor(ActorRef ref) {
            this.ref = ref;
        }

        @Override
        public TraversalEventType getType() {
            return TraversalEventType.Actor;
        }

        @Override
        public String toString() {
            return "ref(" + ref + ")";
        }
    }

    public static class TraversalEventPushIndex extends TraversalEvent {
        public int index;

        public TraversalEventPushIndex() { }

        public TraversalEventPushIndex(int index) {
            this.index = index;
        }

        @Override
        public TraversalEventType getType() {
            return TraversalEventType.Index;
        }

        @Override
        public String toString() {
            return "i(" + index + ")";
        }
    }

    public static class TraversalEventPop extends TraversalEvent {

        @Override
        public TraversalEventType getType() {
            return TraversalEventType.Pop;
        }

        @Override
        public String toString() {
            return "pop";
        }
    }

    public static class TraversalEventEnd<VisitorType> extends TraversalEvent {
        public VisitorType visitor;
        public boolean requireRootAfter;
        public boolean error;

        public TraversalEventEnd() { }

        public TraversalEventEnd(VisitorType visitor, boolean requireRootAfter, boolean error) {
            this.visitor = visitor;
            this.requireRootAfter = requireRootAfter;
            this.error = error;
        }

        @Override
        public TraversalEventType getType() {
            return TraversalEventType.End;
        }

        @Override
        public String toString() {
            return "end(" + visitor +
                    ", requireRootAfter=" + requireRootAfter + (error ? ", ERROR" : "") + ")";
        }
    }
}
