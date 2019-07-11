package csl.dataproc.actor.visit;

import akka.actor.*;
import akka.japi.pf.ReceiveBuilder;

import java.util.Arrays;
import java.util.concurrent.CompletableFuture;

/**
 * generic visiting actor
 *
 * <p>
 *  Suppose a Node type and a NodeVisitor type; a Node accepts visiting of a NodeVisitor.
 *  Also suppose that a NodeActor is an Actor, referenced by a NodeRef and holds a Node as internal state.
 *   <pre>
 *       class Node { void accept(NodeVisitor v) {  v.visit(this); } }
 *       class NodeVisitor { void visit(Node n) {...} }
 *       class NodeActor extends AbstractActor { Node state; ...  }
 *       class NodeRef extends Node { ActorRef ref; }
 *   </pre>
 *  <p>
 *  Then, define a custom TraversalState
 *  <pre>
 *      class NodeTravState extends TraversalState&lt;Node,NodeVisitor&gt; {
 *          public NodeTravState() {}
 *          public NodeTravState(NodeVisitor v) { super(v); }
 *          public TraversalStepTask&lt;Node,NodeVisitor&gt; getNextStepTaskNode(Node r) { ... }
 *          public boolean visitEnter(Node n) { return this.visitor.enter(n);  }
 *          public TraversalNodeType getType(Node n) {
 *             if (n instanceof NodeRef) return Actor;
 *             return Parent;
 *          }
 *          public ActorRef getActor(Node n) { return ((NodeRef) n).ref; }
 *      }
 *  </pre>
 *  To implement the state, see {@link csl.dataproc.actor.visit.TraversalMessages.TraversalState}.
 *  <p>
 *   The NodeActor must respond  the TraversalState
 *     and call {@link csl.dataproc.actor.visit.TraversalMessages.TraversalState#accept(Actor, Object)}.
 *   <pre>
 *     class NodeActor extends AbstractActor {
 *          Node state;
 *          public Receive createReceive() {
 *               return receiveBuilder()
 *                   ...
 *                   .match(TraversalMessages.TraversalState.class, this::traverse)
 *                   .build();
 *          }
 *
 *          public void traverse(TraversalState s) {
 *               s.accept(this, state);
 *          }
 *     }
 *   </pre>
 *
 *  <p>
 *  To start a visiting process, you can use the {@link #start(ActorSystem, Class, Object...)} method.
 *  <pre>
 *      CompletableFuture&lt;Visitor&gt; f = TraverseVisitActor.start(system, root, new NodeTravState(new NodeVisitor()));
 *
 *      NodeVisitor v = f.get();
 *  </pre>
 *
 * <p>
 *     You can define a subclass of the visitor actor
 *      for customizing result data types of traversal.
 *     On a remote environment, a visitor held by the actor must be serializable.
 *  <pre>
 *      class MyVisitActor extends TraversalVisitActor&lt;Node,NodeVisitor&gt; {
 *          int data; //custom state
 *          CompletableFuture&lt;Integer&gt; result;
 *             //instead of the default finisher of CompletableFuture&lt;NodeVisitor&gt;
 *
 *          public MyVisitActor(Node root, NodeVisitor v, CompletableFuture&lt;Integer&gt; r) {
 *              super(root, new NodeTravState(v), null);
 *              this.result = r;
 *          }
 *          public void end(TraversalEventEnd e) {
 *              super.end(e); //the actor will die by the call
 *              this.result.complete(data);
 *              System.err.println("finish visiting");
 *          }
 *
 *          //note: the subclass can override createReceive() and use builder() for obtaining ReceiveBuilder
 *          public Receive createReceive() {
 *              return builder()
 *                       .match(Integer.class, this::addData);
 *          }
 *
 *          public void addData(Integer n) {
 *              data += n;
 *          }
 *
 *          //the above custom message will be sent by visitor object.
 *          // To set the actor target of the message, preStart() is suitable
 *          public void preStart() throws Exception {
 *              super.preStart();
 *              initState.visitor.ref = self();
 *          }
 *      }
 *
 *      class NodeVisitor implements Serializable {
 *          ActoRef ref;
 *          void visitLeaf(Node n) { //The custom TraversalState will call the visit method
 *               ref.tell(1, noSender());
 *          }
 *      }
 *
 *      //usage:
 *      CompletableFuture&lt;Integer&gt; r = new CompletableFuture&lt;&gt;();
 *      system.actorOf(MyVisitActor.class, root, visitor, r)
 *         .tell(VfdtMessages.TraversalStart, ActorRef.noSender());
 *         //the call can be replaced by startWithoutFuture(system, MyVisitActor.class, root, visitor, r);
 *      int n = r.get();
 *  </pre>
 */
public class TraversalVisitActor<NodeType, VisitorType> extends AbstractActor {
    protected NodeType root;
    protected TraversalMessages.TraversalState<NodeType, VisitorType> initState;
    protected CompletableFuture<VisitorType> finisher;

    /**
     * it creates an actor of TraversalVisitActor with passing root and initState + a {@link CompletableFuture},
     *   and starting traversing by {@link #start(ActorSystem, Class, Object...)}.
     */
    @SuppressWarnings("unchecked")
    public static <NodeType, VisitorType> CompletableFuture<VisitorType> start(ActorSystem system, NodeType root, TraversalMessages.TraversalState<NodeType, VisitorType> initState) {
        Class cls = TraversalVisitActor.class;
        return start(system, cls, root, initState);
    }

    /** it creates an actor of the actorType from the system, with passing args + a {@link CompletableFuture},
     *   and starting traversing by sending {@link TraversalMessages#TraversalStart} the the actor
     *     from {@link #startWithoutFuture(ActorSystem, Class, Object...)}.
     *   The value of the returned future will be set after the traversing.
     *     So, you can call {@link CompletableFuture#get()} for waiting the finish. */
    public static <NodeType, VisitorType> CompletableFuture<VisitorType> start(ActorSystem system,
                                                                               Class<? extends TraversalVisitActor<NodeType,VisitorType>> actorType,
                                                                               Object... args) {
        CompletableFuture<VisitorType> f = new CompletableFuture<>();
        Object[] argsWithFuture = Arrays.copyOf(args, args.length + 1);
        argsWithFuture[argsWithFuture.length - 1] = f;
        startWithoutFuture(system, actorType, argsWithFuture);
        return f;
    }

    /**
     * it creates an actor of actorType with args, and sends {@link TraversalMessages#TraversalStart} to the actor.
     */
    public static void startWithoutFuture(ActorSystem system, Class<? extends TraversalVisitActor> actorType, Object... args) {
        ActorRef ref = system.actorOf(Props.create(actorType, args));
        ref.tell(TraversalMessages.TraversalStart, ActorRef.noSender());
    }

    public TraversalVisitActor(NodeType root, TraversalMessages.TraversalState<NodeType, VisitorType> initState, CompletableFuture<VisitorType> finisher) {
        this.root = root;
        this.initState = initState;
        this.finisher = finisher;
    }

    @Override
    public Receive createReceive() {
        return builder()
                .build();
    }

    /** the subclass of the actor can call the method for overriding {@link #createReceive()} */
    protected ReceiveBuilder builder() {
        return receiveBuilder()
                .matchEquals(TraversalMessages.TraversalStart, this::traverseStart)
                .match(TraversalMessages.TraversalState.class, this::traverse)
                .match(TraversalMessages.TraversalEventEnd.class, this::end);
    }

    public void traverseStart(Object o) {
        traverse(initState.withFinisher(self()));
    }

    public void traverse(TraversalMessages.TraversalState<NodeType, VisitorType> t) {
        TraversalStepTask<NodeType,VisitorType> step = t.getNextStepTask(root);
        t = step.step(t);
        if (t != null) {
            ActorRef ref = t.topRef();
            if (ref != null) {
                ref.tell(t, self());
            } else {
                self().tell(t, self());
            }
        }
    }

    @SuppressWarnings("unchecked")
    public void end(TraversalMessages.TraversalEventEnd<VisitorType> e) {
        if (e.requireRootAfter) {
            initState.visitExit(root, e.visitor, e.error);
        }
        if (finisher != null) {
            finisher.complete(e.visitor);
        }
        endStop();
    }
    public void endStop() {
        self().tell(PoisonPill.getInstance(), ActorRef.noSender());
    }
}
