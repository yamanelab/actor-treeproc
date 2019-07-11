package csl.dataproc.actor.visit;

import akka.actor.Actor;

import java.io.Serializable;

/**
 * An interface for visiting the actor instance.
 * Usually a type implementing the ActorVisitor becomes VisitorType of {@link TraversalVisitActor}
 * <pre>
 *     class NodeVisitor implements ActorVisitor&gt;NodeActor&gt; {
 *         public void visitLeaf(Node n) { //for regular(non-actor) node
 *             ...
 *         }
 *
 *         public void visitActor(NodeActor a) { //for actor instance
 *             ...
 *         }
 *     }
 * </pre>
 */
public interface ActorVisitor<NodeActor extends Actor> extends Serializable {
    void visitActor(NodeActor actor);
}
