package csl.dataproc.actor.visit;

import akka.actor.ActorRef;

/**
 * stepping function generated by {@link csl.dataproc.actor.visit.TraversalMessages.TraversalState#getNextStepTask(Object)}
 * */
public abstract class TraversalStepTask<NodeType, VisitorType> {
    public static boolean DEBUG;
    static {
        DEBUG = System.getProperty("csl.dataproc.actor.visit.debug", "false").equals("true");
        if (DEBUG) {
            System.err.println("traversal-debug: ON");
        }
    }

    public TraversalMessages.TraversalState<NodeType, VisitorType> step(TraversalMessages.TraversalState<NodeType, VisitorType> t) {
        if (DEBUG) {
            System.err.println("traversal [" + this + "] step: " + t);
        }
        try {
            TraversalMessages.TraversalState<NodeType, VisitorType> r = stepBody(t);
            if (DEBUG) {
                System.err.println("traversal [" + this +"]    -> " + r);
            }
            return r;
        } catch (Throwable ex) {
            System.err.println("traversal-error: " + t + " : " + ex);
            ex.printStackTrace();
            t.end(false, true);
            return null;
        }
    }

    /** subclass implements the method for running the step and returning a next state
     * @return next state, or null if finished
     * */
    public abstract TraversalMessages.TraversalState<NodeType, VisitorType> stepBody(TraversalMessages.TraversalState<NodeType, VisitorType> t);


    public static class TraversalStepTaskEnd<NodeType, VisitorType> extends TraversalStepTask<NodeType, VisitorType> {
        public boolean requireRootAfter;

        public TraversalStepTaskEnd(boolean requireRootAfter) {
            this.requireRootAfter = requireRootAfter;
        }

        @Override
        public TraversalMessages.TraversalState<NodeType, VisitorType> stepBody(TraversalMessages.TraversalState<NodeType, VisitorType> t) {
            t.end(requireRootAfter, false);
            return null;
        }

        @Override
        public String toString() {
            return " endTask";
        }
    }

    public static class TraversalStepTaskPop<NodeType, VisitorType> extends TraversalStepTask<NodeType, VisitorType> {
        @Override
        public TraversalMessages.TraversalState<NodeType, VisitorType> stepBody(TraversalMessages.TraversalState<NodeType, VisitorType> t) {
            t.stack.remove(0); //Pop
            if (t.getTopType().equals(TraversalMessages.TraversalEventType.Index)) {
                t.stack.remove(0); //PushIndex
                if (t.getTopType().equals(TraversalMessages.TraversalEventType.Empty)) {
                    t.stack.add(new TraversalMessages.TraversalEventEnd());
                    return t;
                } else if (t.getTopType().equals(TraversalMessages.TraversalEventType.Index)) {
                    return t.nextIndex();
                } else {
                    return t.nextPop();
                }
            } else if (!t.getTopType().equals(TraversalMessages.TraversalEventType.Empty)) {
                t.stack.remove(0); //PushActor
                if (t.getTopType().equals(TraversalMessages.TraversalEventType.Index)) {
                    return t.nextIndex();
                } else {
                    return t.nextPop();
                }
            } else {
                t.stack.add(new TraversalMessages.TraversalEventEnd());
                return t;
            }
        }

        @Override
        public String toString() {
            return " popTask";
        }
    }

    public static class TraversalStepTaskNode<NodeType, VisitorType>  extends TraversalStepTask<NodeType, VisitorType> {
        protected NodeType parent;
        protected NodeType child;
        protected int index;

        public TraversalStepTaskNode(NodeType parent, NodeType child, int index) {
            this.parent = parent;
            this.child = child;
            this.index = index;
        }

        @Override
        public TraversalMessages.TraversalState<NodeType, VisitorType> stepBody(TraversalMessages.TraversalState<NodeType, VisitorType> t) {
            if (child != null && parent != null) {
                if (!t.visitChild(parent, index, child)) {
                    return null;
                }
            }

            if (child == null) {
                return stepNextVisitEnd(t, parent);
            } else {
                TraversalMessages.TraversalNodeType type = t.getType(child);
                switch (type) {
                    case Actor:
                        return stepNextRef(t, t.getActor(child));
                    case Parent:
                        return stepNextDown(t, child);
                    case Leaf:
                        return stepNextSiblingLeaf(t, child);
                    default:
                        return null;
                }
            }
        }

        public TraversalMessages.TraversalState<NodeType,VisitorType> stepNextRef(TraversalMessages.TraversalState<NodeType,VisitorType> t, ActorRef nodeRef) {
            nodeRef.tell(t.next(nodeRef), ActorRef.noSender());
            return null;
        }

        public TraversalMessages.TraversalState<NodeType,VisitorType> stepNextDown(TraversalMessages.TraversalState<NodeType,VisitorType> t, NodeType child) {
            if (!t.visitEnter(child)) {
                return null;
            } else {
                return t.next(0);
            }
        }

        public TraversalMessages.TraversalState<NodeType,VisitorType> stepNextSiblingLeaf(TraversalMessages.TraversalState<NodeType,VisitorType> t, NodeType child) {
            if (!t.visitLeaf(child)) {
                return null;
            } else if (t.getTopType().equals(TraversalMessages.TraversalEventType.Actor)) {
                return t.nextPop();
            } else {
                return t.nextIndex();
            }
        }

        public TraversalMessages.TraversalState<NodeType,VisitorType> stepNextVisitEnd(TraversalMessages.TraversalState<NodeType,VisitorType> t, NodeType parent) {
            if (t.visitExit(parent, t.visitor, false)) {
                return t.nextPop();
            } else {
                return null;
            }
        }

    }
}