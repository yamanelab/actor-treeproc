package csl.dataproc.actor.vfdt;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import csl.dataproc.actor.stat.StatSystem;
import csl.dataproc.actor.stat.StatVm;
import csl.dataproc.actor.visit.ActorVisitor;
import csl.dataproc.actor.visit.TraversalMessages;
import csl.dataproc.actor.visit.TraversalVisitActor;
import csl.dataproc.dot.DotAttribute;
import csl.dataproc.dot.DotGraph;
import csl.dataproc.dot.DotNode;
import csl.dataproc.vfdt.VfdtNode;
import csl.dataproc.vfdt.VfdtNodeSplit;
import csl.dataproc.vfdt.VfdtVisitorDot;

import java.awt.*;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * <pre>
 *     ActorVfdtVisitorDot
 *           a=DotActor(host = this)
 *          v=DotTraversalVisitor(ref -&gt; a)
 *          root.accept(v)
 *             ActorVfdtRefNode
 *                ref -----------&gt; ActorVfdtNodeActor
 *             -&gt; accept(v)
 *                   -&gt;  ref.tell(Accept(v)) //v might be copied for remoting
 *                      -----&gt;  accept(Accept(v))
 *                                -&gt; state.accept(v)
 *                                  -&gt; v.visit...(state)
 *                                        DotTraversalVisitor.visit...
 *                                        -&gt;  add(label, ...)
 *             a &lt;-------- v.ref.tell(Add(label, ...)) --
 *             a.receive(Add(label,...))
 *         &lt;--- host.add(label, ...) -
 *                                     v.visitAfterSplit(state)
 *             a &lt;-------- v.ref.tell(Pop())
 *             a.receive(Pop())
 *         &lt;--- host.visitAfterSplit(null) -
 * </pre>
 */
public class ActorVfdtVisitorDot extends VfdtVisitorDot {
    protected Path dotFile;
    public CompletableFuture<DotGraph> graphResult;
    protected StatVm.GraphImageSetter imageSetter;

    protected Map<Long,int[]> systemIdToColor = new HashMap<>();

    public DotGraph run(ActorSystem system, VfdtNode root, Path dotFile) {
        try {
            init(system, dotFile);
            TraversalVisitActor.startWithoutFuture(system, DotActor.class, this, root);
            return result(system);
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    protected void init(ActorSystem system, Path dotFile) {
        this.dotFile = dotFile;
        imageSetter = new StatVm.GraphImageSetter(system, dotFile);
        initGraph();
        graphResult = new CompletableFuture<>();
    }

    protected DotGraph result(ActorSystem system) {
        try {
            DotGraph g = graphResult.get();
            imageSetter.saveVms(g);
            return g;
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    public DotNode add(int childIndex, long id, String label, boolean push, boolean box) {
        this.childIndex = childIndex;
        return add(id, label, push, box);
    }
    public DotNode add(long id, String label, boolean push, boolean box) {
        DotNode dn = this.graph.add(label);
        if (!this.stack.isEmpty()) {
            this.graph.addEdge(this.stack.peek(), dn).getAttrs().add(DotAttribute.labelQuote("" + this.childIndex));
        }

        if (!box) {
            dn.getAttrs().add(DotAttribute.styleRounded());
        }
        dn.getAttrs().add(DotAttribute.shapeBox());

        long sid = StatSystem.system().getSystemId(id);
        int[] rgb = systemIdToColor.computeIfAbsent(sid, s -> nextRgb());
        dn.getAttrs().add(DotAttribute.colorRGB(rgb[0], rgb[1], rgb[2]));

        if (push) {
            this.stack.push(dn);
        }
        imageSetter.setNodeImage(dn, id, false);
        return dn;
    }

    protected int[] nextRgb() {
        int max = 128;
        int n = systemIdToColor.size();

        double h = (n % 2 == 0 ? ((n + max / 2) % max) : n) / (double) max;
        Color c = Color.getHSBColor((float) h, 0.8f, 0.6f);
        return new int[] {c.getRed(), c.getGreen(), c.getBlue()};
    }

    public static class DotActor extends TraversalVisitActor<VfdtNode,DotTraversalVisitor> {
        protected ActorVfdtVisitorDot host;

        public DotActor(ActorVfdtVisitorDot host, VfdtNode root) {
            this(host, root, new DotTraversalVisitor());
        }

        public DotActor(ActorVfdtVisitorDot host, VfdtNode root, DotTraversalVisitor visitor) {
            super(root, new VfdtMessages.VfdtTraversalState<>(visitor), null);
            this.host = host;
        }

        @Override
        public void preStart() throws Exception {
            super.preStart();
            initState.visitor.ref = self();
        }

        @Override
        public Receive createReceive() {
            return builder()
                    .match(VfdtMessages.Add.class, this::add)
                    .match(VfdtMessages.Pop.class, this::pop)
                    .match(String.class, this::ask)
                    .build();
        }

        public void add(VfdtMessages.Add m) {
            System.err.println("dot id=" + m.id);
            host.add(m.childIndex, m.id, m.label, m.push, m.box);
        }

        public void pop(VfdtMessages.Pop m) {
            host.visitAfterSplit(null);
        }

        public void ask(String s) {
            sender().tell(s, self());
        }

        @Override
        public void end(TraversalMessages.TraversalEventEnd<DotTraversalVisitor> e) {
            super.end(e);
            try {
                host.graphResult.complete(host.graph);
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }


    }

    public static class DotTraversalVisitor extends VfdtVisitorDot implements Serializable, ActorVisitor<ActorVfdtNodeActor> {
        protected ActorRef ref;
        protected long lastId = -1;

        public DotTraversalVisitor() {}

        public DotTraversalVisitor(ActorRef ref) {
            this.ref = ref;
        }

        @Override
        protected void add(String label, boolean push, boolean box) {
            ref.tell(new VfdtMessages.Add(lastId, "id=" + lastId + "," + label, push, box, childIndex), ActorRef.noSender());
        }

        @Override
        public void visitAfterSplit(VfdtNodeSplit node) {
            ref.tell(new VfdtMessages.Pop(), ActorRef.noSender());
        }

        private void writeObject(ObjectOutputStream out) throws IOException {
            out.writeInt(this.childIndex);
            out.writeLong(lastId);
            out.writeObject(ref);
        }
        private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
            childIndex = in.readInt();
            lastId = in.readLong();
            ref = (ActorRef) in.readObject();
        }

        @Override
        public void visitActor(ActorVfdtNodeActor actor) {
            lastId = actor.getId();
        }

        @Override
        public String toString() {
            return "DotTraversalVisitor{" +
                    "ref=" + ref +
                    ", lastId=" + lastId +
                    '}';
        }
    }

}
