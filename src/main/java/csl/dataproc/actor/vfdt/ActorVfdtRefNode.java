package csl.dataproc.actor.vfdt;

import akka.actor.ActorRef;
import akka.pattern.PatternsCS;
import akka.util.Timeout;
import csl.dataproc.vfdt.SparseDataLine;
import csl.dataproc.vfdt.VfdtNode;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class ActorVfdtRefNode implements VfdtNode {
    protected ActorRef ref;
    protected int depth;

    public ActorVfdtRefNode() {}

    public ActorVfdtRefNode(ActorRef ref, int depth) {
        this.ref = ref;
        this.depth = depth;
    }

    @Override
    public int getDepth() {
        return depth;
    }

    public static AtomicInteger blockingAsk = new AtomicInteger();

    public <R> R ask(Object msg, Class<R> type) {
        return ask(ref, msg, type);
    }
    public <R> R ask(Object msg, Class<R> type, Timeout timeout) {
        return ask(ref, msg, type, timeout);
    }

    public ActorRef getRef() {
        return ref;
    }

    public static <R> R ask(ActorRef ref, Object msg, Class<R> type) {
        return ask(ref, msg, type, new Timeout(5, TimeUnit.MINUTES));
    }

    /**
     * a client utility for ask pattern:
     * <pre>
     *     T t = ask(ref, msg, T.class, timeout);
     *
     *     //it needs a receive handler sending a result value of T like the following:
     *     receiveBuilder()
     *          .match(MsgType.class, msg -&gt; sender().tell(retVal, self()));
     * </pre>
     */
    public static <R> R ask(ActorRef ref, Object msg, Class<R> type, Timeout timeout) {
        try {
            blockingAsk.incrementAndGet();
            return type.cast(PatternsCS.ask(ref, msg, timeout).toCompletableFuture().get());
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        } finally {
            blockingAsk.decrementAndGet();
        }
    }

    @Override
    public long getInstanceCount() {
        return ask(new VfdtMessages.GetInstanceCount(), Long.class);
    }

    @Override
    public VfdtNode put(DataLine dataLine) {
        if (dataLine instanceof ActorVfdtDataLine) {
            ref.tell(new VfdtMessages.PutDataLine((ActorVfdtDataLine) dataLine), ActorRef.noSender());
        } else {
            ref.tell(new VfdtMessages.PutDataLine((new ActorVfdtDataLine(dataLine, null))), ActorRef.noSender());
        }
        return this;
    }

    @Override
    public VfdtNode putArrayLine(ArrayDataLine arrayDataLine) {
        ref.tell(new VfdtMessages.PutDataLine(new ActorVfdtDataLine(arrayDataLine, null)), ActorRef.noSender());
        return this;
    }

    @Override
    public VfdtNode putSparseLine(SparseDataLine sparseDataLine) {
        ref.tell(new VfdtMessages.PutDataLine(new ActorVfdtDataLine(sparseDataLine, null)), ActorRef.noSender());
        return this;
    }

    @Override
    public boolean accept(NodeVisitor nodeVisitor) {
        return ask(new VfdtMessages.Accept(nodeVisitor), Boolean.class, visitorWait());
    }

    public static Timeout visitorWait() {
        return new Timeout(5, TimeUnit.HOURS);
    }


    @Override
    public VfdtNode stepClassify(DataLine line) {
        if (line instanceof ActorVfdtDataLine) {
            ref.tell(new VfdtMessages.StepClassify((ActorVfdtDataLine) line), ActorRef.noSender());
        } else {
            ref.tell(new VfdtMessages.StepClassify(new ActorVfdtDataLine(line, ActorRef.noSender())), ActorRef.noSender());
        }
        return this;
    }

    public void end() {
        ref.tell(new VfdtMessages.End(), ActorRef.noSender());
    }

    public long getId() {
        return ask(new VfdtMessages.GetId(), Long.class);
    }

    @Override
    public String toString() {
        return "RefNode(" + ref + ")";
    }
}
