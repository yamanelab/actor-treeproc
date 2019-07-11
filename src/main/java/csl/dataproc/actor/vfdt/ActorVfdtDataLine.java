package csl.dataproc.actor.vfdt;

import akka.actor.ActorRef;
import csl.dataproc.vfdt.VfdtNode;

import java.io.Serializable;

public class ActorVfdtDataLine implements VfdtNode.DataLine, Serializable {
    protected VfdtNode.DataLine line;
    protected ActorRef target;

    public ActorVfdtDataLine() {}

    public ActorVfdtDataLine(VfdtNode.DataLine line, ActorRef target) {
        this.line = line;
        this.target = target;
    }

    public VfdtNode.DataLine getLine() {
        return line;
    }

    public ActorRef getTarget() {
        return target;
    }

    @Override
    public long getLong(int i) {
        return line.getLong(i);
    }

    @Override
    public double getDouble(int i) {
        return line.getDouble(i);
    }
}
