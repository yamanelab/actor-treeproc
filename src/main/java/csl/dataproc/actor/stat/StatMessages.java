package csl.dataproc.actor.stat;

import akka.actor.ActorRef;

import java.io.Serializable;

public class StatMessages {

    public static class ActorId implements Serializable {
        public ActorRef target;
        public long id;
        public transient StatMailbox.StatQueue queue;

        public ActorId() {}

        public ActorId(ActorRef target, long id, StatMailbox.StatQueue queue) {
            this.target = target;
            this.id = id;
            this.queue = queue;
        }
    }
}
