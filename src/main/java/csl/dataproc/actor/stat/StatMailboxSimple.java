package csl.dataproc.actor.stat;

import akka.actor.ActorSystem;
import com.typesafe.config.Config;

public class StatMailboxSimple extends StatMailbox {
    public StatMailboxSimple() {
        log("create queue-simple: " + this);
    }

    public StatMailboxSimple(ActorSystem.Settings settings, Config config) {
        this();
    }

    @Override
    public StatQueue createQueue(long id) {
        return new StatQueueControlAware(id, statSystem);
    }
}
