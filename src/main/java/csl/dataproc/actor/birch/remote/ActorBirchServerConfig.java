package csl.dataproc.actor.birch.remote;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import csl.dataproc.actor.birch.ActorBirchConfig;

public class ActorBirchServerConfig extends ActorBirchConfig {

    public ActorBirchServerConfig() { }

    public ActorBirchServerConfig(ActorSystem system) {
        super(system);
    }

    @Override
    public ActorRef getFinishActor() {
        //TODO
        return super.getFinishActor();
    }
}
