package csl.dataproc.actor.remote;

import akka.actor.Actor;
import akka.actor.ActorRef;
import akka.actor.Props;
import akka.dispatch.ControlMessage;
import akka.japi.Creator;
import csl.dataproc.actor.stat.StatSystem;

import java.io.Serializable;
import java.time.Instant;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class RemoteManagerMessages {

    public static class ManagerEvent implements Serializable {
        public String address;
    }

    public static class Join extends ManagerEvent {
        public boolean fromAddress;
        public Join() {}

        public Join(String address, boolean fromAddress) {
            this.address = address;
            this.fromAddress = fromAddress;
        }

        @Override
        public String toString() {
            return "Join(" + address + (fromAddress ? "!" : "") + ")";
        }
    }

    public static class Leave extends ManagerEvent {
        public Leave() {}

        public Leave(String address) {
            this.address = address;
        }

        @Override
        public String toString() {
            return "Leave(" + address + ")";
        }
    }

    public static class Shutdown implements Serializable {}

    public static class Creation<T extends Actor> implements Serializable, Creator<T>, ControlMessage {
        public Class<T> actorClass;
        public Creator<T> creator;
        public String from;
        public long id;
        public boolean reset;
        public Instant startTime;
        public int forwardedCount;

        public Creation() {}

        public Creation(Class<T> actorClass, Creator<T> creator, String from, long id, boolean reset, int forwardedCount) {
            this.actorClass = actorClass;
            this.creator = creator;
            this.from = from;
            this.id = id;
            this.reset = reset;
            this.startTime = Instant.now();
            this.forwardedCount = forwardedCount;
        }

        @Override
        public T create() throws Exception {
            return creator.create();
        }

        public Props props() {
            Props p = Props.create(actorClass, creator);
            if (StatSystem.system().isEnabled()) {
                p = p.withMailbox(RemoteConfig.CONF_STAT_MAILBOX);
            } else {
                p = p.withMailbox(RemoteConfig.CONF_CONTROL_MAILBOX);
            }
            if (creator instanceof DispatcherSpecifiedCreator) {
                p = p.withDispatcher(((DispatcherSpecifiedCreator) creator).getSpecifiedDispatcher());
                System.err.println("props: " + actorClass + " : " + p.dispatcher());
            }
            return p;
        }
    }

    public interface DispatcherSpecifiedCreator {
        /** returns a name of a dispatcher which will be passed to {@link Props#withDispatcher(String)} */
        String getSpecifiedDispatcher();
    }

    public static class Created implements Serializable, ControlMessage {
        public String actor;
        public long id;
        public Instant startTime;
        public int forwardedCount;

        public Created() {}

        public Created(String actor, long id, Instant startTime, int forwardedCount) {
            this.actor = actor;
            this.id = id;
            this.startTime = startTime;
            this.forwardedCount = forwardedCount;
        }
    }

    public static class Test implements Serializable {
        public String message;
        public boolean back;

        public Test() {}

        public Test(String message, boolean back) {
            this.message = message;
            this.back = back;
        }
    }

    public static class SetConfig implements Serializable {
        public RemoteManager.SharedConfig config;

        public SetConfig() { }

        public SetConfig(RemoteManager.SharedConfig config) {
            this.config = config;
        }
    }

    public static class GetStatusAll implements Serializable {} //List<Status>

    public static class GetStatus implements Serializable { }

    public static class Status implements Serializable {
        public String address;
        public long count;
        public long createdCount; //number of calls of waitCreation: includes remote-actors
        public long createdLocalCount; //locally created actors
        public long freeMemory;
        public long totalMemory;

        public Status() {}

        public Status(String address, long count, long createdCount, long createdLocalCount,
                      long freeMemory, long totalMemory) {
            this.address = address;
            this.count = count;
            this.createdCount = createdCount;
            this.createdLocalCount = createdLocalCount;
            this.freeMemory = freeMemory;
            this.totalMemory = totalMemory;
        }

        public Map<String,Object> toJson() {
            LinkedHashMap<String,Object> map = new LinkedHashMap<>();
            map.put("address", address);
            map.put("count", count);
            map.put("createdCount", createdCount);
            map.put("createdLocalCount", createdLocalCount);
            map.put("freeMemory", freeMemory);
            map.put("totalMemory", totalMemory);
            return map;
        }
    }

    public static class IncrementAndGetId implements Serializable, ControlMessage { } //request for unique-id -> long (< 0: follower, >0: leader)

    public static List<Class<?>> getTypes() {
        return Arrays.asList(RemoteManagerMessages.class.getClasses());
    }
}
