package csl.dataproc.actor.stat;

import akka.actor.AbstractActor;
import akka.actor.ActorPath;
import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import csl.dataproc.tgls.util.JsonWriter;
import scala.PartialFunction;

import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.stream.Stream;

/**
 * counting processed messages for an actor.
 *  <p>
 *      <ul>
 *          <li>To use the stat, define an instance field of this class in the actor.</li>
 *          <li>Next, override {@link akka.actor.AbstractActor#aroundReceive(PartialFunction, Object)} and
 *               1. call {@link #receiveBefore(Object)} with the message,
 *               2. call super.aroundReceive, and
 *               3. call {@link #receiveAfter(Object, String)} with the message and a state.
 *               The state can be any string and will be saved as the "state" property for debugging.
 *               The state is held by the stat and it's change will cause file saving.
 *               The instance of the stat may be null.
 *               The receiveAfter will automatically save counts to a file</li>
 *           <li> override {@link AbstractActor#preStart()} for setting the instance of the stat.
 *               The id can be obtained from {@link StatSystem#getId(ActorSystem, String)}
 *                  and the path can be obtained from {@link AbstractActor#self()},
 *                    {@link ActorRef#path()}, and {@link ActorPath#toStringWithoutAddress()}. </li>
 *           <li>Another way for setting the instance:
 *               the actor can receive a {@link csl.dataproc.actor.stat.StatMessages.ActorId} message from {@link StatMailbox}
 *                for setting the instance of the stat with actor-id.</li>
 *      </ul>
 *
 *
 * <pre>
 *     class MyActor extends AbstractActor {
 *         StatReceive stat;
 *
 *         //Override
 *         public void aroundReceive(PartialFunction&lt;Object, BoxedUnit&gt; receive, Object msg) {
 *             if (stat != null) stat.receiveBefore(msg);
 *             try {
 *                super.aroundReceive(receive, msg);
 *             } finally { if (stat != null) stat.receiveAfter(msg, "normal"); }
 *         }
 *
 *         //Override
 *         public void preStart() {
 *             StatSystem s = StatSystem.system();
 *             if (s.isEnabled()) {
 *                long id = s.getId(context().system(), self().path().toStringWithoutAddress())
 *                stat = new StatReceive(id, self().path());
 *             }
 *         }
 *         //or //Override
 *         public Receive createReceive() {
 *            return receiveBuilder()
 *                     .match(ActorId.class, (a) -&gt; stat = new StatReceive(a.id, self().path()))
 *                     ...;
 *         }
 *     }
 * </pre>
 */
public class StatReceive {
    protected long id;
    protected Instant lastConsumedStart = Instant.now();
    protected StatSystem statSystem;

    protected ReceiveCount total;
    protected ReceiveCount duration;

    public static Duration durationStateMin = Duration.ofMillis(20);

    public static String totalNameFormat = "rt%d.json";
    public static String durationNameFormat = "rd%d.jsonp";

    public static Pattern durationNamePattern = Pattern.compile("rd(.*?)\\.jsonp");

    public StatReceive(long id, ActorPath path) {
        this.id = id;
        statSystem = StatSystem.system();
        this.total = newCountTotal(id, path == null ? "" : path.toSerializationFormat());
        this.duration = newCountDuration(id);
    }

    protected ReceiveCount newCountDuration(long id) {
        return new ReceiveCount(id);
    }
    protected ReceiveCount newCountTotal(long id, String path) {
        return new ReceiveCount(id, path);
    }

    public long getId() {
        return id;
    }

    public ReceiveCount getTotal() {
        return total;
    }

    public Instant getLastConsumedStart() {
        return lastConsumedStart;
    }

    public void receiveBefore(Object o) {
        lastConsumedStart = Instant.now();
    }

    public void receiveAfter(Object o, String state) {
        try {
            Duration consumed = Duration.between(lastConsumedStart, Instant.now());
            total.countUp(o, consumed);
            total.state = state;

            duration.countUp(o, consumed);
            if ((!duration.state.equals(state) &&
                    duration.durationNow().compareTo(durationStateMin) >= 0)
                    || duration.isOver(statSystem.getInterval())) {
                duration.updateDuration();
                saveDuration();
                duration = newCountDuration(id);
                duration.state = state;
            }
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    public void saveDuration() {
        statSystem.saveJsonp(durationNameFormat, duration);
    }

    public void saveTotal() {
        try {
            saveDuration();
            JsonWriter.write(total.toJson(), getTotalFile(id));
        } catch (Exception ex) {
            ex.printStackTrace();
        }

    }

    public Path getDurationFile(long id) {
        return statSystem.getFile(durationNameFormat, id);
    }

    public Path getTotalFile(long id) {
        return statSystem.getFile(totalNameFormat, id);
    }

    public static StatReceive.ReceiveCount receiveTotal(StatSystem statSystem, Instant[] timeRange, Stream<Path> receiveFiles) {
        StatSystem.TotalCountReducer<ReceiveCount> r = new StatSystem.TotalCountReducer<ReceiveCount>() {
            @Override
            public ReceiveCount createTotal() {
                return new ReceiveCount(-1L);
            }
            @Override
            public ReceiveCount create(Object json) {
                return new ReceiveCount(json);
            }
            @Override
            public void update(ReceiveCount r, ReceiveCount next) {
                r.count = Math.max(r.count, next.count);
            }
        };
        return statSystem.total(timeRange, receiveFiles, r);
    }

    public void add(StatReceive pre) {
        duration.add(pre.duration);
        total.add(pre.total);
    }

    public static class ReceiveCount extends StatSystem.StatBase {
        public String path;
        public Duration consumed;
        public long count;
        public Map<String,Count> classToCount;

        public String state = "";

        public ReceiveCount(long id) {
            super(id);
        }

        public ReceiveCount(long id, String path) {
            this(id);
            this.path = path;
        }

        public ReceiveCount(Object json) {
            super(-1);
            setJson(json);
        }

        public void add(ReceiveCount count) {
            this.count += count.count;
            this.consumed = consumed.plus(count.consumed);
            count.classToCount.forEach((k,v) -> this.classToCount
                    .computeIfAbsent(k, k2 -> new Count())
                    .add(v));
        }

        @Override
        protected void init() {
            super.init();
            consumed = Duration.ZERO;
            classToCount = new HashMap<>();
        }

        public void countUp(Object o, Duration d) {
            consumed = consumed.plus(d);
            count++;
            Count c = classToCount.computeIfAbsent(o.getClass().getName(), k -> new Count());
            c.count++;
            c.consumed = c.consumed.plus(d);
        }
        public Map<String,Object> toJson() {
            Map<String,Object> m = super.toJson();
            m.put("consumed", consumed.toString());
            m.put("count", count);
            m.put("path", path);

            Map<String,Object> cc = new HashMap<>();
            for (Map.Entry<String,Count> e : classToCount.entrySet()) {
                cc.put(e.getKey(), e.getValue().toJson());
            }
            m.put("classToCount", cc);

            m.put("state", state);
            return m;
        }
        @SuppressWarnings("unchecked")
        public void setJson(Object json) {
            super.setJson(json);
            Map<String,Object> map = (Map<String,Object>) json;
            consumed = durationValue(map, "consumed");
            count = longValue(map, "count");
            Map<String,Object> cc = (Map<String,Object>) map.get("classToCount");
            cc.forEach((k,v) -> classToCount.put(k, new Count(v)));
            state = (String) map.get("state");

            path = (String) map.get("path");
        }
    }

    public static class Count {
        public long count;
        public Duration consumed = Duration.ZERO;

        public Count() {}
        public Count(Object json) {
            setJson(json);
        }

        public Map<String,Object> toJson() {
            Map<String,Object> m = new HashMap<>();
            m.put("count", count);
            m.put("consumed", consumed.toString());
            return m;
        }
        @SuppressWarnings("unchecked")
        public void setJson(Object json) {
            Map<String,Object> m = (Map<String,Object>) json;
            count = StatSystem.StatBase.longValue(m, "count");
            consumed = StatSystem.StatBase.durationValue(m, "consumed");
        }

        public void add(Count c) {
            this.count += c.count;
            this.consumed = this.consumed.plus(c.consumed);
        }
    }
}
