package csl.dataproc.actor.stat;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.dispatch.*;
import akka.japi.Creator;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import csl.dataproc.actor.remote.RemoteManager;
import csl.dataproc.tgls.util.JsonWriter;
import scala.Option;

import java.io.Serializable;
import java.nio.file.Path;
import java.time.Instant;
import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.regex.Pattern;
import java.util.stream.Stream;

/**
 * using conf file for setting:
 * <pre>
 *     stat.conf:
 *         akka.actor.default-mailbox.mailbox-type = "csl.dataproc.actor.stat.StatMailbox"
 * </pre>
 * RemoteConfig and RemoteManager can automatically generate the conf.
 *  <b>If you want to selectively set the mailbox, you can use {@link RemoteManager#create(ActorSystem, Class, Creator)}</b>
 * <p>
 * The constructor of the class sets an instance to the {@link #globalInstance} field.
 * <p>
 * at the end of the system, it will need to call {@link StatMailbox#endGlobal()}.
 */
public class StatMailbox implements MailboxType {
    protected ExecutorService initSender;
    protected StatSystem statSystem;
    protected Comparator<Envelope> comparator;

    protected static StatMailbox globalInstance;

    public StatMailbox() {
        initSender = Executors.newFixedThreadPool(2);
        statSystem = StatSystem.system();

        if (globalInstance != null) {
            log("existing mailbox: " + globalInstance);
        }
        globalInstance = this;
    }
    public StatMailbox(ActorSystem.Settings settings, Config config) {
        this();
    }

    public static Config load() {
        return ConfigFactory.load(StatMailbox.class.getClassLoader(),
                StatMailbox.class.getPackage().getName().replace('.', '/') + "/stat.conf");
    }

    @Override
    public MessageQueue create(Option<ActorRef> owner, Option<ActorSystem> system) {
        //System.err.println("create " + owner + " : " + queueCount.get());
        if (owner.isEmpty() && system.isEmpty()){
            log("stat-mailbox with empty actor and system");
        }
        long id = -1;
        if (owner.isDefined()) {
            if (system.isDefined()) {
                id = StatSystem.system().getId(system.get(), owner.get().path().toStringWithoutAddress());
            }
            try {
                StatQueue q = createQueue(id);
                sendActorId(q, owner, system);
                return q;
            } catch (Exception ex) {
                return createQueue(id);
            }
        } else {
            return createQueue(id);
        }
    }


    public StatQueue createQueue(long id) {
        if (comparator == null) {
            comparator = initComparator(statSystem);
        }
        if (!comparator.equals(DEFAULT_COMPARATOR)) {
            try {
                return new StatQueuePriority(id, statSystem, 24, comparator);
            } catch (Exception ex) {
                ex.printStackTrace();
                return new StatQueueControlAware(id, statSystem);
            }
        } else {
            return new StatQueueControlAware(id, statSystem);
        }
    }

    /** indicating that ControlMessage is prioritized any other messages, can be realized by ControlAwareMailbox */
    public static final EnvelopePriorityComparator DEFAULT_COMPARATOR = new EnvelopePriorityComparator(ControlMessage.class);

    public static class EnvelopePriorityComparator implements Serializable, Comparator<Envelope> {
        protected Class<?>[] types;

        public EnvelopePriorityComparator(Class<?>... types) {
            this.types = types;
        }

        @Override
        public int compare(Envelope o1, Envelope o2) {
            return Integer.compare(find(o1.message()), find(o2.message()));
        }

        protected int find(Object m) {
            int i = 0;
            for (Class<?> c : types) {
                if (c.isInstance(m)) {
                    return i;
                }
                ++i;
            }
            return i;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            EnvelopePriorityComparator that = (EnvelopePriorityComparator) o;
            return Arrays.equals(types, that.types);
        }

        @Override
        public int hashCode() {
            return Arrays.hashCode(types);
        }
    }

    /**
     * mailbox-type for supporting StatSystem#envelopeComparatorType
     */
    public static class EnvelopePriorityMailboxType implements MailboxType {
        protected Comparator<Envelope> comparator;
        protected StatSystem statSystem;

        public EnvelopePriorityMailboxType() {
            this.statSystem = StatSystem.system();
        }

        public EnvelopePriorityMailboxType(StatSystem statSystem) {
            this.statSystem = statSystem;
        }

        public EnvelopePriorityMailboxType(ActorSystem.Settings s, Config c) { //important
            this();
        }

        @Override
        public MessageQueue create(Option<ActorRef> owner, Option<ActorSystem> system) {
            if (comparator == null) {
                comparator = initComparator(StatSystem.system());
            }
            if (comparator.equals(DEFAULT_COMPARATOR)) {
                return new UnboundedControlAwareMailbox.MessageQueue();
            } else {
                return new UnboundedPriorityMailbox.MessageQueue(24, comparator);
            }
        }
    }

    @SuppressWarnings("unchecked")
    public static Comparator<Envelope> initComparator(StatSystem statSystem) {
        String compType = statSystem.getEnvelopeComparatorType();
        if (compType != null) {
            try {
                return (Comparator<Envelope>) Class.forName(compType).newInstance();
            } catch (Exception ex) {
                ex.printStackTrace();
                return DEFAULT_COMPARATOR;
            }
        } else {
            return DEFAULT_COMPARATOR;
        }
    }

    public void log(String str) {
        RemoteManager m = RemoteManager.instance;
        if (m != null) {
            m.log(str);
        } else {
            System.err.println("[" + LocalDateTime.now() + "] " + str);
        }
    }


    //sending ActorId (id number of the actor and it's queue)
    public void sendActorId(StatQueue q, Option<ActorRef> owner, Option<ActorSystem> system) {
        initSender.execute(() -> {
            ActorRef a = owner.get();
            StatMessages.ActorId aid = new StatMessages.ActorId(a, q.id, q);
            try {
                Thread.sleep(10);
                aid.target.tell(aid, ActorRef.noSender());
            } catch (Exception ex) {
                System.err.println("error " + ex + " : retry");
                try {
                    Thread.sleep(100);
                } catch (Exception ex2) {
                    ex2.printStackTrace();
                }
                aid.target.tell(aid, ActorRef.noSender());
            }
        });
    }


    public static void endGlobal() {
        StatMailbox g = globalInstance;
        if (g != null) {
            g.end();
        }
    }

    public void end() {
        initSender.shutdown();
        initSender = null;
    }

    public static MailboxInOutCount mailboxTotal(StatSystem statSystem, Instant[] timeRange, Stream<Path> mailboxFiles) {
        StatSystem.TotalCountReducer<MailboxInOutCount> r = new StatSystem.TotalCountReducer<MailboxInOutCount>() {
            @Override
            public MailboxInOutCount createTotal() {
                return new MailboxInOutCount(-1L);
            }

            @Override
            public MailboxInOutCount create(Object json) {
                return new MailboxInOutCount(json);
            }

            @Override
            public void update(MailboxInOutCount r, MailboxInOutCount next) {
                r.merge(next);
            }
        };
        return statSystem.total(timeRange, mailboxFiles, r);
    }

    public static String totalNameFormat = "mt%d.json";
    public static String durationNameFormat = "md%d.jsonp";

    public static Pattern durationNamePattern = Pattern.compile("md(.*?)\\.jsonp");

    public static abstract class StatQueue implements MessageQueue {
        protected long id;
        protected MailboxInOutCount total;
        protected MailboxInOutCount duration;
        protected StatSystem statSystem;

        public StatQueue(long id, StatSystem statSystem) {
            this.id = id;
            this.statSystem = statSystem;
            total = new MailboxInOutCount(id);
            duration = new MailboxInOutCount(id);
        }

        public void setId(long id) {
            this.id = id;
            total.id = id;
            duration.id = id;
        }

        public long getId() {
            return id;
        }

        public void enqueueBase(Envelope handle) {
            int num = numberOfMessages();
            total.enqueue(num, handle.message().getClass());
            duration.enqueue(num, handle.message().getClass());

            checkAndSave(false);
        }

        public void dequeueBase() {
            int num = numberOfMessages();
            total.dequeue(num);
            duration.dequeue(num);

            checkAndSave(false);
        }

        public void cleanUpBase() {
            checkAndSave(true);
            saveTotal();
        }


        public synchronized void checkAndSave(boolean force) {
            if (duration.isOver(statSystem.getInterval()) || force) {
                duration.updateDuration();
                saveDuration();
                duration = new MailboxInOutCount(id);
            }
        }


        public void saveTotal() {
            try {
                saveDuration();
                total.updateDuration();
                JsonWriter.write(total.toJson(), getTotalFile(id));
            } catch (Exception ex) {
                ex.printStackTrace();
            }
        }

        public void saveDuration() {
            statSystem.saveJsonp(durationNameFormat, duration);
        }

        public Path getTotalFile(long id) {
            return statSystem.getFile(totalNameFormat, id);
        }

    }

    public static class StatQueueControlAware extends StatQueue implements MessageQueue, UnboundedControlAwareMessageQueueSemantics {
        protected UnboundedControlAwareMailbox.MessageQueue body;

        public StatQueueControlAware(long id, StatSystem statSystem) {
            super(id, statSystem);
            body = new UnboundedControlAwareMailbox.MessageQueue();
        }

        @Override
        public Queue<Envelope> controlQueue() {
            return body.controlQueue();
        }

        @Override
        public Queue<Envelope> queue() {
            return body.queue();
        }

        @Override
        public void enqueue(ActorRef receiver, Envelope handle) {
            enqueueBase(handle);
            body.enqueue(receiver, handle);
        }

        @Override
        public Envelope dequeue() {
            dequeueBase();
            return body.dequeue();
        }

        @Override
        public int numberOfMessages() {
            return body.numberOfMessages();
        }

        @Override
        public boolean hasMessages() {
            return body.hasMessages();
        }

        @Override
        public void cleanUp(ActorRef owner, MessageQueue deadLetters) {
            cleanUpBase();
            body.cleanUp(owner, deadLetters);
        }
    }

    public static class StatQueuePriority extends StatQueue implements MessageQueue {
        protected UnboundedPriorityMailbox.MessageQueue body;
        @SuppressWarnings("unchecked")
        public StatQueuePriority(long id, StatSystem statSystem) {
            super(id, statSystem);
            String compType = statSystem.getEnvelopeComparatorType();
            try {
                body = new UnboundedPriorityMailbox.MessageQueue(24,
                        (Comparator<Envelope>) Class.forName(compType).newInstance());
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }

        public StatQueuePriority(long id, StatSystem statSystem, int initCap, Comparator<Envelope> e) {
            super(id, statSystem);
            String compType = statSystem.getEnvelopeComparatorType();
            body = new UnboundedPriorityMailbox.MessageQueue(initCap, e);
        }

        @Override
        public void enqueue(ActorRef receiver, Envelope handle) {
            enqueueBase(handle);
            body.enqueue(receiver, handle);
        }

        @Override
        public Envelope dequeue() {
            dequeueBase();
            return body.dequeue();
        }

        @Override
        public int numberOfMessages() {
            return body.numberOfMessages();
        }

        @Override
        public boolean hasMessages() {
            return body.hasMessages();
        }

        @Override
        public void cleanUp(ActorRef owner, MessageQueue deadLetters) {
            cleanUpBase();
            body.cleanUp(owner, deadLetters);
        }
    }

    public static class MailboxInOutCount extends StatSystem.StatBase {
        public Map<String,StatCount> classToCount;
        public StatCount totalEnqueueCount;
        public StatCount totalDequeueCount;

        public MailboxInOutCount(long id) {
            super(id);
        }

        public MailboxInOutCount(Object json) {
            super(-1);
            setJson(json);
        }

        @Override
        protected void init() {
            super.init();
            classToCount = new ConcurrentHashMap<>();
            totalEnqueueCount = new StatCount();
            totalDequeueCount = new StatCount();
        }

        public synchronized void enqueue(int size, Class<?> cls) {
            String name = cls.getName();
            classToCount.computeIfAbsent(name, k -> new StatCount())
                    .inc(size);
            totalEnqueueCount.inc(size);
        }

        public synchronized void dequeue(int size) {
            totalDequeueCount.inc(size);
        }

        public long queueCount() {
            return totalDequeueCount.count + totalEnqueueCount.count;
        }

        public Map<String,Object> toJson() {
            Map<String,Object> obj = super.toJson();
            obj.put("totalDequeueCount", totalDequeueCount.toJson());
            obj.put("totalEnqueueCount", totalEnqueueCount.toJson());

            Map<String,Object> classToCountJson = new HashMap<>();
            classToCount.forEach((k,v) -> classToCountJson.put(k, v.toJson()));
            obj.put("classToCount", classToCountJson);
            return obj;
        }
        @SuppressWarnings("unchecked")
        public void setJson(Object json) {
            super.setJson(json);
            Map<String,Object> m = (Map<String,Object>) json;
            totalDequeueCount.setJson(((Map<String,Object>) m.get("totalDequeueCount")));
            totalEnqueueCount.setJson(((Map<String,Object>) m.get("totalEnqueueCount")));
            ((Map<String,Object>) m.get("classToCount")).forEach((k,v) -> {
                classToCount.computeIfAbsent(k, (k2) -> new StatCount()).setJson((Map<String,Object>) v);
            });
        }

        public void merge(MailboxInOutCount m) {
            this.totalEnqueueCount.merge(m.totalEnqueueCount);
            this.totalDequeueCount.merge(m.totalDequeueCount);
        }

        public long maxQueueSize() {
            return Math.max(totalEnqueueCount.maxQueueSize, totalDequeueCount.maxQueueSize);
        }
    }

    public static class StatCount {
        public long maxQueueSize;
        public long minQueueSize = -1;
        public long count;

        public void inc(int n) {
            if (maxQueueSize < n) {
                maxQueueSize = n;
            }
            if (minQueueSize == -1 || minQueueSize > n) {
                minQueueSize = n;
            }
            ++count;
        }

        public Map<String,Object> toJson() {
            Map<String, Object> json = new HashMap<>();
            json.put("maxQueueSize", maxQueueSize);
            json.put("minQueueSize", minQueueSize);
            json.put("count", count);
            return json;
        }

        public void setJson(Map<String,Object> json) {
            maxQueueSize = ((Number) json.getOrDefault("maxQueueSize", 0)).longValue();
            minQueueSize = ((Number) json.getOrDefault("minQueueSize", -1L)).longValue();
            count = ((Number) json.getOrDefault("count", 0)).longValue();
        }

        public void merge(StatCount c) {
            maxQueueSize = Math.max(maxQueueSize, c.maxQueueSize);
            minQueueSize = Math.min(minQueueSize, c.minQueueSize);
            count = Math.max(count, c.count);
        }
    }


}
