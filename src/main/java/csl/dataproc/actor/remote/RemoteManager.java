package csl.dataproc.actor.remote;

import akka.actor.*;
import akka.cluster.Cluster;
import akka.cluster.Cluster$;
import akka.cluster.ClusterEvent;
import akka.japi.Creator;
import akka.japi.pf.FI;
import akka.pattern.PatternsCS;
import akka.serialization.Serialization;
import akka.util.Timeout;
import csl.dataproc.actor.birch.BirchMessages;
import csl.dataproc.actor.stat.StatSystem;
import csl.dataproc.actor.stat.StatVm;
import csl.dataproc.tgls.util.JsonWriter;
import scala.concurrent.Await;
import scala.concurrent.duration.FiniteDuration;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.*;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

/**
 * a remote managing actor.
 *  <p>
 *      the class is a main class, can be used for starting a cluster node.
 *  </p>
 *
 *  <p>
 *      Your remote application can create the actor by {@link #createManager(ActorSystem, RemoteConfig)}.
 *       Both the system and the config can be obtained from {@link RemoteConfig}.
 *  </p>
 *
 * <p>
 *     The class relies on  akka {@link Cluster}.
 *     The other nodes are maintained by the list of {@link #getNodes()}.
 *     An actor of this class can create another manager {@link #getNextManager()} on a next remote node.
 * </p>
 *
 * <p>
 *     {@link #scatterIntervalTask()}:
 *      the actor periodically executes the method, in order to join to or collect remote nodes.
 *      The task is set by {@link #updateScatterIntervalTask(long)} in {@link #preStart()}.
 * </p>
 *
 * <p>
 *     if {@link StatSystem#isEnabled()}, it sets {@link StatVm}.
 *       It first call {@link StatVm#saveRuntime(ActorSystem, Duration)}
 *         for setting up an interval task to collect and save data.
 * </p>
 *
 * <p> your application can create actors of any type by {@link #create(ActorSystem, Class, Creator)}.
 * </p>
 * <pre>
 *    node1: {@link #create(ActorSystem,Class, Creator)} with {@link Creator} c
 *        =&gt; {@link #waitCreation(Class, Creator)}
 *             get and increment the createdCount: id = BlockingQueue{}
 *            =&gt; {@link #createActor(RemoteManagerMessages.Creation)} with cr:Creation(c, node1, id, reset:false)
 *               if {@link #availableActorLocal()}, create an actor a, and
 *                  {@link #created(RemoteManagerMessages.Created)} with Created(a, cr.id)
 *
 *               else, {@link #getNextManager()}.tell(cr, self())
 *   =&gt;
 *    node2: {@link #createActor(RemoteManagerMessages.Creation)} with cr
 *            if {@link #availableActorLocal()}, create an actor a , and
 *                {@link #tellManager(String, Object)} with cr.from(==node1), Created(a, cr.id)
 *            else, {@link #getNextManager()}.tell(cr, self())
 *    //asynchronously
 *    node1:   block with createdActors.get(id).take()
 *               return the created actor and remove id from createdActors
 * </pre>
 *
 * <p> config delivering, {@link RemoteManagerMessages.SetConfig}:
 *     the manager actor can receive {@link RemoteManagerMessages.SetConfig} message and deliver it to other nodes.
 *     The message has {@link SharedConfig} instance to be delivered.
 * </p>
 */
public class RemoteManager extends AbstractActor {
    public static String NAME = "clusterManager";
    public static String PATH = "/user/" + NAME;
    protected Cluster cluster;

    public static RemoteManager instance;
    protected ActorSystem system;
    protected Address address;
    protected AtomicLong count = new AtomicLong();
    protected List<RemoteNode> nodes = new ArrayList<>();
    protected ActorRef nextManager;
    protected long initCount;
    protected List<String> joinTargets;

    protected Cancellable scatterTask;
    protected long lastScatterTaskDuration;
    protected Instant startTime;

    protected boolean leader;

    protected AtomicLong createdCount = new AtomicLong();
    protected Map<Long,BlockingQueue<ActorRef>> createdActors = new ConcurrentHashMap<>();
    protected AtomicLong localCreatedCount = new AtomicLong();

    protected boolean test;

    protected long systemId;

    protected StatVm statVm;
    protected FiniteDuration nextNodeTimeout;
    protected FiniteDuration idTimeout;
    protected FiniteDuration newActorTimeout;

    protected List<SharedConfig> sendingConfigs = new ArrayList<>();

    protected boolean reduceLog = System.getProperty("csl.dataproc.actor.remote.reduceLog", "false").equals("true");

    protected AtomicLong idCount = new AtomicLong(0);
    protected static AtomicLong fallbackIdCount = new AtomicLong(0);

    public static void main(String[] args) {
        RemoteConfig conf = new RemoteConfig();
        List<String> rest = conf.parseArgs(Arrays.asList(args));
        if (rest.contains("--help")) {
            return;
        }
        if (!rest.isEmpty()) {
            System.err.println("unknown arguments: " + rest);
        }

        ActorSystem system = conf.createSystem();
        saveConfig(conf, system);
        createManager(system, conf);
    }

    public static void saveConfig(RemoteConfig conf, ActorSystem system) {
        StatSystem statSystem = StatSystem.system();
        if (statSystem.isEnabled()) {
            Path dir = statSystem.getDir();
            if (dir != null) {
                try {
                    Files.createDirectories(dir);
                } catch (Exception ex) {
                    ex.printStackTrace();
                }
                JsonWriter.write(RemoteConfig.toJsonByIntrospection(conf),
                        dir.resolve(String.format("config-remote-%d.json", conf.getSystemId())));
            }
        }
    }

    public static ActorRef createManager(ActorSystem system, RemoteConfig conf) {
        return createManager(system, conf.leader, conf.count, conf.joins, conf.test, conf.getSystemId());
    }

    public static ActorRef createManager(ActorSystem system, boolean leader, long count, List<String> joinTargets, boolean test,
                                         long systemId) {
        return system.actorOf(Props.create(RemoteManager.class, () -> new RemoteManager(leader, count, joinTargets, test, systemId))
                .withMailbox(RemoteConfig.CONF_CONTROL_MAILBOX)
                .withDispatcher(RemoteConfig.CONF_PINNED_DISPATCHER), NAME);
    }

    public RemoteManager(boolean leader, long initCount, List<String> joinTargets, boolean test, long systemId) {
        this.leader = leader;
        this.initCount = initCount;
        count.set(initCount);
        this.joinTargets = joinTargets;
        startTime = Instant.now();
        this.test = test;
        this.systemId = systemId;
        nextNodeTimeout = FiniteDuration.apply(10, TimeUnit.HOURS);
        idTimeout = FiniteDuration.apply(10, TimeUnit.HOURS);
        newActorTimeout = FiniteDuration.apply(10, TimeUnit.HOURS);
    }

    public void log(String str) {
        Instant now = Instant.now();
        LocalTime time = LocalTime.now();
        System.err.println("[" + address + ": A(" + count.get() + ") " +
                time + " (" + Duration.between(startTime, now) + ")] " + str);
    }

    @Override
    public void preStart() throws Exception {
        address = context().system().provider().getDefaultAddress();
        try {
            cluster = Cluster$.MODULE$.get(context().system());
        } catch (Exception ex) {
            log("disabled cluster: " + ex);
        }

        if (cluster != null) {
            cluster.subscribe(self(), ClusterEvent.MemberEvent.class);
        }
        this.system = context().system();
        instance = this;

        addRemoteNode(address.toString(), true, false);
        joinTargets.forEach(t -> addRemoteNode(t, false, false));

        StatSystem.system().setSystemId(system, systemId);

        updateScatterIntervalTask(2);
        scatterIntervalTask();

        if (StatSystem.system().isEnabled()) {
            statVm = new StatVm();
            statVm.saveRuntime(system, StatSystem.system().getInterval());
        }
        log("remote-manager start: " + address.toString() + " : " + LocalDateTime.now());
    }

    public synchronized void updateScatterIntervalTask(long secs) {
        if (scatterTask != null) {
            scatterTask.cancel();
        }
        FiniteDuration interval = new FiniteDuration(secs, TimeUnit.SECONDS);
        scatterTask = system.scheduler().schedule(
                interval, interval,
                this::scatterIntervalTask, system.dispatcher());
        lastScatterTaskDuration = secs;
    }

    @Override
    public void postStop() throws Exception {
        if (cluster != null) {
            cluster.unsubscribe(self());
        }
        if (scatterTask != null) {
            scatterTask.cancel();
        }
        if (statVm != null) {
            statVm.saveTotal();
        }
        scatterLeaveSelf();
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(RemoteManagerMessages.Creation.class, wrap(this::createActor))
                .match(RemoteManagerMessages.Created.class, wrap(this::created))
                .match(ClusterEvent.MemberUp.class, wrap(this::memberUp))
                .match(ClusterEvent.MemberLeft.class, wrap(this::memberLeft))
                .match(RemoteManagerMessages.Join.class, wrap(this::join))
                .match(RemoteManagerMessages.Leave.class, wrap(this::leave))
                .match(RemoteManagerMessages.Shutdown.class, wrap(this::shutdown))
                .match(RemoteManagerMessages.Test.class, wrap(this::test))
                .match(RemoteManagerMessages.SetConfig.class, wrap(this::setConfig))
                .match(RemoteManagerMessages.GetStatusAll.class, wrap(this::getStatusAll))
                .match(RemoteManagerMessages.GetStatus.class, wrap(this::getStatus))
                .match(RemoteManagerMessages.IncrementAndGetId.class, wrap(this::receiveIncrementAndGetId))
                .build();
    }

    private <M> FI.UnitApply<M> wrap(FI.UnitApply<M> f) {
        return f;
    }

    public void memberUp(ClusterEvent.MemberUp up) {
        String addr = up.member().address().toString();
        addRemoteNode(addr, address.toString().equals(addr), true);
    }

    public void memberLeft(ClusterEvent.MemberLeft left) {
        removeRemoteNode(left.member().address().toString());
    }


    public synchronized boolean addRemoteNode(String node, boolean self, boolean updateTime) {
        boolean added = false;
        RemoteNode n = findRemoteNode(node);
        if (n == null) {
            n = new RemoteNode(node);
            added = true;
        }
        if (self) {
            n.self = true;
        }
        if (updateTime) {
            Instant pre = n.lastCheckedTime;
            n.lastCheckedTime = Instant.now();
            validateRef(pre, n);
            if (!added && pre == null) {
                logRemoteNodes();
                sendAllConfigs(n);
            }
        }
        if (added) {
            nodes.add(n);
            Collections.sort(nodes);
            nextManager = null;
            updateStatNode();
            logRemoteNodes();
            sendAllConfigs(n);
        }
        return added;
    }

    private void validateRef(Instant pre, RemoteNode updatedNode) {
        if (pre != null && Duration.between(pre, updatedNode.lastCheckedTime).compareTo(Duration.ofMinutes(1)) < 0) {
            updatedNode.refInvalid = false;
        }
    }

    protected void sendAllConfigs(RemoteNode node) {
        for (SharedConfig conf : sendingConfigs) {
            sendConfig(node, conf);
        }
    }

    protected RemoteNode findRemoteNode(String node) {
        for (RemoteNode n : getNodes()) {
            if (n.address.equals(node)) {
                return n;
            }
        }
        return null;
    }

    public synchronized List<RemoteNode> getNodes() {
        return new ArrayList<>(nodes);
    }

    public synchronized boolean removeRemoteNode(String node) {
        RemoteNode n = findRemoteNode(node);
        if (n != null) {
            nodes.remove(n);
            nextManager = null;
            updateStatNode();
            logRemoteNodes();
            return true;
        } else {
            return false;
        }
    }

    public void logRemoteNodes() {
        List<RemoteNode> nodes = getNodes();
        int i = 0;
        StringBuilder buf = new StringBuilder();
        buf.append("nodes[").append(nodes.size()).append("]");
        for (RemoteNode node : nodes) {
            buf.append("\n [").append(i).append( "]: ").append(node);
            ++i;
        }
        log(buf.toString());
    }

    protected long joinCount;
    protected Instant joinPrevCheck = Instant.now();

    public void join(RemoteManagerMessages.Join j) {
        checkJoinFlood(j);
        if (addRemoteNode(j.address, false, j.fromAddress)) {
            joinSelfBack(j);
            scatterToOtherNodes(j);
        }
    }

    protected void checkJoinFlood(RemoteManagerMessages.Join j) {
        ++joinCount;
        Instant now = Instant.now();
        Duration d = Duration.between(joinPrevCheck, now);
        if (d.compareTo(Duration.ofSeconds(3)) >= 0) {
            if (joinCount > 1000) {
                log("join flood: " + joinCount + " : " + j);
                logRemoteNodes();
            }
            joinPrevCheck = now;
            joinCount = 0;
        }
    }

    public void scatterToOtherNodes(RemoteManagerMessages.ManagerEvent cause) {
        for (RemoteNode node : getNodes()) {
            if (!node.self && !cause.address.equals(node.address)) {
                tellManager(node.address, cause);
            }
        }
    }

    public void joinSelfBack(RemoteManagerMessages.Join j) {
        tellManager(j.address, new RemoteManagerMessages.Join(address.toString(), true));
    }

    public void scatterJoinSelf() {
        scatterToOtherNodes(new RemoteManagerMessages.Join(address.toString(), true));
    }

    public void scatterJoinToEntireNodes() {
        for (RemoteNode node : getNodes()) {
            scatterToOtherNodes(new RemoteManagerMessages.Join(node.address, node.self));
        }
    }

    public void scatterLeaveSelf() {
        scatterToOtherNodes(new RemoteManagerMessages.Leave(address.toString()));
    }

    /**
     * <ul>
     *     <li>leader: {@link #scatterJoinToEntireNodes()}
     *        <ul>
     *            <li>for each node: {@link #scatterToOtherNodes(RemoteManagerMessages.ManagerEvent)}
     *                 with {@link RemoteManagerMessages.Join} (node.address):
     *                   sending the Join to all other nodes excluding self node</li>
     *        </ul>
     *     </li>
     *
     *     <li>follower: {@link #scatterJoinSelf()}
     *         <ul>
     *             <li>{@link #scatterToOtherNodes(RemoteManagerMessages.ManagerEvent)}
     *                  with {@link RemoteManagerMessages.Join} (this.address):
     *                    sending the self Join to all other nodes excluding self node</li>
     *         </ul>
     *     </li>
     *
     *     <li> once a manager receive a Join, then
     *          {@link #join(RemoteManagerMessages.Join)} adds the new node to its list or updates time-stamp for the node.
     *          Also, if it adds a new node, then {@link #scatterToOtherNodes(RemoteManagerMessages.ManagerEvent)} the join</li>
     *
     * </ul>
     */
    public void scatterIntervalTask() {
        invalidateRefs();
        if (leader) {
            scatterJoinToEntireNodes();
        } else {
            scatterJoinSelf();
        }
        long next = 120;
        if (lastScatterTaskDuration < next && Duration.between(startTime, Instant.now()).compareTo(Duration.ofMinutes(1)) >= 0) {
            updateScatterIntervalTask(next);
            log("update join interval " + lastScatterTaskDuration);
            logRemoteNodes();
        }
        if (test) {
            scatterTest();
        }
    }

    private synchronized void invalidateRefs() {
        Instant now = Instant.now();
        for (RemoteNode n : nodes) {
            if (n.lastCheckedTime != null && Duration.between(n.lastCheckedTime, now).compareTo(Duration.ofMinutes(1)) >= 0) {
                n.refInvalid = true;
            }
        }
    }

    public void scatterTest() {
        int i = 0;
        for (RemoteNode node : getNodes()) {
            if (!node.self) {
                String msg = "test@" + Instant.now() + " -> " + node;
                log("start test[" + i+ "] : " + msg);
                tellManager(node.address, new RemoteManagerMessages.Test(
                        msg, false));
            }
            ++i;
        }
    }

    public void leave(RemoteManagerMessages.Leave l) {
        if (removeRemoteNode(l.address)) {
            scatterToOtherNodes(l);
        }
    }

    public void tellManager(String addr, Object msg) {
        system.actorSelection(addr + PATH).tell(msg, self());
    }


    public void resetActorCount() {
        long n = count.get();
        if (n < 0) {
            count.set(initCount);
            log(String.format("resetActorCount: %,d -> %,d", n, initCount));
        }
    }

    public boolean availableActorLocal() {
        long n = count.decrementAndGet();
        if (n == 0 || n == -1L) {
            log("availableActorLocal: " + n);
        }
        return n >= 0;
    }

    public synchronized ActorRef getNextManager() {
        RemoteNode n = findRemoteNode(address.toString());
        int otherNodes = nodes.size() - 1;
        if (otherNodes > 0) {
            int retryCount = 0;
            int i = nodes.indexOf(n);
            while (retryCount < otherNodes * 2) {
                i++;
                i = i % nodes.size();
                n = nodes.get(i);
                if (n.self) {
                    continue;
                }

                try {
                    return getManager(n);
                } catch (Exception ex) {
                    log("getNextManager error [" + i + "]: retryCount=" + retryCount + " : " + n.address + PATH + " : " + n + " : " + ex);
                    StatVm svm = StatVm.instance;
                    if (svm != null) {
                        svm.addTimeoutNextManager();
                    }
                }
                ++retryCount;
            }
        }
        log("next failure: use self");
        return self();
    }

    public ActorRef getManager(RemoteNode n) throws Exception {
        if (n.self) {
            return self();
        } else {
            ActorRef refCache = getRef(n);
            if (refCache != null) {
                return refCache;
            } else {
                ActorRef ref = Await.result(system.actorSelection(n.address + PATH)
                        .resolveOne(nextNodeTimeout), nextNodeTimeout);
                setRefToNode(n, ref);
                return ref;
            }
        }
    }

    private synchronized ActorRef getRef(RemoteNode n) {
        return n.refInvalid ? null : n.ref;
    }

    private synchronized void setRefToNode(RemoteNode n, ActorRef ref) {
        n.ref = ref;
        n.refInvalid = false;
    }


    public void updateStatActor(int newActor) {
        StatVm stat = StatVm.instance;
        if (stat != null) {
            stat.setRemainActorCount(count.get());
            stat.addNewActorCount(newActor);
        }
    }

    public void updateStatNode() {
        StatVm stat = StatVm.instance;
        if (stat != null) {
            stat.setNodes(new ArrayList<>(getNodes().stream()
                    .map(n -> n.address)
                    .collect(Collectors.toList())));
            stat.setRemainActorCount(count.get());
            if (stat.totalRuntime.path.isEmpty()) {
                stat.totalRuntime.path = self().path().toString();
            }
        }
    }

    public void shutdown(RemoteManagerMessages.Shutdown s) {
        log("shutdown" + (leader ? " leader [" + nodes.size() + "]" : " node"));
        if (leader) {
            for (RemoteNode node : nodes) {
                if (!node.self) {
                    log(" send shutdown : " + node);
                    tellManager(node.address, new RemoteManagerMessages.Shutdown());
                }
            }
        }
        self().tell(PoisonPill.getInstance(), ActorRef.noSender());
        system.terminate();
    }

    /**
     * create an actor with applying the configuration of the manager.
     *   <p>
     *     To create the actor, it constructs a {@link RemoteManagerMessages.Creation} message, and
     *        called {@link RemoteManagerMessages.Creation#props()}
     *         which creates an Props with setting
     *            {@link RemoteConfig#CONF_STAT_MAILBOX} (if stat is enabled)
     *         or {@link RemoteConfig#CONF_CONTROL_MAILBOX} (if stat is disabled).
     *      The
     *   <p>
     *     The method can be used with fallbackSystem even when the manager is not set.
     *     So it can be used as the replacement of system.actorOf(...).
     *   <p>
     *      Regarding serialization of lambda: In Java8, a lambda can be serializable if the target interface is serializable.
     *        But it seems not recommended for mainly stability of the code (i.e. serialVersionUID ?).
     *        So, it is better to define an isolated class of {@link Creator} for remote creation of actors.
     *        <p>
     *        Note: Akka's Props is a case class and thus Serializable, Creator is also a Serializable,
     *          but scala.Function0 seems not to be Serializable, which some constructors of Props take.
     *   <p>
     *     If you need to specify dispatcher for the actor,
     *       you can pass {@link RemoteManagerMessages.DispatcherSpecifiedCreator} as the arg props.
     * @param fallbackSystem the system used when there is no {@link #instance}.
     * @param actor the creating Actor class
     * @param props for remote environment, it must be Serializable.
     * @param <T> the actual Actor type
     * @return the created actor ref of the actor class
     */
    public static <T extends Actor> ActorRef create(ActorSystem fallbackSystem, Class<T> actor, Creator<T> props) {
        RemoteManager m = instance;
        if (m == null) {
            //it should supply an ID?
            return fallbackSystem.actorOf(new RemoteManagerMessages.Creation<>(actor, props, "", 0, false, 0).props());
        } else {
            return m.waitCreation(actor, props);
        }
    }

    public <T extends Actor> ActorRef waitCreation(Class<T> actor, Creator<T> props) {
        Instant time = Instant.now();
        long n = createdCount.getAndIncrement();
        LinkedBlockingQueue<ActorRef> newActorQueue = new LinkedBlockingQueue<>();
        createdActors.put(n, newActorQueue);

        RemoteManagerMessages.Creation<T> creation = new RemoteManagerMessages.Creation<>(actor, props, address.toString(), n, false, 0);
        createActor(creation);
        try {
            ActorRef ref = newActorQueue.poll(newActorTimeout.length(), newActorTimeout.unit()); //wait
            if (ref == null) { //timeout
                ref = getCreatedActorLocal(creation.props());
                StatVm svm = StatVm.instance;
                if (svm != null) {
                    svm.addTimeoutNewActor();
                }
            }
            createdActors.remove(n);
            updateCreationTime(Duration.between(time, Instant.now()));
            return ref;
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    public <T extends Actor> void createActor(RemoteManagerMessages.Creation<T> c) {
        if (c.reset) {
            resetActorCount();
        }
        if (availableActorLocal()) {
            createActorLocal(c);
        } else {
            createActorOnNextManager(c);
        }
    }

    public <T extends Actor> ActorRef getCreatedActorLocal(Props props) {
        localCreatedCount.getAndIncrement();
        ActorRef ref = system.actorOf(props);
        updateStatActor(1);
        return ref;
    }

    public <T extends Actor> void createActorLocal(RemoteManagerMessages.Creation<T> c) {
        ActorRef ref = getCreatedActorLocal(c.props());

        String path = Serialization.serializedActorPath(ref);
        RemoteManagerMessages.Created created = new RemoteManagerMessages.Created(path, c.id, c.startTime, c.forwardedCount);
        if (address.toString().equals(c.from)) {
            created(created);
        } else {
            tellManager(c.from, created);
        }
    }

    public <T extends Actor> void createActorOnNextManager(RemoteManagerMessages.Creation<T> c) {
        ActorRef nextManager = this.nextManager;
        if (nextManager == null) {
            nextManager = getNextManager();
            this.nextManager = nextManager;
        }
        if (nextManager.equals(self())) {
            c.reset = true;
            createActor(c);
        } else {
            if (nextManager.path().address().toString().equals(c.from)) { //back to top
                c.reset = !c.reset;
            }
            c.forwardedCount++;
            nextManager.tell(c, self());
        }
    }

    public void updateCreationTime(Duration d) {
        StatVm vm = StatVm.instance;
        if (vm != null) {
            vm.addCreationTime(d);
        }
    }

    public void created(RemoteManagerMessages.Created c) {
        try {
            ActorRef ref = system.provider().resolveActorRef(c.actor);
            BlockingQueue<ActorRef> q = createdActors.get(c.id);
            if (q == null) {
                //timeout
                Duration d = Duration.between(c.startTime, Instant.now());
                log("!!! created: " + c.actor + " id: " + c.id + " -> resolved: " + ref +
                        " (" + d + ") TIMEOUT: " + newActorTimeout);
                //newActorTimeoutMillis = Math.max(newActorTimeoutMillis, d.toMillis());
                ref.tell(PoisonPill.getInstance(), self());
                updateCreated(c.forwardedCount, false);
            } else {
                q.add(ref);
                if (!reduceLog) {
                    log("created: " + c.actor + " id: " + c.id + " -> resolved: " + ref +
                            " (" + Duration.between(c.startTime, Instant.now()) + ")");
                }
                updateCreated(c.forwardedCount, true);
            }
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    public void updateCreated(int forwarded, boolean success) {
        StatVm vm = StatVm.instance;
        if (vm != null) {
            vm.addCreated(forwarded, success);
        }
    }

    public void test(RemoteManagerMessages.Test t) {
        log(t.message + (t.back ? " (back)" : ""));
        if (!t.back) {
            sender().tell(new RemoteManagerMessages.Test(
                    t.message + " @" + Instant.now() + " -> " + sender().path(), true), self());
        }
    }

    public synchronized void addSharedConfig(SharedConfig config) {
        sendingConfigs.add(config);
        for (RemoteNode node : getNodes()) {
            sendConfig(node, config);
        }
    }

    protected void sendConfig(RemoteNode node, SharedConfig config) {
        if (!node.self && node.lastCheckedTime != null) { //confirmed node
            log("send config to " + node.address + " : " + config);
            tellManager(node.address, new RemoteManagerMessages.SetConfig(config));
        }
    }

    public synchronized void setConfig(RemoteManagerMessages.SetConfig conf) {
        conf.config.arrivedAtRemoteNode();
        sendingConfigs.add(conf.config);
        log("arrive shared config: " + conf.config);
    }

    public void getStatus(RemoteManagerMessages.GetStatus s) {
        sender().tell(createStatus(), self());
    }

    public void getStatusAll(RemoteManagerMessages.GetStatusAll s) {
        List<RemoteManagerMessages.Status> ss = new ArrayList<>();
        for (RemoteNode node : this.nodes) {
            RemoteManagerMessages.Status status;
            if (node.self) {
                status = createStatus();
            } else {
                try {
                    ActorRef ref = getManager(node);
                    Timeout timeout = new Timeout(2, TimeUnit.SECONDS);
                    status = (RemoteManagerMessages.Status) PatternsCS.ask(ref, new RemoteManagerMessages.GetStatus(), timeout)
                            .toCompletableFuture().get(2, TimeUnit.SECONDS);
                } catch (Exception ex) {
                    log("status error: " + node.address + " : " + ex);
                    status = new RemoteManagerMessages.Status(node.address, -1, -1, -1, -1, -1);
                }
            }
            ss.add(status);
            log(String.format("status: %s", formatStatus(status)));
        }
        if (sender() != null) {
            sender().tell(ss, self());
        }
    }

    public RemoteManagerMessages.Status createStatus() {
        long totalRam = Runtime.getRuntime().totalMemory();
        long freeRam = Runtime.getRuntime().freeMemory();
        return new RemoteManagerMessages.Status(address.toString(), count.get(), createdCount.get(), localCreatedCount.get(),
                freeRam, totalRam);
    }

    public String formatStatus(RemoteManagerMessages.Status status) {
        return String.format("%s: count=%,d, created=%,d, createdLocal=%,d, freeMem=%,d, totalMem=%,d",
                status.address, status.count, status.createdCount, status.createdLocalCount, status.freeMemory, status.totalMemory);
    }

    /**
     * @return obtains a unique ID if minus-value <0, then it means the id obtained from a follower, and
     *          plus-value >0, than it means the id obtained from a leader;
     *           the method might send messages for other nodes by incrementAndGetId, so the id unconditionally increments
     */
    public static long incrementAndGetId() {
        RemoteManager manager = instance;
        if (manager == null) {
            return fallbackIdCount.incrementAndGet();
        } else {
            return manager.incrementAndGetIdFromNodes();
        }
    }

    public long incrementAndGetIdFromNodes() {
        if (leader) {
            return idCount.incrementAndGet();
        } else {
            List<RemoteNode> ns;
            synchronized (this) {
                ns = new ArrayList<>(nodes);
            }
            for (RemoteNode node : ns) {
                if (!node.self) {
                    Instant now = Instant.now();
                    try {
                        ActorRef ref = getManager(node);
                        long id = (long) PatternsCS.ask(ref, new RemoteManagerMessages.IncrementAndGetId(), Timeout.durationToTimeout(idTimeout))
                                .toCompletableFuture().get();
                        if (id >= 0) { //leader
                            return id;
                        }
                    } catch (Exception ex) {
                        log("incrementAndGetId error: " + node.address + " " + Duration.between(now, Instant.now()) + " : " + ex);
                    }
                }
            }
            return -idCount.incrementAndGet(); //minus value as a follower
        }
    }

    public void receiveIncrementAndGetId(RemoteManagerMessages.IncrementAndGetId m) {
        sender().tell((leader ? 1L : -1L) * idCount.incrementAndGet(), self());
    }

    public static class RemoteNode implements Comparable<RemoteNode> {
        public String address;
        public boolean self;
        public Instant lastCheckedTime;

        public ActorRef ref;
        public boolean refInvalid;

        public RemoteNode(String address) {
            this.address = address;
        }

        @Override
        public String toString() {
            return address + " " + (self ? "(self)" : toStringTime());
        }

        public String toStringTime() {
            if (lastCheckedTime == null) {
                return "?";
            } else {
                return "<" + LocalDateTime.ofInstant(lastCheckedTime, ZoneId.systemDefault()).toString()
                        + " (" + Duration.between(lastCheckedTime, Instant.now()) + " ago)>";
            }
        }

        @Override
        public int compareTo(RemoteNode o) {
            return address.compareTo(o.address);
        }


        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            RemoteNode that = (RemoteNode) o;
            return address != null ? address.equals(that.address) : that.address == null;
        }

        @Override
        public int hashCode() {
            return address != null ? address.hashCode() : 0;
        }
    }

    /**
     * <pre>
     *     public class MyConf implements SharedConfig, Serializable {
     *         static MyConf instance;
     *         int param;
     *         public MyConf() {}
     *         public MyConf(int param) { //original constructor
     *             this.param = param;
     *             instance = this;
     *         }
     *         public void arrivedAtRemoteNode() {
     *             instance = this;
     *         }
     *     }
     *
     *     RemoteManager.instance.addSharedConfig(new MyConf(123));
     *     //or
     *     remoteManagerRef.tell(new SetConfig(new MyConf(123)), ActorRef.noSender());
     *
     *     public class MyMessage implements Serializable {
     *         transient MyConf conf;
     *         public MyMessage() { conf = MyConf.instance; }
     *     }
     * </pre>
     */
    public interface SharedConfig {
        void arrivedAtRemoteNode();
    }
}
