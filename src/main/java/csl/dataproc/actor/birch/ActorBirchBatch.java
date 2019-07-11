package csl.dataproc.actor.birch;

import akka.actor.*;
import akka.japi.Creator;
import akka.pattern.PatternsCS;
import akka.stream.impl.Throttle;
import akka.util.Timeout;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Registration;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import csl.dataproc.actor.remote.*;
import csl.dataproc.actor.stat.*;
import csl.dataproc.actor.visit.TraversalVisitActor;
import csl.dataproc.birch.*;
import csl.dataproc.csv.CsvLine;
import csl.dataproc.dot.DotGraph;
import csl.dataproc.tgls.util.JsonWriter;
import scala.PartialFunction;
import scala.concurrent.duration.FiniteDuration;
import scala.runtime.BoxedUnit;

import java.io.PrintWriter;
import java.io.Serializable;
import java.io.Writer;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class ActorBirchBatch {
    protected ActorSystem system;
    protected ActorBirchConfig config;
    protected BirchBatchEx batch;
    protected boolean terminateAfterOutput;

    public static void main(String[] args) {
        new ActorBirchBatch().run(args);
    }

    public void run(String... args) {
        List<String> restArgs = init(Arrays.asList(args));
        if (restArgs.contains("--help")) {
            system.terminate();
            return;
        }

        batch.shuffle();
        batch.construct();
        batch.output();
        if (terminateAfterOutput) {
            terminate();
        }
    }

    public BirchBatchEx getBatch() {
        return batch;
    }

    public List<String> init(List<String> args) {
        StatSystem.system().setEnvelopeComparatorType(BirchMessages.BirchEnvelopeComparator.class.getName());
        RemoteConfig remoteConfig = new RemoteConfig();
        //remoteConfig.kryoSerializer = KryoSerializerMonitoring.class.getName(); //for debug
        //remoteConfig.kryoSerializer = KryoSerializerForBatchMonitoring.class.getName(); //for debug
        //remoteConfig.kryoSerializer = KryoSerializerForBirch.class.getName(); //performance degrade

        remoteConfig.parseArgs(args);

        String defaultMailbox = RemoteConfig.CONF_CONTROL_MAILBOX + ".mailbox-type";

        system = remoteConfig.createSystem();
        List<String> rest = init(system, remoteConfig, remoteConfig.restArgs);

        setDefaultStatDir();

        return rest;
    }

    public static class KryoSerializerForBatchMonitoring extends KryoSerializerForBirch {
        protected KryoSerializerMonitoring.SerializeReport report;
        public KryoSerializerForBatchMonitoring(ExtendedActorSystem system) {
            super(system);
            report = new KryoSerializerMonitoring.SerializeReport();
        }
        @Override
        public byte[] toBinary(Object o) {
            byte[] data = super.toBinary(o);
            report.report(o, data == null ? 0 : data.length);
            return data;
        }

        @Override
        public void toBinary(Object o, ByteBuffer byteBuffer) {
            super.toBinary(o, byteBuffer);
            int total = byteBuffer.remaining();
            report.report(o, total);
        }
    }

    public static class KryoSerializerForBirch extends KryoSerializer {
        public KryoSerializerForBirch(ExtendedActorSystem system) {
            super(system);
        }

        @SuppressWarnings("unchecked")
        @Override
        public int registerType(Kryo kryo, Class<?> type, int n) {
            if (type.equals(ActorBirchRefNode.class)) {
                Registration r = kryo.register(type, n);
                r.setSerializer(new SerializerForRefNode((Serializer<ActorBirchRefNode>) r.getSerializer()));
                ++n;
                return n;
            } else {
                return super.registerType(kryo, type, n);
            }
        }

    }

    public static class SerializerForRefNode extends Serializer<ActorBirchRefNode> {
        Serializer<ActorBirchRefNode> original;

        public SerializerForRefNode(Serializer<ActorBirchRefNode> original) {
            this.original = original;
        }

        @Override
        public void write(Kryo kryo, Output output, ActorBirchRefNode object) {
            ClusteringFeature cf = object.clusteringFeature;
            object.clusteringFeature = null; //exclude CF from ref-node
            try {
                original.write(kryo, output, object);
            } finally {
                object.clusteringFeature = cf;
            }
        }

        @Override
        public ActorBirchRefNode read(Kryo kryo, Input input, Class<? extends ActorBirchRefNode> type) {
            return original.read(kryo, input, type);
        }
    }

    public List<String> init(ActorSystem system, RemoteConfig remoteConfig, List<String> args) {
        ActorBirchConfig.globalSystem = system;

        config = newConfig(system);
        batch = newBatchEx();

        StatSystem.system().setEnvelopeComparatorType(BirchMessages.BirchEnvelopeComparator.class.getName());

        batch.remoteConfig = remoteConfig;
        if (remoteConfig.isRemote()) {
            batch.remoteManager = RemoteManager.createManager(system, remoteConfig);
            batch.remoteManager.tell(new RemoteManagerMessages.SetConfig(config), ActorRef.noSender());
            try {
                Thread.sleep(500); //time for RemoteManager.instance = ...
            } catch (Exception ex) {
                ex.printStackTrace();
            }
        } else if (remoteConfig.stat) {
            batch.localStatVm = new StatVm();
            batch.localStatVm.saveRuntime(system, StatSystem.system().getInterval());
        }

        args = parseBirchActorArgs(args);

        List<String> ret;
        if (args.isEmpty()) {
            ret = args;
        } else {
            ret = batch.parseArgs(args);
        }
        config.setTraceIfDebug();
        return ret;
    }

    protected List<String> parseBirchActorArgs(List<String> args) {
        List<String> rest = new ArrayList<>(args.size());
        for (int i = 0; i < args.size(); ++i) {
            String arg = args.get(i);
            if (arg.endsWith("--terminate-after-output")) {
                terminateAfterOutput = true;
            } else if (arg.equals("--construct-wait-ms")) {
                ++i;
                batch.constructWaitMs = Long.valueOf(args.get(i));
            } else if (arg.equals("--construct-wait-retries-limit")) {
                ++i;
                batch.constructWaitRetriesLimit = Integer.valueOf(args.get(i));
            } else if (arg.equals("--finish-log-duration-ms")) {
                ++i;
                config.finishLogDuration = Duration.ofMillis(Long.valueOf(args.get(i)));
            } else if (arg.equals("--help")) {
                showHelp();
                rest.add(arg);
            } else {
                rest.add(arg);
            }
        }

        return rest;
    }

    public void showHelp() {
        System.out.println(String.join("\n",
                "actor-arguments:",
                "    --terminate-after-output",
                "    --construct-wait-ms <long> : default is 500",
                "    --construct-wait-retries-limit <int> : default is 10. -1 means unlimited",
                "    --finish-log-duration-ms <long> : default is 200."));
    }

    protected ActorBirchConfig newConfig(ActorSystem system) {
        return new ActorBirchConfig(system);
    }

    protected BirchBatchEx newBatchEx() {
        return new BirchBatchEx(config, system);
    }


    public void setDefaultStatDir() {
        if (batch.remoteConfig.stat && batch.remoteConfig.statDir == null) {
            Path statDir = batch.getOutDir().resolve("actor-stat");
            batch.remoteConfig.statDir = statDir.toString();
            StatSystem statSystem = StatSystem.system();
            statSystem.setDir(statDir);
            try {
                Files.createDirectories(statDir);
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }
        if (StatSystem.system().isEnabled()) {
            saveConfigToStatDir();
        }
    }

    protected void saveConfigToStatDir() {
        StatSystem statSystem = StatSystem.system();
        Path statDir = statSystem.getDir();
        Map<String,Object> json = new HashMap<>();
        json.put("remoteConfig", RemoteConfig.toJsonByIntrospection(batch.remoteConfig));
        json.put("birchConfig", config.toJson());
        JsonWriter.write(json, statDir.resolve(String.format("config-birch-%d.json",
                statSystem.getSystemId(system))));
    }


    public void terminate() {

        if (batch.remoteConfig.shutdownCluster && RemoteManager.instance != null) {
            batch.remoteManager.tell(new RemoteManagerMessages.Shutdown(), ActorRef.noSender());
        }
        if (batch.getRootChanger() != null) {
            batch.getRootChanger().tell(new BirchMessages.End(), ActorRef.noSender());
            try {
                Thread.sleep(10000);
            } catch (Exception ex) {
                ex.printStackTrace();
            }
        }
        config.terminate();
        StatMailbox.endGlobal();
    }

    public static class BirchBatchEx extends BirchBatch {
        public StatVm localStatVm;
        public RemoteConfig remoteConfig;
        public ActorRef remoteManager;
        protected ActorSystem system;
        protected ActorRef rootChanger;

        public long constructWaitMs = 500;
        public int constructWaitRetriesLimit = 10;

        protected Instant constructStartTime;
        protected InputThrottle inputThrottle;

        public long inputCount;

        public BirchBatchEx(ActorBirchConfig config, ActorSystem system) {
            super(config);
            purity = new BirchPurity() {
                @Override
                protected DendrogramPurityUnit initPurity(DendrogramPurityUnit.PurityInput input) {
                    return initCreatePurity(input);
                }
            };
            this.system = system;
            rootChanger = config.createRootChanger();
        }

        public DendrogramPurityUnit initCreatePurity(DendrogramPurityUnit.PurityInput input) {
            ActorDendrogramPurityUnit d = new ActorDendrogramPurityUnit(system, input);
            try {
                PrintWriter out = new PrintWriter(Files.newBufferedWriter(getOutDir().resolve("d-purity.txt")));
                d.setAdditionalOutput(out);
            } catch (Exception ex) {
                ex.printStackTrace();
            }
            return d;
        }

        public List<DataField> getFields() {
            return fields;
        }

        public Path getOutDir() {
            if (outDir == null) {
                return Paths.get(".");
            }
            return outDir;
        }

        public InputThrottle getInputThrottle() {
            return inputThrottle;
        }

        @Override
        public BirchNode construct(InputI input, List<DataField> fields, Iterable<CsvLine> lines) {
            constructStartTime = Instant.now();
            inputCount = 0;
            constructInitRoot(fields.size());
            if (inputThrottle != null) {
                inputThrottle.start(system);
            }
            input.makeDataPointStream()
                    .forEach(this::inputData);

            if (inputThrottle != null) {
                inputThrottle.stop();
            }
            constructWait(inputCount);
            return constructAfter();
        }

        //deprecated
        @Override
        public BirchNode construct(int dimension, Iterable<BirchNodeInput> points) {
            constructStartTime = Instant.now();
            constructInitRoot(dimension);
            inputCount = 0;
            if (inputThrottle != null) {
                inputThrottle.start(system);
            }
            points.forEach(this::inputData);
            if (inputThrottle != null) {
                inputThrottle.stop();
            }
            constructWait(inputCount);
            return constructAfter();
        }

        public void inputData(BirchNodeInput e) {
            rootChanger.tell(new BirchMessages.PutToRoot(e), ActorRef.noSender());
            if (inputThrottle != null) {
                inputThrottle.incrementInputCount();
            }
            ++inputCount;
        }

        public void constructInitRoot(int dimension) {
            inputThrottle = new InputThrottle(((ActorBirchConfig) config).getFinishActor());
            log("init throttle");
                // it will cause to create the finisher on the leader VM

            rootChanger.tell(new BirchMessages.ChangeRoot(
                    BirchMessages.toSerializable(((ActorBirchConfig) config), newRoot(dimension),
                            BirchMessages.SerializeCacheStrategy.None)), ActorRef.noSender());
        }

        public void constructWait(long inputCount) {
            Timeout timeout = Timeout.apply(5, TimeUnit.SECONDS);
            int tries = 0;
            long prevCount = -1;
            Instant time = Instant.now();
            Duration checkTime = Duration.ofSeconds(10);
            boolean regularFinish = false;
            while (true) {
                try {
                    Long count = (Long) PatternsCS.ask(((ActorBirchConfig) config).getFinishActor(),
                            new BirchMessages.GetFinishCount(), timeout)
                            .toCompletableFuture().get();
                    if (count == prevCount) {
                        ++tries;
                        Thread.sleep(constructWaitMs);
                    }
                    if (count >= inputCount) {
                        log(String.format("finish inputCount=%,d", count));
                        regularFinish = true;
                        break;
                    }
                    prevCount = count;
                } catch (Exception ex) {
                    log(String.format("fail getFinishCount: retries=%,d prevCount=%,d inputCount=%,d %s", tries, prevCount, inputCount, ex));
                    ++tries;
                }
                if (constructWaitRetriesLimit != -1 && tries >= constructWaitRetriesLimit) {
                    log(String.format("fail getFinishCount: retries=%,d prevCount=%,d inputCount=%,d", tries, prevCount, inputCount));
                    break;
                }

                Instant nextTime = Instant.now();
                Duration elapsed = Duration.between(time, nextTime);
                if (elapsed.compareTo(checkTime) > 0) {
                    log(String.format("construct-wait: elapsed %s: retries=%,d prevCount=%,d inputCount=%,d", elapsed, tries, prevCount, inputCount));
                    checkTime = checkTime.multipliedBy(2);
                }
            }

            setConstructTimeNow(constructStartTime);
            if (regularFinish) {
                try {
                    Instant finishTime = (Instant) PatternsCS.ask(((ActorBirchConfig) config).getFinishActor(),
                            new BirchMessages.GetFinishTime(), timeout)
                            .toCompletableFuture().get();
                    constructTime = Duration.between(constructStartTime, finishTime);
                } catch (Exception ex) {
                    log(String.format("fail obtaining finishTime. use current time: %s", ex));
                }
            }
        }

        public BirchNode constructAfter() {
            Timeout t = new Timeout(5, TimeUnit.MINUTES);
            try {
                return (BirchNode) PatternsCS.ask(rootChanger, new BirchMessages.GetRoot(), t).toCompletableFuture().get();
            } catch (Exception ex) {
                ex.printStackTrace();
                return null;
            }
        }

        @Override
        public DotGraph toDot(BirchNode node) {
            Path parentDir = parent(this.dotFile);
            if (!Files.exists(parentDir)) {
                try {
                    Files.createDirectories(parentDir);
                } catch (Exception ex) {
                    ex.printStackTrace();
                }
            }
            return new ActorBirchVisitorDot().run(system, node, dotFile);
        }

        public void log(String str) {
            System.err.println(str);
        }

        @Override
        public long countEntries(BirchNode e) {
            try {
                log("countEntries");
                return TraversalVisitActor.start(system, e,
                        new BirchMessages.BirchTraversalState<>(new CountEntriesVisitor()))
                        .get().getCount();
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }

        @Override
        public long countEntryPoints(BirchNode e) {
            try {
                log("countEntryPoints");
                return TraversalVisitActor.start(system, e,
                        new BirchMessages.BirchTraversalState<>(new CountEntryPointsVisitor()))
                        .get().getCount();
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }

        @Override
        public void save(BirchNode e, Path dir) {
            log("save " + dir);
            ActorBirchVisitorSave.run(system, e, summaryName, outNamePattern, dir.toString(), getInputForRead());
        }

        @Override
        public void saveSingle(BirchNode e, Path dir) {
            log("save single " + dir);
            ActorBirchVisitorSaveSingleFile.run(system, e, dir.resolve(this.outSingleName).toString());
        }

        @Override
        public Map<Integer, Long> getTreeDepthToNodes(BirchNode e) {
            try {
                log("getTreeDepthToNodes");
                return TraversalVisitActor.start(system, e,
                        new BirchMessages.BirchTraversalState<>(new SerializableCountDepthVisitor()))
                    .get().getDepthToNodes();
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }

        public static class SerializableCountDepthVisitor extends CountDepthVisitor implements Serializable {
        }

        public static Path parent(Path p) {
            return p == null ? Paths.get(".") :
                    (p.getParent() == null ? Paths.get(".") :
                        p.getParent());
        }

        @Override
        public void output() {
            long removed = fixChildIndex();
            this.root = constructAfter();
            long entries = countEntries(root);
            log(String.format("finish fixChildIndex : entries removed %,d -> %,d", removed, entries));

            super.output();
        }

        @Override
        public void outputPurity() {
            log("outSingleName: " + outSingleName);
            if (dotFile != null) {
                Path dotNoImage = parent(dotFile).resolve("no-image-" + dotFile.getFileName().toString());
                new ActorBirchVisitorDot(false).run(system, root, dotFile)
                        .save(dotNoImage.toFile());
            }

            boolean purityOn = !skipPurity && config.isTest();
            log("outputPurity: " + purityOn);
            if (purityOn) {
                purity.init();
                double p = purity.run(root);
                System.out.printf("DendrogramPurity : %f\n", p);
                calculatedPurity = p;
            }
        }

        @SuppressWarnings("unchecked")
        @Override
        public Map<String, Object> getTreeStats(BirchNode e) {
            Map<String,Object> json = super.getTreeStats(e);
            if (remoteConfig.isRemote()) {
                try {
                    Timeout timeout = new Timeout(2, TimeUnit.SECONDS);
                    List<RemoteManagerMessages.Status> ss = (List<RemoteManagerMessages.Status>)
                            PatternsCS.ask(remoteManager, new RemoteManagerMessages.GetStatusAll(), timeout)
                                    .toCompletableFuture().get(2, TimeUnit.SECONDS);

                    json.put("managerStatus", ss.stream()
                            .map(RemoteManagerMessages.Status::toJson)
                            .collect(Collectors.toList()));
                } catch (Exception ex) {
                    log(" failed treeStats " + ex);
                }
            }
            return json;
        }

        public ActorRef getRootChanger() {
            return rootChanger;
        }

        public ActorSystem getSystem() {
            return system;
        }

        public RemoteConfig getRemoteConfig() {
            return remoteConfig;
        }

        public StatVm getLocalStatVm() {
            return localStatVm;
        }

        @Override
        protected BirchNode newRoot(int dimension) {
            if (config.debug) {
                PutTraceJson t = new PutTraceJson(getOutDir().resolve("trace.jsonp"));
                BirchNodeCFTree.trace = t;
                traceRootExt = t;
                log("trace file: " + t.getFile());

            }
            return config.newRoot(dimension);
        }

        public Map<String,Object> statInconsistency() {
            log("statInconsistency");

            Instant countTime = Instant.now();
            long max = countEntries(constructAfter());
            log(String.format("   countEntries: %,d", max));
            Duration countDiff = Duration.between(countTime, Instant.now());
            long timeoutSec = Math.max(600, countDiff.getSeconds() * 20L);

            CompletableFuture<Map<String,Object>> result = new CompletableFuture<>();

            log(String.format("   start statInconsistency timeout=%,d secs", timeoutSec));

            ActorRef ref = system.actorOf(Props.create(StatInconsistencyFinisherActor.class, max, result)); //local-actor
            try {
                ActorBirchNodeActor.statInconsistencyStart((ActorBirchConfig) config, constructAfter(), null, 0, ref);

                return result.get(timeoutSec, TimeUnit.SECONDS);
            } catch (Exception ex) {
                log("statInconsistency error: " + ex);
                ex.printStackTrace();
                return new LinkedHashMap<>();
            } finally {
                ref.tell(PoisonPill.getInstance(), ActorRef.noSender());
            }
        }

        public long fixChildIndex() {
            log("fixChildIndex");

            Instant countTime = Instant.now();
            long max = countEntries(constructAfter());
            log(String.format("   countEntries: %,d", max));
            Duration countDiff = Duration.between(countTime, Instant.now());
            long timeoutSec = Math.max(600, countDiff.getSeconds() * 10L);

            CompletableFuture<Long> result = new CompletableFuture<>();

            log(String.format("   start fixChildIndex timeout=%,d secs", timeoutSec));

            ActorRef ref = system.actorOf(Props.create(FixFinisherActor.class, max, result)); //local-actor
            try {
                BirchNode root = constructAfter();
                ActorBirchNodeActor.fixChildIndexStart((ActorBirchConfig) config, root, root, 0, ref);

                return result.get(timeoutSec, TimeUnit.SECONDS);
            } catch (Exception ex) {
                log("fixChildIndex error: " + ex);
                ex.printStackTrace();
                return -1;
            } finally {
                ref.tell(PoisonPill.getInstance(), ActorRef.noSender());
            }
        }
    }

    public static class RootChangeCreator implements Creator<RootChangeActor>,
            RemoteManagerMessages.DispatcherSpecifiedCreator {
        protected ActorBirchConfig config;

        public RootChangeCreator(ActorBirchConfig config) {
            this.config = config;
        }

        @Override
        public RootChangeActor create() {
            return new RootChangeActor(config);
        }

        @Override
        public String getSpecifiedDispatcher() {
            return RemoteConfig.CONF_PINNED_DISPATCHER;
        }
    }

    public static class RootChangeActor extends AbstractActor {
        protected ActorBirchConfig config;
        protected BirchNode root;

        protected StatReceive statReceive;
        protected StatMailbox.StatQueue queue;

        protected int maxDepth;

        public RootChangeActor(ActorBirchConfig config) {
            this.config = config;
        }

        @Override
        public void aroundReceive(PartialFunction<Object, BoxedUnit> receive, Object msg) {
            if (statReceive != null){
                statReceive.receiveBefore(msg);
            }
            try {
                super.aroundReceive(receive, msg);
            } finally {
                if (statReceive != null) {
                    statReceive.receiveAfter(msg, "rootChangeActor");
                }
            }
        }

        @Override
        public void preStart() throws Exception {
            super.preStart();
            StatSystem statSystem = StatSystem.system();
            if (statSystem.isEnabled()) {
                statReceive = new StatReceive(statSystem.getId(
                        context().system(), self().path().toStringWithoutAddress()),
                        self().path());
            }
            getTraceRootExt().newRootChangeActor(this);
        }

        @Override
        public Receive createReceive() {
            return receiveBuilder()
                    .match(BirchMessages.ChangeRoot.class, this::changeRoot)
                    .match(BirchMessages.PutToRoot.class, this::putToRoot)
                    .match(BirchMessages.GetRoot.class, this::getRoot)
                    .match(BirchMessages.End.class, this::end)
                    .match(StatMessages.ActorId.class, this::actorId)
                    .build();
        }

        public void changeRoot(BirchMessages.ChangeRoot r) {
            getTraceRootExt().changeRoot(this, r.node);
            if (this.root != null && maxDepth >= r.maxDepth) {
                ActorBirchNodeActor.log(config, "illegal root maxDepth change: %d -> %d : %s", maxDepth, r.maxDepth, (r.put == null ? "null-put"  : r.put.path));
                changeRootIllegal(r);
            } else {
                root = r.node;
                if (config.debug) {
                    ActorBirchNodeActor.log(config, "changeRoot: %s, maxDepth: %d -> %d ", root, maxDepth, r.maxDepth);
                    ActorBirchNodeActor.log(config, "     root-parent: %s", root.getParent());
                    config.recordStackInfo("[changeRoot]");
                }
                this.maxDepth = r.maxDepth;
            }
        }

        public void changeRootIllegal(BirchMessages.ChangeRoot r) {
            if (root instanceof ActorBirchRefNode) {
                r.put.path.add(root);
                r.put.path = r.put.path.stream()
                        .map(ActorBirchNodeActor.BirchNodeInvalidPath::new)
                        .collect(Collectors.toList());
                r.put.maxDepth++;
                if (config.debug) {
                    config.recordStackInfo("AddNewNode");
                }
                ((ActorBirchRefNode) root).getRef().tell(
                        new BirchMessages.AddNewNode(r.put, r.node, r.node.getClusteringFeature(), null, null),
                        ActorRef.noSender());
            }
        }

        public void putToRoot(BirchMessages.PutToRoot r) {
            getTraceRootExt().putToRoot(this, r.entry, r.putCount);
            ArrayList<BirchNode> path = new ArrayList<>();
            path.add(BirchMessages.toSerializable(config, root, BirchMessages.SerializeCacheStrategy.Lazy, BirchMessages.debugCountPutToRoot));
            if (root instanceof ActorBirchRefNode) {
                BirchMessages.Put put = new BirchMessages.Put(path, r.entry, self());
                put.putCount = r.putCount;
                put.toRoot = true;
                if ((put.putCount % 100) == 0 && put.putCount > 0) {
                    ActorBirchNodeActor.log(config, "putToRoot %s, %s", put, root);
                }
                ((ActorBirchRefNode) root).put(put); //maybe REMOTE
            } else {
                root = root.put(r.entry); //maybe REMOTE
            }
        }

        public void getRoot(BirchMessages.GetRoot m) {
            sender().tell(root, self());
        }

        public void actorId(StatMessages.ActorId m) {
            this.queue = m.queue;
            if (queue != null && queue.getId() != -1 && statReceive != null) {
                queue.setId(statReceive.getId());
            }
        }

        public void end(BirchMessages.End m) {
            ActorBirchNodeActor.endTree(root);

            if (statReceive != null){
                statReceive.saveTotal();
            }

            self().tell(PoisonPill.getInstance(), self());
        }
    }

    public static class StatInconsistencyFinisherActor extends AbstractActor {
        CompletableFuture<Map<String,Object>> result;
        long finishCount;
        long max;

        long inconsistentParents;
        long inconsistentChildIndices;
        long actors;
        long refNodes;

        long actorAndRefNodes; //must be 0
        long actorAndEntries;

        public StatInconsistencyFinisherActor(long max, CompletableFuture<Map<String,Object>> result) {
            this.result = result;
            this.max = max;
        }

        @Override
        public Receive createReceive() {
            return receiveBuilder()
                    .match(BirchMessages.StatInconsistencyReport.class, this::statInconsistencyReport)
                    .build();
        }

        public void statInconsistencyReport(BirchMessages.StatInconsistencyReport m) {
            if (m.inconsistentParent) {
                inconsistentParents++;
            }
            if (m.inconsistentChildIndex) {
                inconsistentChildIndices++;
            }
            if (m.actor) {
                actors++;
            }
            if (m.refNode) {
                refNodes++;
            }
            if (m.actor && m.refNode) {
                actorAndRefNodes++;
            }
            if (m.entry) {
                finishCount++;
            }
            if (m.actor && m.entry) {
                actorAndEntries++;
            }
            if (finishCount >= max) {
                Map<String,Object> map = new LinkedHashMap<>();
                map.put("inconsistentParents", inconsistentParents);
                map.put("inconsistentChildIndices", inconsistentChildIndices);
                map.put("actors", actors);
                map.put("refNodes", refNodes);
                map.put("entries", finishCount);
                map.put("actorAndEntries", actorAndEntries);
                map.put("actorAndRefNodes", actorAndRefNodes);
                result.complete(map);
            }
        }
    }

    public static class FixFinisherActor extends AbstractActor {
        CompletableFuture<Long> result;
        long max;
        long finishCount;

        long reducedChildSize;

        public FixFinisherActor(long max, CompletableFuture<Long> result) {
            this.result = result;
            this.max = max;
        }

        @Override
        public Receive createReceive() {
            return receiveBuilder()
                    .match(BirchMessages.FixChildIndexReport.class, this::fixChildIndex)
                    .build();
        }

        public void fixChildIndex(BirchMessages.FixChildIndexReport m) { //from bottom entry
            if (m.entry) {
                ++finishCount;
            }
            reducedChildSize += m.reducedChildSize;
            if (finishCount >= max) {
                result.complete(reducedChildSize);
            }
        }
    }

    public static class InputThrottle {
        ActorRef finisher;
        long inputCount;
        long inputCountLastCheckPoint;
        long finishedCountLastCheckPoint;

        volatile scala.concurrent.duration.Duration throttle = FiniteDuration.Zero();
        Cancellable checkTask;

        static long oneMilliNano = FiniteDuration.apply(1, TimeUnit.MILLISECONDS).toNanos();
        static FiniteDuration minThrottle = FiniteDuration.apply(10, TimeUnit.MILLISECONDS);
        static FiniteDuration maxThrottle = FiniteDuration.apply(2, TimeUnit.MINUTES);

        public InputThrottle(ActorRef finisher) {
            this.finisher = finisher;
        }

        public synchronized void start(ActorSystem system) {
            if (checkTask == null) {
                FiniteDuration scheduleTime = FiniteDuration.apply(1, TimeUnit.SECONDS);
                checkTask = system.scheduler().schedule(scheduleTime, scheduleTime, this::check, system.dispatcher());
            }
        }

        public synchronized void stop() {
            System.err.println(String.format("#InputThrottle stop: throttle=%s, inputCount=%,d, finishedCountLastCheckPoint=%,d",
                    throttle, inputCount, finishedCountLastCheckPoint));
            if (checkTask != null) {
                checkTask.cancel();
                checkTask = null;

            }
        }

        public /*synchronized*/ void incrementInputCount() {
            ++inputCount;
            sleep();
        }

        public void sleep() {
            scala.concurrent.duration.Duration throttle = this.throttle;
            if (!throttle.equals(FiniteDuration.Zero())) {
                long ms = throttle.toMillis();
                int nano = (int) (throttle.toNanos() % oneMilliNano);
                try {
                    Thread.sleep(ms, nano);
                } catch (Exception ex) {
                    ex.printStackTrace();
                }
            }
        }

        public synchronized void check() {
            long inputCountIncreased = inputCount - inputCountLastCheckPoint;
            long finishedCount = getFinishedCount();
            long finishedCountIncreased = finishedCount - finishedCountLastCheckPoint;

            double max = 100;
            double p;
            if (finishedCountIncreased == 0) {
                if (inputCountIncreased == 0) {
                    p = 0;
                } else {
                    p = max;
                }
            } else {
                p = Math.min(max, (inputCountIncreased / (double) (finishedCountIncreased * 2L)));
            }

            scala.concurrent.duration.Duration prevThrottle = throttle;
            if (throttle.equals(FiniteDuration.Zero())) {
                if (p > 1) {
                    throttle = FiniteDuration.apply(10, TimeUnit.MILLISECONDS)
                            .mul(p);
                }
            } else {
                throttle = throttle.mul(p);
                if (throttle.compare(FiniteDuration.apply(10, TimeUnit.MILLISECONDS)) < 0) {
                    throttle = FiniteDuration.Zero();
                }
            }

            if (minThrottle.compareTo(throttle) > 0) {
                throttle = FiniteDuration.Zero();
            } else if (throttle.compareTo(maxThrottle) > 0) {
                throttle = maxThrottle;
            }
            if (!prevThrottle.equals(throttle)) {
                System.err.println(String.format("#InputThrottle: throttle=%s, inputCountIncreased=%,d, finishedCountIncreased=%,d, p=%f",
                        throttle, inputCountIncreased, finishedCountIncreased, p));
            }

            inputCountLastCheckPoint = inputCount;
            finishedCountLastCheckPoint = finishedCount;
        }

        public long getFinishedCount() {
            Timeout timeout = Timeout.apply(FiniteDuration.apply(10, TimeUnit.SECONDS));
            try {
                return (long) PatternsCS.ask(finisher, new BirchMessages.GetFinishCount(), timeout)
                        .toCompletableFuture().get();
            } catch (Exception ex) {
                System.err.println("#InputThrottle getFinishedCount error: " + ex);
                return -1;
            }
        }
    }

    static PutTraceRootExtension traceRootExt = new PutTraceRootExtensionEmpty();

    public static PutTraceRootExtension getTraceRootExt() {
        return traceRootExt;
    }

    public interface PutTraceRootExtension {
        void newRootChangeActor(RootChangeActor rootChanger);
        void changeRoot(RootChangeActor rootChangeActor, BirchNode newRoot);
        void putToRoot(RootChangeActor rootChangeActor, BirchNodeInput input, int putCount);
        /** set the context of subsequent trace calls */
        void setNextNodeActor(ActorBirchNodeActor nodeActor);

        void putEntry(ActorBirchNodeActor nodeActor, List<BirchNode> path, BirchNodeInput input);

        void getChildrenCfs(ActorBirchNodeActor childNodeActor, int nextIndex, List<BirchNode> children,
                            BirchNodeInput input, BirchMessages.GetChildrenCfsPostProcess postProcess,
                            BirchNode parent);

        void putWithChildrenCfs(ActorBirchNodeActor nodeActor, List<BirchNode> path, BirchNodeInput input,
                                List<BirchMessages.NodeAndCf> collected);

        void splitByNewEntry(ActorBirchNodeActor nodeActor, List<BirchNode> path, BirchNodeInput input,
                             List<BirchMessages.NodeAndCf> collected);

        void addNewNode(ActorBirchNodeActor nodeActor, List<BirchNode> path, BirchNodeInput input,
                        BirchNode newNode, ClusteringFeature cf);

        void addNewNodeEntry(ActorBirchNodeActor nodeActor, List<BirchNode> path, BirchNodeInput input, List<BirchMessages.NodeAndCf> children);
        void addNewNodeToLink(ActorBirchNodeActor nodeActor, List<BirchNode> path, BirchNodeInput input, BirchNode newNode, ClusteringFeature cf, int count);

        void splitByNewNode(ActorBirchNodeActor nodeActor, List<BirchNode> path, BirchNodeInput input,
                             List<BirchMessages.NodeAndCf> collected, BirchNode newNode);

    }

    public static class PutTraceRootExtensionEmpty implements PutTraceRootExtension {
        @Override
        public void newRootChangeActor(RootChangeActor rootChanger) { }
        @Override
        public void changeRoot(RootChangeActor rootChangeActor, BirchNode newRoot) { }
        @Override
        public void putToRoot(RootChangeActor rootChangeActor, BirchNodeInput input, int putCount) { }
        @Override
        public void setNextNodeActor(ActorBirchNodeActor nodeActor) { }
        @Override
        public void putEntry(ActorBirchNodeActor nodeActor, List<BirchNode> path, BirchNodeInput input) { }
        @Override
        public void getChildrenCfs(ActorBirchNodeActor childNodeActor, int nextIndex, List<BirchNode> children, BirchNodeInput input, BirchMessages.GetChildrenCfsPostProcess postProcess, BirchNode parent) { }
        @Override
        public void putWithChildrenCfs(ActorBirchNodeActor nodeActor, List<BirchNode> path, BirchNodeInput input, List<BirchMessages.NodeAndCf> collected) { }
        @Override
        public void splitByNewEntry(ActorBirchNodeActor nodeActor, List<BirchNode> path, BirchNodeInput input, List<BirchMessages.NodeAndCf> collected) { }
        @Override
        public void addNewNode(ActorBirchNodeActor nodeActor, List<BirchNode> path, BirchNodeInput input, BirchNode newNode, ClusteringFeature cf) { }
        @Override
        public void addNewNodeEntry(ActorBirchNodeActor nodeActor, List<BirchNode> path, BirchNodeInput input, List<BirchMessages.NodeAndCf> children) { }
        @Override
        public void addNewNodeToLink(ActorBirchNodeActor nodeActor, List<BirchNode> path, BirchNodeInput input, BirchNode newNode, ClusteringFeature cf,
                                     int count) {}
        @Override
        public void splitByNewNode(ActorBirchNodeActor nodeActor, List<BirchNode> path, BirchNodeInput input, List<BirchMessages.NodeAndCf> collected, BirchNode newNode) { }
    }



    public static class PutTraceJson extends BirchNodeCFTree.PutTrace implements PutTraceRootExtension {
        protected Path file;
        protected ActorBirchNodeActor nodeActor;

        public static final String newRootChangeActor = "newRootChangeActor";
        public static final String changeRoot = "changeRoot";
        public static final String putToRoot = "putToRoot";
        public static final String putStart = "putStart";
        public static final String putEntry = "putEntry";
        public static final String getChildrenCfs = "getChildrenCfs";
        public static final String putWithChildrenCfs = "putWithChildrenCfs";
        public static final String splitByNewEntry = "splitByNewEntry";
        public static final String addNewNode = "addNewNode";
        public static final String addNewNodeToLink = "addNewNodeToLink";
        public static final String addNewNodeEntry = "addNewNodeEntry";
        public static final String splitByNewNode = "splitByNewNode";
        public static final String findClosestChild = "findClosestChild";
        public static final String finishWithAddEntry = "finishWithAddEntry";
        public static final String finishWithAddNodeTree = "finishWithAddNodeTree";
        public static final String finishWithAddNodeEntry = "finishWithAddNodeEntry";
        public static final String split = "split";
        public static final String newRoot = "newRoot";


        public PutTraceJson(Path file) {
            this.file = file;
        }

        public Path getFile() {
            return file;
        }

        public Map<String,Object> newMap(String eventName) {
            if (eventName != null) {
                Map<String,Object> m = new LinkedHashMap<>();
                m.put("type", eventName);
                return m;
            } else {
                return new TreeMap<>();
            }
        }

        public void save(Map<String,Object> o) {
            try {
                Path file = getFile();
                Writer w = Files.newBufferedWriter(file,
                        StandardOpenOption.APPEND,
                        StandardOpenOption.CREATE,
                        StandardOpenOption.WRITE);
                JsonWriter jsonWriter = new JsonWriter(w).withNewLines(false);
                jsonWriter.write(o);
                w.write('\n');
                w.close();
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }

        public String pathToJson(AbstractActor a) {
            return a == null ? "null" : "ref:" + a.getSelf().path().toSerializationFormat();
        }

        public String nodeToString(BirchNode node) {
            if (node == null) {
                return "null";
            } else if (node instanceof ActorBirchRefNode) {
                return "ref:" + ((ActorBirchRefNode) node).getRef().path().toSerializationFormat();
            } else if (node instanceof ActorBirchNodeActor.BirchNodeInvalidPath) {
                return "invalid:" + nodeToString(((ActorBirchNodeActor.BirchNodeInvalidPath) node).getOriginal());
            } else if (node instanceof BirchNodeInput) {
                return "input:" + ((BirchNodeInput) node).getId();
            } else {
                return "node:" + node.getClass().getSimpleName() +
                        "@" + Integer.toHexString(System.identityHashCode(node));
            }
        }

        public Object nodesToJson(List<BirchNode> path) {
            if (path == null) {
                return Collections.emptyList();
            } else {
                return path.stream()
                        .map(this::nodeToString)
                        .collect(Collectors.toList());
            }
        }

        public Object nodeCfsToJson(List<BirchMessages.NodeAndCf> ns) {
            if (ns == null) {
                return Collections.emptyList();
            } else {
                return ns.stream()
                        .map(this::nodeCfToJson)
                        .collect(Collectors.toList());
            }
        }

        public Object nodeCfToJson(BirchMessages.NodeAndCf cf) {
            Map<String,Object> m = new LinkedHashMap<>();
            m.put("node", nodeToString(cf.node));
            m.put("cf", cfToJson(cf.cf));
            m.put("index", cf.index);
            return m;
        }

        public Object cfToJson(ClusteringFeature cf) {
            return Arrays.asList(cf.getEntries(), cf.getSquareSum());
        }



        @Override
        public void newRootChangeActor(RootChangeActor rootChanger) {
            Map<String,Object> m = newMap(newRootChangeActor);
            m.put("rootChanger", pathToJson(rootChanger));
            save(m);
        }

        @Override
        public void changeRoot(RootChangeActor rootChangeActor, BirchNode newRoot) {
            Map<String,Object> m = newMap(changeRoot);
            m.put("rootChanger", pathToJson(rootChangeActor));
            m.put("root", nodeToString(newRoot));
            save(m);
        }

        @Override
        public void putToRoot(RootChangeActor rootChangeActor, BirchNodeInput input, int putCount) {
            if (putCount == 0 || (putCount % 100) == 0) { //avoid flooding
                Map<String, Object> m = newMap(putToRoot);
                m.put("rootChanger", pathToJson(rootChangeActor));
                m.put("input", nodeToString(input));
                m.put("putCount", putCount);
                save(m);
            }
        }

        @Override
        public void setNextNodeActor(ActorBirchNodeActor nodeActor) {
            this.nodeActor = nodeActor;
        }

        @Override
        public void putStart(BirchNode node, BirchNodeInput input) {
            Map<String,Object> m = newMap(putStart);
            m.put("this", pathToJson(nodeActor));
            m.put("input", nodeToString(input));
            save(m);
        }

        @Override
        public void putEntry(ActorBirchNodeActor nodeActor, List<BirchNode> path, BirchNodeInput input) {
            Map<String,Object> m = newMap(putEntry);
            m.put("this", pathToJson(nodeActor));
            m.put("input", nodeToString(input));
            m.put("path", nodesToJson(path));
            save(m);
        }

        @Override
        public void getChildrenCfs(ActorBirchNodeActor childNodeActor, int nextIndex, List<BirchNode> children, BirchNodeInput input, BirchMessages.GetChildrenCfsPostProcess postProcess, BirchNode parent) {
            Map<String,Object> m = newMap(getChildrenCfs);
            m.put("this", pathToJson(nodeActor));
            m.put("index", nextIndex);
            m.put("children", nodesToJson(children));
            m.put("postProcess", postProcess.name());
            m.put("input", nodeToString(input));
            m.put("parent", nodeToString(parent));
            save(m);
        }

        @Override
        public void putWithChildrenCfs(ActorBirchNodeActor nodeActor, List<BirchNode> path, BirchNodeInput input, List<BirchMessages.NodeAndCf> collected) {
            Map<String,Object> m = newMap(putWithChildrenCfs);
            m.put("this", pathToJson(nodeActor));
            m.put("input", nodeToString(input));
            m.put("path", nodesToJson(path));
            m.put("collected", nodeCfsToJson(collected));
            save(m);
        }

        @Override
        public void splitByNewEntry(ActorBirchNodeActor nodeActor, List<BirchNode> path, BirchNodeInput input, List<BirchMessages.NodeAndCf> collected) {
            Map<String,Object> m = newMap(splitByNewEntry);
            m.put("this", pathToJson(nodeActor));
            m.put("input", nodeToString(input));
            m.put("path", nodesToJson(path));
            m.put("collected", nodeCfsToJson(collected));
            save(m);
        }

        @Override
        public void addNewNode(ActorBirchNodeActor nodeActor, List<BirchNode> path, BirchNodeInput input, BirchNode newNode, ClusteringFeature cf) {
            Map<String,Object> m = newMap(addNewNode);
            m.put("this", pathToJson(nodeActor));
            m.put("input", nodeToString(input));
            m.put("path", nodesToJson(path));
            m.put("newNode", nodeToString(newNode));
            m.put("cf", cfToJson(cf));
            save(m);
        }

        @Override
        public void addNewNodeToLink(ActorBirchNodeActor nodeActor, List<BirchNode> path, BirchNodeInput input, BirchNode newNode, ClusteringFeature cf,
                                     int count) {
            Map<String,Object> m = newMap(addNewNodeToLink);
            m.put("this", pathToJson(nodeActor));
            m.put("input", nodeToString(input));
            m.put("path", nodesToJson(path));
            m.put("newNode", nodeToString(newNode));
            m.put("cf", cfToJson(cf));
            m.put("count", count);
            save(m);
        }

        @Override
        public void addNewNodeEntry(ActorBirchNodeActor nodeActor, List<BirchNode> path, BirchNodeInput input, List<BirchMessages.NodeAndCf> children) {
            Map<String,Object> m = newMap(addNewNodeEntry);
            m.put("this", pathToJson(nodeActor));
            m.put("input", nodeToString(input));
            m.put("path", nodesToJson(path));
            m.put("collected", nodeCfsToJson(children));
            save(m);
        }

        @Override
        public void splitByNewNode(ActorBirchNodeActor nodeActor, List<BirchNode> path, BirchNodeInput input, List<BirchMessages.NodeAndCf> collected, BirchNode newNode) {
            Map<String,Object> m = newMap(splitByNewNode);
            m.put("this", pathToJson(nodeActor));
            m.put("input", nodeToString(input));
            m.put("path", nodesToJson(path));
            m.put("collected", nodeCfsToJson(collected));
            m.put("newNode", nodeToString(newNode));
            save(m);
        }

        @Override
        public void findClosestChild(BirchNode node, BirchNode selected, int childIndex, double selectedD, BirchNodeInput input) {
            Map<String,Object> m = newMap(findClosestChild);
            m.put("this", pathToJson(nodeActor));
            m.put("input", nodeToString(input));
            m.put("childIndex", childIndex);
            m.put("selectedD", Double.isInfinite(selectedD) ? "Infinity" :
                                Double.isNaN(selectedD) ? "NaN" :
                                        selectedD);
            save(m);

        }

        @Override
        public void finishWithAddEntry(BirchNode node, BirchNodeInput input) {
            Map<String,Object> m = newMap(finishWithAddEntry);
            m.put("this", pathToJson(nodeActor));
            m.put("input", nodeToString(input));
            m.put("node", nodeToString(node));
            save(m);
        }

        @Override
        public void finishWithAddNodeTree(BirchNode node, BirchNode target, BirchNode treeNode, BirchNodeInput input) {
            Map<String,Object> m = newMap(finishWithAddNodeTree);
            m.put("this", pathToJson(nodeActor));
            m.put("input", nodeToString(input));
            m.put("node", nodeToString(node));
            m.put("target", nodeToString(target));
            m.put("treeNode", nodeToString(treeNode));
            save(m);
        }

        @Override
        public void finishWithAddNodeEntry(BirchNode node, BirchNode target, BirchNodeInput input) {
            Map<String,Object> m = newMap(finishWithAddNodeEntry);
            m.put("this", pathToJson(nodeActor));
            m.put("input", nodeToString(input));
            m.put("node", nodeToString(node));
            m.put("target", nodeToString(target));
            save(m);

        }

        @Override
        public void split(BirchNode parent, BirchNode newNode, List<BirchNode> left, List<BirchNode> right, BirchNodeInput input) {
            Map<String,Object> m = newMap(split);
            m.put("this", pathToJson(nodeActor));
            m.put("input", nodeToString(input));
            m.put("parent", nodeToString(parent));
            m.put("newNode", nodeToString(newNode));
            m.put("left", nodesToJson(left));
            m.put("right", nodesToJson(right));
            save(m);
        }

        @Override
        public void newRoot(BirchNode nr, BirchNode left, BirchNode right, BirchNodeInput input) {
            Map<String,Object> m = newMap(newRoot);
            m.put("this", pathToJson(nodeActor));
            m.put("newRoot", nodeToString(nr));
            m.put("input", nodeToString(input));
            m.put("left", nodeToString(left));
            m.put("right", nodeToString(right));
            save(m);

        }
    }
}
