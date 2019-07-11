package csl.dataproc.actor.vfdt;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.pattern.PatternsCS;
import akka.util.Timeout;
import csl.dataproc.actor.remote.RemoteConfig;
import csl.dataproc.actor.remote.RemoteManager;
import csl.dataproc.actor.remote.RemoteManagerMessages;
import csl.dataproc.actor.stat.StatSystem;
import csl.dataproc.actor.stat.StatVm;
import csl.dataproc.csv.CsvLine;
import csl.dataproc.csv.CsvReaderI;
import csl.dataproc.csv.arff.ArffAttributeType;
import csl.dataproc.dot.DotGraph;
import csl.dataproc.tgls.util.JsonWriter;
import csl.dataproc.vfdt.VfdtBatch;
import csl.dataproc.vfdt.VfdtNode;
import csl.dataproc.vfdt.VfdtNodeSplit;

import java.io.Serializable;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class ActorVfdtBatch {
    protected ActorSystem system;
    protected ActorVfdtConfig config;
    protected VfdtBatchEx batch;

    public static VfdtTestSession testSession;

    public static void main(String[] args) {
        new ActorVfdtBatch().run(args);
    }

    public void run(String... args) {
        List<String> rest = init(Arrays.asList(args));
        if (rest.contains("--help")) {
            system.terminate();
            return;
        }
        if (!rest.isEmpty()) {
            System.err.println("unknown arguments: " + rest);
        }
        batch.learn(); //test will be caused after the learning
    }

    public ActorSystem getSystem() {
        return system;
    }

    public ActorVfdtConfig getConfig() {
        return config;
    }

    public VfdtBatch getBatch() {
        return batch;
    }


    public List<String> init(List<String> args) {
        RemoteConfig remoteConfig = RemoteConfig.create(args);

        system = remoteConfig.createSystem();
        List<String> rest = init(system, remoteConfig, remoteConfig.restArgs);

        setDefaultStatDir();

        initStageActor();

        return rest;
    }

    public List<String> init(ActorSystem system, RemoteConfig remoteConfig, List<String> args) {
        ActorVfdtConfig.globalSystem = system;

        config = newConfig(system);
        batch = newBatchEx();

        batch.remoteConfig = remoteConfig;
        if (remoteConfig.isRemote()) {
            batch.remoteManager = RemoteManager.createManager(system, remoteConfig);
            batch.remoteManager.tell(new RemoteManagerMessages.SetConfig(config), ActorRef.noSender());
            try {
                Thread.sleep(500); //time for RemoteManager.instance = ...
            } catch (Exception ex) {
                ex.printStackTrace();
            }
            batch.log("remote-manager: " + batch.remoteManager + " : instance=" + RemoteManager.instance);
        } else if (remoteConfig.stat) {
            batch.localStatVm = new StatVm();
            batch.localStatVm.saveRuntime(system, StatSystem.system().getInterval());
        }

        if (args.isEmpty()) {
            return args;
        } else {
            return batch.parseArgs(args);
        }
    }

    protected ActorVfdtConfig newConfig(ActorSystem system) {
        return new ActorVfdtConfig(system);
    }

    protected VfdtBatchEx newBatchEx() {
        return new VfdtBatchEx(config);
    }

    public void initStageActor() {
        if (batch.neglectTime < 0) {
            ActorVfdtBatchStageActor.setup(system, batch);
        } else {
            ActorVfdtBatchStageActor.setupNeglect(system, batch);
        }
    }

    public void setDefaultStatDir() {
        if (batch.remoteConfig.stat && batch.remoteConfig.statDir == null) {
            Path statDir = batch.getOutputDir().resolve("actor-stat");
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
        json.put("vfdtConfig", RemoteConfig.toJsonByIntrospection(config));
        JsonWriter.write(json, statDir.resolve(String.format("config-vfdt-%d.json", statSystem.getSystemId(system))));
    }

    public static class VfdtTestSession implements RemoteManager.SharedConfig, Serializable {
        public List<ArffAttributeType> types;

        public VfdtTestSession() { }

        public VfdtTestSession(List<ArffAttributeType> types) {
            this.types = types;
            testSession = this;
        }

        @Override
        public void arrivedAtRemoteNode() {
            testSession = this;
        }
    }

    public static class VfdtBatchEx extends VfdtBatch {
        public StatVm localStatVm;
        public ActorRef remoteManager;
        public RemoteConfig remoteConfig;
        public ActorRef stageRef;
        public ActorRef stageEndRef;

        public double neglectTime = -1;

        public Instant learnStart = Instant.now();
        public Instant testStart = Instant.now();

        public VfdtBatchEx(ActorVfdtConfig config) {
            super(config);
        }

        @Override
        public List<String> parseArgs(List<String> args) {
            List<String> rest = super.parseArgs(args);
            if (rest.contains("--help")) {
                return rest;
            }

            List<String> rest2 = new ArrayList<>();
            for (int i = 0; i < rest.size(); ++i) {
                String arg = rest.get(i);
                if (arg.equals("--neglect")) {
                    ++i;
                    neglectTime = Double.valueOf(rest.get(i));
                } else {
                    rest2.add(arg);
                }
            }

            return rest2;
        }

        @Override
        public void setTypesFromSpec() {
            super.setTypesFromSpec();
            testSession = new VfdtTestSession(getTypes());
            if (remoteConfig.isRemote()) {
                remoteManager.tell(new RemoteManagerMessages.SetConfig(testSession), ActorRef.noSender());
            }
        }

        @Override
        public void learn(Iterable<? extends CsvLine> input) {
            learnStart = Instant.now();
            log("start learn");
            long i = 0;
            this.rootInit();
            for (CsvLine line : input) {
                if (line.isEmptyLine() || line.isEnd()) {
                    continue;
                }
                ActorVfdtDataLine d = new ActorVfdtDataLine(VfdtNode.parse(getTypes(), line), stageRef);
                put(d);
                ++i;
            }
            log("after learn input: " + i);
            if (stageEndRef != null) {
                stageEndRef.tell(new VfdtMessages.LearnEnd(i), ActorRef.noSender());
            }
        }

        public void rootInit() {
            if (root == null) {
                root = config.newRoot(types);
            }
        }
        public void put(VfdtNode.DataLine dataLine) {
            root = root.put(dataLine);
            ++learnInstanceCount;
        }

        @Override
        public void test(Path path) {
            testStart = Instant.now();
            log("test " + path);

            long i = 0;
            try (CsvReaderI<? extends CsvLine> reader = open(path)) {
                for (CsvLine line : reader) {
                    if (line.isEmptyLine() || line.isEnd()) {
                        continue;
                    }
                    ActorVfdtDataLine d = new ActorVfdtDataLine(VfdtNode.parse(getTypes(), line), stageRef);
                    getRoot().stepClassify(d);
                    ++i;
                }
            } catch (Exception ex) {
                ex.printStackTrace();
            }
            log("after test input: " + i);
            if (stageEndRef != null) {
                stageEndRef.tell(new VfdtMessages.ClassifyEnd(i), ActorRef.noSender());
            }
        }


        public static Path parent(Path p) {
            return p == null ? Paths.get(".") :
                    (p.getParent() == null ? Paths.get(".") :
                            p.getParent());
        }

        @Override
        public DotGraph toDot() {
            Path parentDir = parent(this.getDotFile());
            if (!Files.exists(parentDir)) {
                try {
                    Files.createDirectories(parentDir);
                } catch (Exception ex) {
                    ex.printStackTrace();
                }
            }
            return new ActorVfdtVisitorDot().run(system(), getRoot(), this.getDotFile());
        }

        protected ActorSystem system() {
            return ((ActorVfdtConfig) getConfig()).getSystem();
        }

        public void setLearnTime(Duration time) {
            learnTime = time;
        }

        public void setTestTime(Duration time) {
            testTime = time;
        }

        public void setTotalTime(Duration time) {
            totalTime = time;
        }

        public void setLearnTimeFromStart(Instant to) {
            setLearnTime(Duration.between(learnStart, to));
        }

        public void setTestTimeFromStart(Instant to) {
            setTestTime(Duration.between(testStart, to));
        }

        public void setTotalTimeFromStart(Instant to) {
            setTotalTime(Duration.between(learnStart, to));
        }

        @Override
        public void output(Path dir) {
            if (!Files.exists(dir)) {
                try {
                    Files.createDirectories(dir);
                } catch (Exception ex) {
                    ex.printStackTrace();
                }
            }
            if (experiment) {
                statMap = ActorVfdtVisitorStat.runExperiment(system(), dir, getRoot());
            } else {
                statMap = ActorVfdtVisitorStat.run(system(), dir, getRoot());
            }
            outputAnswerStats(dir);
            outputTreeStats(getRoot(), dir);
        }

        @SuppressWarnings("unchecked")
        @Override
        public Map<String, Object> getTreeStats(VfdtNode e) {
            Map<String, Object> json = super.getTreeStats(e);
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
                    log(" failed finishing-actor " + ex);
                }
            }
            return json;
        }

        @Override
        public Map<Integer, Long> getTreeDepthToNodes(VfdtNode e) {
            return ActorVfdtVisitorCountDepth.run(system(), e);
        }

        public void log(String str) {
            RemoteManager m = RemoteManager.instance;
            if (m != null) {
                m.log(str);
            } else {
                System.err.println("[" + LocalDateTime.now() + "] " + str);
            }
        }

        public void endRoot() {
            end(getRoot());
        }
    }

    public static void end(VfdtNode node) {
        if (node instanceof ActorVfdtRefNode) {
            ((ActorVfdtRefNode) node).end();
        } else if (node instanceof VfdtNodeSplit) {
            ((VfdtNodeSplit) node).getChildNodes()
                    .forEach(ActorVfdtBatch::end);
        }
    }

}
