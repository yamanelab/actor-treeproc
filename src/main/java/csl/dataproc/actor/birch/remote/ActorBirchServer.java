package csl.dataproc.actor.birch.remote;

import akka.actor.*;
import akka.pattern.PatternsCS;
import akka.util.Timeout;
import csl.dataproc.actor.birch.ActorBirchBatch;
import csl.dataproc.actor.birch.ActorBirchConfig;
import csl.dataproc.actor.birch.ActorBirchVisitorSaveSingleFile;
import csl.dataproc.actor.birch.BirchMessages;
import csl.dataproc.actor.remote.RemoteManagerMessages;
import csl.dataproc.birch.BirchBatch;
import csl.dataproc.birch.BirchConfig;
import csl.dataproc.birch.BirchNodeInput;
import csl.dataproc.csv.CsvIO;
import csl.dataproc.csv.CsvLine;
import csl.dataproc.csv.CsvReader;
import csl.dataproc.tgls.util.JsonWriter;

import java.io.StringReader;
import java.io.Writer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class ActorBirchServer extends ActorBirchBatch {
    public static void main(String[] args) {
        new ActorBirchServer().run(args);
    }

    protected ActorRef serverRef;

    @Override
    public void run(String... args) {
        List<String> restArgs = init(Arrays.asList(args));
        if (restArgs.contains("--help")) {
            system.terminate();
            return;
        }

        Path outDir = getBatch().getOutDir();
        if (outDir != null && !Files.exists(outDir)) {
            try {
                Files.createDirectories(outDir);
            } catch (Exception ex) {
                System.err.println("creating output dir: " + outDir + " " + ex);
            }
        }

        Address addr = system.provider().getDefaultAddress();
        serverRef = system.actorOf(Props.create(ServerActor.class, () -> new ServerActor(this)), "server"); //local creation
        System.out.println("### serverRef: " + serverRef.path().toStringWithAddress(addr));
    }

    public void constructInit() {
        batch.constructInitRoot(batch.getFields().size());
    }

    public Stream<BirchNodeInput> getInputs(Iterable<CsvLine> lines) {
        return batch.getInputForRead().makeDataPointStream(lines);
    }

    @Override
    protected ActorBirchConfig newConfig(ActorSystem system) {
        return new ActorBirchServerConfig(system);
    }

    @Override
    protected BirchBatchEx newBatchEx() {
        return new BirchBatchExServer(config, system);
    }

    public static class BirchBatchExServer extends BirchBatchEx {
        protected InputWithIndex input;
        protected Writer fileIndexWriter;

        public BirchBatchExServer(ActorBirchConfig config, ActorSystem system) {
            super(config, system);
        }

        @Override
        public InputI getInput(String input) {
            if (this.input == null) {
                this.input = new InputWithIndex(input, this.charset.name(), new CsvIO.CsvSettings(this.settings), this.skipHeaders, this.fields, this.classLabel, this.config);

                String name = "fileIndex.csv";
                try {
                    fileIndexWriter = Files.newBufferedWriter(getOutDir().resolve(name));
                    this.input.setFileIndexLogger((path,size) -> {
                        try {
                            fileIndexWriter.write(path + "," + size + "\n");
                            fileIndexWriter.flush();
                        } catch (Exception ex) {
                            ex.printStackTrace();
                        }
                    });
                } catch (Exception ex) {
                    ex.printStackTrace();
                }
            }
            return this.input;
        }
    }

    public static class InputWithIndex extends BirchBatch.Input {
        protected long index;
        protected long lastMaxIndex;
        protected BiConsumer<String,String> fileIndexLogger;

        public InputWithIndex() {
        }

        public InputWithIndex(String inputFile, String charsetName, CsvIO.CsvSettings settings, int skipHeaders, List<BirchBatch.DataField> fields, BirchBatch.DataField classLabel, BirchConfig config) {
            super(inputFile, charsetName, settings, skipHeaders, fields, classLabel, config);
        }

        public void setFileIndexLogger(BiConsumer<String,String> fileIndexLogger) {
            this.fileIndexLogger = fileIndexLogger;
        }

        public CsvReader makeNextReaderFromFile(String path) {
            CsvReader r = CsvReader.get(Paths.get(path));
            if (fileIndexLogger != null) {
                fileIndexLogger.accept(path, Long.toString(this.index));
            }
            return r;
        }

        @Override
        public Stream<BirchNodeInput> makeDataPointStream(Iterable<CsvLine> lines) {
            index = lastMaxIndex;
            return super.makeDataPointStream(lines);
        }

        @Override
        public BirchNodeInput makeDataPoint(CsvLine line) {
            Stream<BirchBatch.DataField> dataCons;
            if (classLabel == null) {
                dataCons = fields.stream();
            } else {
                dataCons = Stream.concat(fields.stream(), Stream.of(classLabel));
            }

            BirchNodeInput input = config.newPoint(index + line.getStart(),
                    dataCons.mapToDouble(f -> f.extract(line))
                            .toArray());
            lastMaxIndex = Math.max(lastMaxIndex, index + line.getEnd());
            return input;
        }
    }

    public static class ServerActor extends AbstractActor {
        protected ActorBirchServer server;
        protected Instant lastInputTime;
        protected long inputCount;

        public ServerActor(ActorBirchServer server) {
            this.server = server;
            lastInputTime = Instant.EPOCH;
            server.constructInit();
        }

        @Override
        public Receive createReceive() {
            return receiveBuilder()
                    .match(BirchMessages.ServerTextInput.class, this::input)
                    .match(BirchMessages.ServerLoadInput.class, this::loadInput)
                    .match(BirchMessages.ServerStatus.class, this::status)
                    .match(BirchMessages.ServerSave.class, this::save)
                    .match(BirchMessages.ServerStat.class, this::stat)
                    .match(BirchMessages.ServerPurity.class, this::purity)
                    .match(BirchMessages.StatInconsistency.class, this::statInconsistency)
                    .match(BirchMessages.FixChildIndex.class, this::fixChildIndex)
                    .match(RemoteManagerMessages.Shutdown.class, this::shutdown)
                    .matchAny(this::redirectToRootChanger)
                    .build();
        }

        public void log(String str) {
            System.err.println("### [" + Instant.now() + "]" + str);
        }

        public void input(BirchMessages.ServerTextInput input) {
            log("input " + input.data.length());
            Instant time = Instant.now();
            Duration duration = Duration.between(lastInputTime, time);
            if (inputCount == 0 || duration.compareTo(Duration.ofSeconds(3)) > 0) {
                log("[" + duration + "] inputCount: " + inputCount + " input.length:" + input.data.length());
            }
            lastInputTime = time;
            InputThrottle throttle = server.getBatch().getInputThrottle();
            CsvReader r = CsvReader.get(new StringReader(input.data));
            try {
                throttle.start(server.getBatch().getSystem());
                server.getInputs(r)
                        .forEach(this::inputData);
            } finally {
                throttle.stop();
            }
        }

        public void loadInput(BirchMessages.ServerLoadInput input) {
            log("loadInput " + input.serverFile);
            Instant time = Instant.now();
            Duration duration = Duration.between(lastInputTime, time);
            log("[" + duration + "] inputCount: " + inputCount);
            lastInputTime = time;

            BirchBatch.InputI i = server.getBatch().getInputForRead();
            InputThrottle throttle = server.getBatch().getInputThrottle();
            try (CsvReader reader = openForLoad(input.serverFile, i)) {
                throttle.start(server.getBatch().getSystem());
                i.makeDataPointStream(reader)
                        .forEach(this::inputData);
            } finally {
                throttle.stop();
            }
            log("loadInput server-finish " + input.serverFile);
        }

        private CsvReader openForLoad(String serverFile, BirchBatch.InputI i) {
            if (i instanceof InputWithIndex) {
                return ((InputWithIndex) i).makeNextReaderFromFile(serverFile);
            } else {
                return CsvReader.get(Paths.get(serverFile));
            }
        }

        public void status(BirchMessages.ServerStatus s) {
            String res = " lastInputTime:" + lastInputTime + ", inputCount: " + inputCount;
            log(res);
            Map<String,Object> finish = getFinishInfo();
            String finishStr = JsonWriter.v().write(finish).toSource();
            log(finishStr);
            sender().tell(res + "\n" + finishStr, self());
        }

        public void inputData(BirchNodeInput i) {
            server.getBatch().getRootChanger()
                    .tell(new BirchMessages.PutToRoot(i), ActorRef.noSender());
            ++inputCount;
            server.getBatch().getInputThrottle().incrementInputCount();
        }

        public void redirectToRootChanger(Object msg) {
            server.getBatch().getRootChanger()
                    .tell(msg, sender());
        }

        public void save(BirchMessages.ServerSave s) {
            log("start save "  +s.serverFile);
            ActorBirchVisitorSaveSingleFile.run(server.system, server.getBatch().constructAfter(), s.serverFile);
            sender().tell("saved", self());
        }

        @SuppressWarnings("unchecked")
        public Map<String,Object> getFinishInfo() {
            Map<String,Object> json = new HashMap<>();
            ActorRef finishActor = this.server.config.getFinishActor();
            log(" stat: obtain finish-count from finishing-actor " + finishActor);
            try {
                Timeout timeout = new Timeout(2, TimeUnit.SECONDS);
                Long count = (Long) PatternsCS.ask(finishActor, new BirchMessages.GetFinishCount(), timeout)
                        .toCompletableFuture().get(2, TimeUnit.SECONDS);
                Instant time = (Instant) PatternsCS.ask(finishActor, new BirchMessages.GetFinishTime(), timeout)
                        .toCompletableFuture().get(2, TimeUnit.SECONDS);

                List<RemoteManagerMessages.Status> ss = (List<RemoteManagerMessages.Status>)
                        PatternsCS.ask(server.batch.remoteManager, new RemoteManagerMessages.GetStatusAll(), timeout)
                        .toCompletableFuture().get(2, TimeUnit.SECONDS);

                json.put("finishCount", count);
                json.put("finishTime", time.toString());
                json.put("managerStatus", ss.stream()
                    .map(RemoteManagerMessages.Status::toJson)
                    .collect(Collectors.toList()));

            } catch (Exception ex) {
                log(" failed finishing-actor " + ex);
            }
            return json;
        }

        public void stat(BirchMessages.ServerStat s) { //-> String
            log("start stat");
            Map<String,Object> json = server.getBatch().getTreeStats(server.getBatch().constructAfter());

            json.putAll(getFinishInfo());
            sender().tell(JsonWriter.v().write(json).toSource(), self());
        }

        public void statInconsistency(BirchMessages.StatInconsistency m) { //-> Map
            log("start statInconsistency");
            Map<String,Object> json = server.getBatch().statInconsistency();

            json.forEach((k,v) ->
                    log(String.format("  %s: %s", k, v)));

            sender().tell(JsonWriter.v().write(json).toSource(), self());
        }

        public void fixChildIndex(BirchMessages.FixChildIndex m) { // -> Long
            log("start fixChildIndex");
            long removed = server.getBatch().fixChildIndex();
            long entries = server.getBatch().countEntries(server.getBatch().constructAfter());
            log(String.format("finish fixChildIndex : entries removed %,d -> %,d", removed, entries));
            sender().tell(entries, self());
        }

        public void purity(BirchMessages.ServerPurity p) {
            log("start purity");
            sender().tell("not-yet-implemented", self());
            //TODO
//            try (ActorDendrogramPurityUnit dpUnit = new ActorDendrogramPurityUnit(server.getBatch().getSystem(), server.getBatch().getInputForRead());
//                 PrintWriter out = new PrintWriter(Files.newBufferedWriter(getOutDir().resolve("d-purity.txt")))) {
//                dpUnit.setAdditionalOutput(out);
//
//                dpUnit.setClassSize(classSize);
//                double purity = dpUnit.calculatePurity(root);
//                dpUnit.log(String.format("DendrogramPurity : %f\n", purity));
//            } catch (IOException e) {
//                throw new RuntimeException(e);
//            }
        }

        public void shutdown(RemoteManagerMessages.Shutdown s) {
            server.terminate();
        }
    }
}
