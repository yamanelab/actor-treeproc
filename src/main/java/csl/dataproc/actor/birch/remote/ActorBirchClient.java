package csl.dataproc.actor.birch.remote;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.pattern.PatternsCS;
import akka.util.Timeout;
import csl.dataproc.actor.birch.BirchMessages;
import csl.dataproc.actor.remote.RemoteConfig;
import csl.dataproc.actor.remote.RemoteManagerMessages;
import scala.concurrent.Await;
import scala.concurrent.duration.FiniteDuration;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.*;

public class ActorBirchClient {

    public static void main(String[] args) throws Exception {
        new ActorBirchClient().run(args);
    }

    protected RemoteConfig config;
    protected ActorSystem system;
    protected String actorPath;
    protected ActorRef actorRef;

    public static Instant startTime = Instant.now();

    public void run(String... args) throws Exception {
        List<String> restArgs = init(args);
        if (restArgs.contains("--help")) {
            System.err.println(
                    " --server-put <localFile>   : put csv data. - means standard-input\n" +
                    " --server-put-file <serverFile> : put csv data on the server\n" +
                    " --server-stat <localFile>  : save stat to file. - means standard-output\n" +
                    " --server-save <serverFile> : save tree structure to file on the server\n" +
                    " --server-status            : show status of the server\n" +
                    " --server-fix-child-index   : issue FixChildIndex message\n" +
                    " --server-stat-inconsistency <localFile>\n" +
                    " --server-shutdown\n" +
                    " <actorPath>                : target actor path like akka.tcp://dataproc@host:54321/user/server");

            system.terminate();
            return;
        }

        try {
            List<Command> cs = parseArgs(restArgs);
            setActor(actorPath);
            for (Command c : cs) {
                c.run(actorRef);
            }
        } finally {
            system.terminate();
        }
    }


    public List<String> init(String... args) {
        config = RemoteConfig.create(Arrays.asList(args));
        system = config.createSystem();

        return config.restArgs;
    }

    public List<Command> parseArgs(List<String> restArgs) {
        List<Command> commands = new ArrayList<>();
        for (int i = 0; i < restArgs.size(); ++i) {
            String arg = restArgs.get(i);
            if (arg.equals("--server-put")) {
                ++i;
                String localFile = restArgs.get(i);
                commands.add(new CommandPut(localFile));
            } else if (arg.equals("--server-put-file")) {
                ++i;
                String serverFile = restArgs.get(i);
                commands.add(new CommandLoad(serverFile));
            } else if (arg.equals("--server-save")) {
                ++i;
                String serverFile = restArgs.get(i);
                commands.add(new CommandSave(serverFile));
            } else if (arg.equals("--server-status")) {
                commands.add(new CommandStatus());
            } else if (arg.equals("--server-stat")) {
                ++i;
                String localFile = restArgs.get(i);
                commands.add(new CommandStat(localFile));
            } else if (arg.equals("--server-stat-inconsistency")) {
                ++i;
                String localFile = restArgs.get(i);
                commands.add(new CommandStatInconsistency(localFile));
            } else if (arg.equals("--server-purity")) {
                ++i;
                String localFile = restArgs.get(i);
                commands.add(new CommandPurity(localFile));
            } else if (arg.equals("--server-fix-child-index")) {
                commands.add(new CommandFixChildIndex());
            } else if (arg.equals("--server-shutdown")) {
                commands.add(new CommandShutdown());
            } else {
                actorPath = arg;
            }
        }
        return commands;
    }


    public void setActor(String actorPath) throws Exception {
        log("try to resolve : " + actorPath);
        Timeout timeout = new Timeout(5, TimeUnit.SECONDS);
        FiniteDuration duration = new FiniteDuration(5, TimeUnit.SECONDS);
        actorRef = Await.result(system.actorSelection(actorPath).resolveOne(timeout), duration);
        log("resolved " + actorRef);
    }

    public static void log(String str) {
        System.err.printf("### [%s] : %s\n", Duration.between(startTime, Instant.now()), str);
    }

    public static class Command {
        public void run(ActorRef actor) throws Exception {

        }
    }

    public static long LIMIT_BYTES = 128000L;

    public class CommandPut extends Command {
        protected String localFile;
        protected List<String> lines = new ArrayList<>();
        protected long currentBytes;

        public CommandPut(String localFile) {
            this.localFile = localFile;
        }

        @Override
        public void run(ActorRef actor) throws Exception {
            if (localFile.equals("-")) {
                log("put stdin");
                runStream(actor, System.in);
            } else {
                log("put file: " + localFile);
                runStream(actorRef, new FileInputStream(new File(localFile)));
            }
        }

        public void runStream(ActorRef actorRef, InputStream in) {
            Instant lastSentTime = Instant.now();
            long lineCount = 0;
            ExecutorService service = Executors.newSingleThreadExecutor();
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8))) {
                while (true) {
                    Future<String> line = service.submit(reader::readLine);
                    try {
                        String data = line.get(1, TimeUnit.SECONDS);
                        if (data == null) {
                            send(actorRef, " by finish");
                            break;
                        }
                        if (currentBytes + data.length() + 1 >= LIMIT_BYTES) { //suppose no wide chars
                            send(actorRef, " by buffer-full");
                        }
                        currentBytes += data.length() + 1;
                        lines.add(data);

                        ++lineCount;

                        Instant now = Instant.now();
                        if (Duration.between(lastSentTime, now).compareTo(Duration.ofSeconds(3)) > 0) {
                            send(actorRef, " by time-duration");
                        }
                    } catch (TimeoutException timeout) {
                        send(actorRef, " by timeout");

                        log("waiting....");
                        String data = line.get(); //get forever
                        log(" returned");
                        if (data == null) {
                            break;
                        } else {
                            lines.add(data);
                            ++lineCount;
                        }
                    }
                }
            } catch (InterruptedException ie) {
                log("interrupted");
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            } finally {
                service.shutdown();
                log("finish lineCount: " + lineCount);
                try {
                    new CommandStatus().run(actorRef);
                } catch (Exception ex) {
                    throw new RuntimeException(ex);
                }
            }
        }

        public void send(ActorRef actorRef, String msg) {
            if (!lines.isEmpty()) {
                log("send [" + lines.size() + "] " + msg);
                actorRef.tell(new BirchMessages.ServerTextInput(String.join("\n", lines)), ActorRef.noSender());
                lines.clear();
                currentBytes = 0;
            }
        }
    }

    public class CommandLoad extends Command {
        protected String serverFile;
        public CommandLoad(String serverFile) {
            this.serverFile = serverFile;
        }

        @Override
        public void run(ActorRef actor) throws Exception {
            log("send load-and-put serverFile: " + serverFile);
            actor.tell(new BirchMessages.ServerLoadInput(serverFile), ActorRef.noSender());
        }
    }

    public class CommandStatus extends Command {
        @Override
        public void run(ActorRef actor) throws Exception {
            Object ret = PatternsCS.ask(actor, new BirchMessages.ServerStatus(), new Timeout(5, TimeUnit.SECONDS))
                .toCompletableFuture().get();
            log("status : " + ret);
        }
    }

    public class CommandSave extends Command {
        protected String serverFile;

        public CommandSave(String serverFile) {
            this.serverFile = serverFile;
        }

        @Override
        public void run(ActorRef actor) throws Exception {
            log("save serverFile: " + serverFile);
            PatternsCS.ask(actor, new BirchMessages.ServerSave(serverFile), new Timeout(5, TimeUnit.HOURS))
                .toCompletableFuture().get(5, TimeUnit.HOURS);
            log("save returned");
        }
    }

    public class CommandStat extends Command {
        protected String localFile;

        public CommandStat(String localFile) {
            this.localFile = localFile;
        }

        @Override
        public void run(ActorRef actor) throws Exception {
            log("stat " + localFile);
            Object ret = PatternsCS.ask(actor, new BirchMessages.ServerStat(), new Timeout(5, TimeUnit.HOURS))
                .toCompletableFuture().get(5, TimeUnit.HOURS); //String
            log("stat returned");
            if (localFile.equals("-")) {
                System.out.println(ret);
            } else {
                Files.write(Paths.get(localFile), Collections.singletonList(ret.toString()));
            }
        }
    }

    public class CommandStatInconsistency extends Command {
        protected String localFile;

        public CommandStatInconsistency(String localFile) {
            this.localFile = localFile;
        }

        @Override
        public void run(ActorRef actor) throws Exception {
            log("statInconsistency " + localFile);
            Object ret = PatternsCS.ask(actor, new BirchMessages.StatInconsistency(), new Timeout(5, TimeUnit.HOURS))
                    .toCompletableFuture().get(5, TimeUnit.HOURS); //String
            log("statInconsistency returned");
            if (localFile.equals("-")) {
                System.out.println(ret);
            } else {
                Files.write(Paths.get(localFile), Collections.singletonList(ret.toString()));
            }
        }
    }

    public class CommandPurity extends Command {
        protected String localFile;

        public CommandPurity(String localFile) {
            this.localFile = localFile;
        }

        @Override
        public void run(ActorRef actor) throws Exception {
            log("purity " + localFile);
            Object ret = PatternsCS.ask(actor, new BirchMessages.ServerPurity(), new Timeout(5, TimeUnit.DAYS))
                    .toCompletableFuture().get(5, TimeUnit.DAYS); //String
            log("purity returned");
            if (localFile.equals("-")) {
                System.out.println(ret);
            } else {
                Files.write(Paths.get(localFile), Collections.singletonList(ret.toString()));
            }
        }
    }

    public class CommandFixChildIndex extends Command {
        @Override
        public void run(ActorRef actor) throws Exception {
            log("fixChildIndex");
            Object ret = PatternsCS.ask(actor, new BirchMessages.FixChildIndex(), new Timeout(5, TimeUnit.HOURS))
                .toCompletableFuture().get(5, TimeUnit.HOURS);
            log("fixChildIndex returned: " + ret);
            System.out.println(ret);
        }
    }

    public class CommandShutdown extends Command {
        @Override
        public void run(ActorRef actor) throws Exception {
            log("shutdown");
            actor.tell(new RemoteManagerMessages.Shutdown(), ActorRef.noSender());
        }
    }
}
