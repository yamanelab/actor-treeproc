package csl.dataproc.actor.vfdt;

import akka.actor.*;
import csl.dataproc.actor.remote.RemoteConfig;
import csl.dataproc.actor.remote.RemoteManager;
import csl.dataproc.actor.remote.RemoteManagerMessages;
import csl.dataproc.actor.stat.StatMailbox;
import csl.dataproc.actor.stat.StatMessages;
import csl.dataproc.actor.stat.StatSystem;
import csl.dataproc.actor.stat.StatVm;
import csl.dataproc.vfdt.VfdtBatch;
import scala.concurrent.duration.FiniteDuration;

import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.concurrent.TimeUnit;

public class ActorVfdtBatchStageActor extends AbstractActor {
    protected ActorSystem system = ActorVfdtConfig.globalSystem;
    protected ActorVfdtBatch.VfdtBatchEx batch;

    public ActorVfdtBatchStageActor(ActorVfdtBatch.VfdtBatchEx batch) {
        this.batch = batch;
    }

    protected LeanTestState learn = new LeanTestState(true);
    protected LeanTestState test = new LeanTestState(false);
    protected LeanTestState current = learn;
    protected LeanTestState prevInterval = learn.copy();
    protected Instant prevIntervalTime = Instant.now();

    protected Cancellable intervalTask;

    @Override
    public void preStart() throws Exception {
        intervalTask = context().system().scheduler().schedule(
                new FiniteDuration(10, TimeUnit.SECONDS),
                new FiniteDuration(10, TimeUnit.SECONDS),
                self(), new VfdtMessages.EndCheck(), context().dispatcher(), self());
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(VfdtMessages.LearnReturn.class, this::learnReturn)
                .match(VfdtMessages.LearnEnd.class, this::learnEnd)
                .match(VfdtMessages.ClassifyReturn.class, this::classifyReturn)
                .match(VfdtMessages.ClassifyEnd.class, this::classifyEnd)
                .match(VfdtMessages.EndCheck.class, this::endCheck)
                .match(DeadLetter.class, this::deadLetter)
                .match(StatMessages.ActorId.class, this::actorId)
                .build();
    }

    public void actorId(StatMessages.ActorId id) { } //ignore

    public void endCheck(VfdtMessages.EndCheck m) {
        batch.log("check interval: \n" +
            "  " + (current == learn ? "*" : " ") + learn.toString() + "\n" +
            "  " + (current == test  ? "*" : " ") + test.toString());

        if (current == learn) {
            checkLearnFinish();
        } else {
            if (!test.finished) {
                checkTerminate();
            }
        }

        if (current.equals(prevInterval)) {
            forceNext();
        } else {
            showIntervalProgress();
        }
        prevIntervalTime = Instant.now();
        prevInterval = current.copy();
    }

    public void forceNext() {
        //no change
        batch.log("no state change: " + current);
        if (current == learn) {
            testAndOutput();
        } else {
            if (!test.finished) {
                terminate();
            }
        }
    }

    public void showIntervalProgress() {
        long diffCount;
        if (prevInterval.learn == current.learn) {
            diffCount = current.progressCount() - prevInterval.progressCount();
        } else if (current.learn) {
            diffCount = current.progressCount() + (learn.progressCount() - prevInterval.progressCount());
        } else {
            diffCount = current.progressCount() + 1;
        }

        Duration elapsed = Duration.between(prevIntervalTime, Instant.now());
        double countPerSec = diffCount / (elapsed.getSeconds() + (elapsed.getNano() / 1000_000_000.0));
        Duration timeForCount = diffCount == 0 ? Duration.ZERO : elapsed.dividedBy(diffCount);
        batch.log(String.format(" interval progress: %,d : elapsed: %s,  %.4f/sec,  %s/count",
                diffCount, elapsed.toString(), countPerSec, timeForCount.toString()));
    }

    public void learnReturn(VfdtMessages.LearnReturn m) {
        learn.returnedCount++;
        checkLearnFinish();
    }

    public void learnEnd(VfdtMessages.LearnEnd m) {
        learn.maxCount = m.count;
        batch.log("learn end: " + learn);
        checkLearnFinish();
    }

    public void checkLearnFinish() {
        if (learn.isFinish()) {
            batch.log("learn finish: " + learn);
            showIntervalProgress();
            batch.setLearnTimeFromStart(Instant.now());
            testAndOutput();
        }
    }

    public void testAndOutput() {
        current = test;
        learn.finished = true;
        batch.test();
    }

    public void classifyReturn(VfdtMessages.ClassifyReturn m) {
        long ans = m.line.getLong(batch.getTypes().size() - 1);
        long estimated = m.classValue;
        batch.getStatMap()
                .computeIfAbsent(ans, VfdtBatch.AnswerStat::new)
                .put(estimated);
        test.returnedCount++;
        checkTerminate();
    }

    public void classifyEnd(VfdtMessages.ClassifyEnd m) {
        test.maxCount = m.count;
        batch.log("test end: " + test);
        checkTerminate();
    }

    public void checkTerminate() {
        if (test.isFinish()) {
            batch.log("test finish: " + test);
            showIntervalProgress();
            batch.setTestTimeFromStart(Instant.now());
            batch.setTotalTimeFromStart(Instant.now());
            terminate();
        }
    }

    public void terminate() {
        test.finished = true;
        if (intervalTask != null) {
            intervalTask.cancel();
            intervalTask = null;
        }
        if (batch.localStatVm != null) {
            batch.localStatVm.saveTotal();
        }
        batch.output();
        batch.log("blocking: " + ActorVfdtRefNode.blockingAsk.get());
        batch.endRoot();

        if (batch.remoteConfig.shutdownCluster && batch.remoteManager != null) {
            batch.log("shutdown cluster");
            batch.remoteManager.tell(new RemoteManagerMessages.Shutdown(), self());
        }

        self().tell(PoisonPill.getInstance(), self());
        system.terminate();
        StatMailbox.endGlobal();
    }


    public void deadLetter(DeadLetter l) {
        StatVm vm = StatVm.instance;
        if (vm != null) {
            vm.addDeadLetter(l);
        }
        if (l.message() instanceof VfdtMessages.PutDataLine) {
            learn.deadCount++;
            if (learn.deadCount < 10) {
                batch.log("dead-letter: " + l.message() + " : " + l.recipient());
            }
            checkLearnFinish();
        } else if (l.message() instanceof VfdtMessages.StepClassify) {
            test.deadCount++;
            if (test.deadCount < 10) {
                batch.log("dead-letter: " + l.message() + " : " + l.recipient());
            }
            checkTerminate();
        } else if (l.message() instanceof RemoteManagerMessages.Creation<?>) {
            RemoteManager m = RemoteManager.instance;
            if (m != null) {
                m.createActorLocal((RemoteManagerMessages.Creation<?>) l.message());
            }
        }
    }

    public static class LeanTestState implements Cloneable {
        public boolean learn;
        public long returnedCount;
        public long deadCount;
        public long maxCount = Long.MAX_VALUE;
        public boolean finished;

        public LeanTestState(boolean learn) {
            this.learn = learn;
        }

        public LeanTestState copy() {
            try {
                return (LeanTestState) super.clone();
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }

        public long progressCount() {
            return returnedCount + deadCount;
        }

        public boolean isFinish() {
            return returnedCount + deadCount >= maxCount;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            LeanTestState that = (LeanTestState) o;

            if (learn != that.learn) return false;
            if (returnedCount != that.returnedCount) return false;
            if (deadCount != that.deadCount) return false;
            return maxCount == that.maxCount;
        }

        @Override
        public int hashCode() {
            int result = (learn ? 1 : 0);
            result = 31 * result + (int) (returnedCount ^ (returnedCount >>> 32));
            result = 31 * result + (int) (deadCount ^ (deadCount >>> 32));
            result = 31 * result + (int) (maxCount ^ (maxCount >>> 32));
            return result;
        }

        @Override
        public String toString() {
            return String.format("%s: finish=%s, returned=%,d, dead=%,d, max=%,d",
                    (learn ? "learn": "test "), Boolean.toString(isFinish()),
                    returnedCount, deadCount, maxCount);
        }
    }

    public static void setup(ActorSystem system, ActorVfdtBatch.VfdtBatchEx batch) {
        Props p = Props.create(ActorVfdtBatchStageActor.class, () -> new ActorVfdtBatchStageActor(batch));
        if (StatSystem.system().isEnabled()) {
            p = p.withMailbox(RemoteConfig.CONF_STAT_MAILBOX);
        }
        p = p.withDispatcher(RemoteConfig.CONF_PINNED_DISPATCHER);

        batch.stageRef = system.actorOf(p);
        batch.stageEndRef = batch.stageRef;
        system.eventStream().subscribe(batch.stageRef, AllDeadLetters.class);
    }

    public static void setupNeglect(ActorSystem system, ActorVfdtBatch.VfdtBatchEx batch) {
        Props p = Props.create(NeglectStageActor.class, () -> new NeglectStageActor(batch));
        batch.stageRef = null;
        batch.stageEndRef = system.actorOf(p);
    }

    public static double neglectDefaultUnitMillis = 0.2;

    public static class NeglectStageActor extends ActorVfdtBatchStageActor {
        public NeglectStageActor(ActorVfdtBatch.VfdtBatchEx batch) {
            super(batch);
        }

        @Override
        public void preStart() throws Exception {
        }

        @Override
        public Receive createReceive() {
            return receiveBuilder()
                    //.match(VfdtMessages.LearnReturn.class, this::learnReturn)
                    .match(VfdtMessages.LearnEnd.class, this::learnEnd)
                    //.match(VfdtMessages.ClassifyReturn.class, this::classifyReturn)
                    .match(VfdtMessages.ClassifyEnd.class, this::classifyEnd)
                    .match(VfdtMessages.EndCheck.class, this::endCheck)
                    .match(DeadLetter.class, this::deadLetter)
                    .match(StatMessages.ActorId.class, this::actorId)
                    .build();
        }


        @Override
        public void learnEnd(VfdtMessages.LearnEnd m) {
            learn.maxCount = m.count;
            batch.log("learn end: " + learn);
            scheduleEnd(getScheduleTime(m.count, batch.neglectTime));
        }

        public long getScheduleTime(long count, double unitMillis) {
            if (unitMillis < 0) {
                unitMillis = neglectDefaultUnitMillis;
            }
            long sec = (long) (unitMillis * count / 1000);
            if (sec <= 0) {
                sec = 3;
            }
            return sec;
        }

        public void scheduleEnd(long seconds) {
            batch.log("next step scheduled: " + LocalDateTime.now().plus(seconds, ChronoUnit.SECONDS) + " (" + seconds + " seconds)");
            context().system().scheduler()
                    .scheduleOnce(new FiniteDuration(seconds, TimeUnit.SECONDS),
                            self(), new VfdtMessages.EndCheck(), context().dispatcher(), ActorRef.noSender());
        }

        @Override
        public void classifyEnd(VfdtMessages.ClassifyEnd m) {
            test.maxCount = m.count;
            batch.log("test end: " + test);
            scheduleEnd(getScheduleTime(m.count, batch.neglectTime));
        }

        @Override
        public void endCheck(VfdtMessages.EndCheck m) {
            batch.log("scheduled end check");
            if (learn == current) {
                learn.finished = true;
                ActorVfdtNodeActor.LeafMessageCount count = getCount(VfdtMessages.PutDataLine.class);
                batch.setLearnTimeFromStart(count.time);
                learn.returnedCount += count.count;

                batch.log("current: " + learn + " : " + batch.getLearnTime());
                batch.log("next   : " + test);
                testAndOutput();
            } else {
                test.finished = true;
                ActorVfdtNodeActor.LeafMessageCount count = getCount(VfdtMessages.StepClassify.class);
                batch.setTestTimeFromStart(count.time);
                batch.setTotalTimeFromStart(count.time);
                test.returnedCount += count.count;

                batch.log("current: " + test + " : " + batch.getTestTime());
                batch.log("terminate");
                terminate();
            }
        }

        protected ActorVfdtNodeActor.LeafMessageCount getCount(Class<?> cls) {
            ActorVfdtNodeActor.LeafMessageCount c = ActorVfdtVisitorLeafMsgCount.run(system, batch.getRoot())
                    .getOrDefault(cls.getName(), new ActorVfdtNodeActor.LeafMessageCount());
            if (c.time == null) {
                c.time = Instant.EPOCH;
            }
            return c;
        }
    }

}
