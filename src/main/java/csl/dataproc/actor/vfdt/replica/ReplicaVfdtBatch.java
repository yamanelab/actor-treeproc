package csl.dataproc.actor.vfdt.replica;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.pattern.PatternsCS;
import akka.util.Timeout;
import csl.dataproc.actor.vfdt.*;
import csl.dataproc.dot.DotGraph;
import csl.dataproc.vfdt.VfdtNode;
import csl.dataproc.vfdt.VfdtNodeGrowingSplitter;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;

public class ReplicaVfdtBatch extends ActorVfdtBatch {
    public static void main(String[] args) {
        VfdtNodeGrowingSplitter.DEBUG = true;
        new ReplicaVfdtBatch().run(args);
    }

    @Override
    protected ActorVfdtConfig newConfig(ActorSystem system) {
        return new ReplicaVfdtConfig(system);
    }

    @Override
    protected VfdtBatchEx newBatchEx() {
        return new ReplicaBatchEx((ReplicaVfdtConfig) config);
    }


    public static class ReplicaBatchEx extends VfdtBatchEx {
        protected ActorRef root1;
        protected ActorRef root2;

        public ReplicaBatchEx(ReplicaVfdtConfig config) {
            super(config);
        }

        public ReplicaVfdtConfig replicaConfig() {
            return  (ReplicaVfdtConfig) config;
        }

        @Override
        public void rootInit() {
            if (root1 == null) {
                root1 = replicaConfig().createRoot(getTypes());
                root2 = replicaConfig().createRoot(getTypes());

                log("root1: " + root1);
                log("root2: " + root2);

                await(root1, new ReplicaMessages.Pair(root2));
                await(root2, new ReplicaMessages.Pair(root1));

                root = new ActorVfdtRefNode(root1, 0);
            }
        }

        protected void await(ActorRef root, Object msg) {
            try {
                Timeout timeout = new Timeout(1, TimeUnit.MINUTES);
                PatternsCS.ask(root, msg, timeout)
                        .toCompletableFuture().get();
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }

        @Override
        public void put(VfdtNode.DataLine dataLine) {
            ActorRef ref;
            if (learnInstanceCount % 2 == 0) {
                ref = root1;
            } else {
                ref = root2;
            }

            ref.tell(new VfdtMessages.PutDataLine((ActorVfdtDataLine) dataLine), ActorRef.noSender());
            ++learnInstanceCount;
        }

        @Override
        public void test() {
            root1.tell(new ReplicaMessages.UpdateMerge(null), ActorRef.noSender());
            super.test();
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
            return new ReplicaVfdtVisitorDot().run(system(), getRoot(), this.getDotFile());
        }

        @Override
        public void endRoot() {
            super.endRoot();
            end(new ActorVfdtRefNode(root2, 0));
        }
    }
}
