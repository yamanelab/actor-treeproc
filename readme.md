# actor-tree

## Building

Use Java(>=8) and [Apache Maven](https://maven.apache.org) 

```bash
mvn package 
```

The above command line will download depending libraries and generate a jar named like `actor-treeproc-1.0-SNAPSHOT.jar`  contained in the  `target` sub-directory.

## Running

* main-classes

    * VFDT: `csl.dataproc.vfdt.VfdtBatch`
    * BIRCH: `csl.dataproc.birch.BirchBatch`

    * VFDT actor version: `csl.dataproc.actor.vfdt.ActorVfdtBatch`
    * VFDT actor version + root-node replication: `csl.dataproc.actor.vfdt.replica.ReplicaVfdtBatch`
    * BIRCH actor version: `csl.dataproc.actor.birch.ActorBirchBatch`
    * Remote companion: `csl.dataproc.actor.remote.RemoteManager` 

To run one of above classes, you can use [Exec Maven plugin](http://www.mojohaus.org/exec-maven-plugin/):

```bash
mvn exec:java -Dexec.classpathScope=test \
              -Dexec.mainClass=csl.dataproc.vfdt.VfdtBatch \
              -Dexec.args=
```



