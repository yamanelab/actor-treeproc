package csl.dataproc.actor.birch;

import csl.dataproc.birch.BirchNode;

/** a parent type for non-actor nodes:
 *    any method invocations to an instance of the type do not cause message sending to an actor */
public interface BirchNodeConcrete extends BirchNode {
}
