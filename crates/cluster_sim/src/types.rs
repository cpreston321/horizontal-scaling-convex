//! Core identifier and value types shared across the simulation.

/// A node, which in this model is also a partition: one single-writer per
/// partition (the committer). Node 0 is the cluster coordinator / placement
/// owner, mirroring `PartitionId::DEFAULT` in the real code.
pub type NodeId = u32;

/// A logical key. Each key is owned by exactly one partition under the current
/// placement map.
pub type Key = u32;

/// A commit timestamp from the global timestamp oracle. Strictly increasing.
pub type Ts = u64;

/// A written value. Unique per write so the invariant checker can tell which
/// transaction produced a stored value.
pub type Value = u64;

/// A 2PC transaction id, assigned by the coordinator.
pub type TxnId = u64;

/// Virtual simulation time, in abstract ticks. Never wall-clock.
pub type SimTime = u64;
