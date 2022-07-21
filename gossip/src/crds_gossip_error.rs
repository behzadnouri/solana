#[derive(PartialEq, Eq, Debug)]
pub enum CrdsGossipError {
    NoPeers,
    PushMessageTimeout,
    BadPruneDestination,
    PruneMessageTimeout,
}
