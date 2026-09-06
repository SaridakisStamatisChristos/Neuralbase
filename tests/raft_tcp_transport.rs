// SPDX-License-Identifier: Apache-2.0

use neuralbase::consensus::{
    RaftMessage, RequestVoteArgs, RequestVoteReply, TcpTransport, Transport,
};
use std::collections::HashMap;
use std::net::TcpListener as StdTcpListener;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::timeout;

fn reserve_addr() -> String {
    let listener = StdTcpListener::bind("127.0.0.1:0").expect("reserve loopback port");
    let addr = listener.local_addr().expect("read loopback address");
    drop(listener);
    addr.to_string()
}

#[tokio::test]
async fn logical_node_ids_route_over_explicit_tcp_addresses() {
    let addr1 = reserve_addr();
    let addr2 = reserve_addr();

    let mut peers1 = HashMap::new();
    peers1.insert("n2".to_string(), addr2.clone());
    let mut peers2 = HashMap::new();
    peers2.insert("n1".to_string(), addr1.clone());

    let t1 = Arc::new(
        TcpTransport::listen_with_peers("n1".to_string(), &addr1, peers1)
            .await
            .expect("listen n1"),
    );
    let t2 = Arc::new(
        TcpTransport::listen_with_peers("n2".to_string(), &addr2, peers2)
            .await
            .expect("listen n2"),
    );

    t1.send(
        &"n2".to_string(),
        RaftMessage::RequestVote(RequestVoteArgs {
            term: 7,
            candidate_id: "n1".to_string(),
            last_log_index: 11,
            last_log_term: 6,
        }),
    )
    .await;

    let (from, msg) = timeout(Duration::from_secs(2), t2.recv())
        .await
        .expect("n2 receive timeout")
        .expect("n2 transport closed");
    assert_eq!(from, "n1");
    match msg {
        RaftMessage::RequestVote(args) => {
            assert_eq!(args.term, 7);
            assert_eq!(args.candidate_id, "n1");
            assert_eq!(args.last_log_index, 11);
            assert_eq!(args.last_log_term, 6);
        }
        other => panic!("unexpected message at n2: {other:?}"),
    }

    t2.send(
        &"n1".to_string(),
        RaftMessage::RequestVoteReply(RequestVoteReply {
            term: 7,
            vote_granted: true,
        }),
    )
    .await;

    let (from, msg) = timeout(Duration::from_secs(2), t1.recv())
        .await
        .expect("n1 receive timeout")
        .expect("n1 transport closed");
    assert_eq!(from, "n2");
    match msg {
        RaftMessage::RequestVoteReply(reply) => {
            assert_eq!(reply.term, 7);
            assert!(reply.vote_granted);
        }
        other => panic!("unexpected reply at n1: {other:?}"),
    }
}
