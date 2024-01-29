use datatypes::value::Value;
use hydroflow::scheduled::graph::Hydroflow;
use hydroflow::scheduled::graph_ext::GraphExt;
use hydroflow::scheduled::handoff::VecHandoff;
use hydroflow::scheduled::port::{Port, RECV};
use itertools::Itertools;

use crate::expr::{MapFilterProject, ScalarExpr};
use crate::hydro_compute::types::{Erroff, Hoff, OkErrRecvPort, OkErrSendPort, VecDiff};
use crate::repr::{self, Row};

/// representation of a Collection, include both ok and err output
///
/// **Expensive** to clone since it need to build a subgraph to clone it's input to multiple output
///
pub struct CollectionBundle {
    /// straight ahead a vector storing this tick's output
    pub ok: Port<RECV, Hoff>,
    pub err: Port<RECV, Erroff>,
}

pub fn new_port_pairs<T>(df: &mut Hydroflow) -> (OkErrSendPort<T>, OkErrRecvPort<T>) {
    let (ok_send, ok_recv) = df.make_edge::<_, VecHandoff<T>>("ok");
    let (err_send, err_recv) = df.make_edge::<_, Erroff>("err");
    ((ok_send, err_send), (ok_recv, err_recv))
}

/// TODO: generalize this to replicate any type of port
pub(crate) fn replicate_recv_port(
    (ok, err): OkErrRecvPort,
    times: usize,
    df: &mut Hydroflow,
) -> Vec<OkErrRecvPort> {
    let (oks_send, oks_recv): (Vec<_>, Vec<_>) = (0..times)
        .map(|_| df.make_edge::<_, Hoff>("replicate_ok"))
        .unzip();
    let (errs_send, errs_recv): (Vec<_>, Vec<_>) = (0..times)
        .map(|_| df.make_edge::<_, Erroff>("replicate_err"))
        .unzip();

    df.add_subgraph_n_m(
        "replicate",
        vec![ok],
        oks_send,
        move |ctx, recv_ports, send_ports| {
            let recv = recv_ports[0].take_inner();
            for send in send_ports {
                send.give(recv.clone());
            }
        },
    );

    df.add_subgraph_n_m(
        "replicate",
        vec![err],
        errs_send,
        move |ctx, recv_ports, send_ports| {
            let recv = recv_ports[0].take_inner();
            for send in send_ports {
                send.give(recv.clone());
            }
        },
    );
    oks_recv.into_iter().zip(errs_recv).collect_vec()
}

impl CollectionBundle {
    /// consume self and produce a vec of the same CollectionBundle
    pub fn replicate(self, times: usize, df: &mut Hydroflow) -> Vec<Self> {
        replicate_recv_port((self.ok, self.err), times, df)
            .into_iter()
            .map(|(ok, err)| Self { ok, err })
            .collect_vec()
    }
    pub fn from_ok_err(ok: Port<RECV, Hoff>, err: Port<RECV, Erroff>) -> Self {
        Self { ok, err }
    }
}

#[test]
fn test_clone() {
    let mut df = Hydroflow::new();
    let (send_ok, recv_ok) = df.make_edge::<_, Hoff>("ok");
    let (send_err, recv_err) = df.make_edge::<_, Erroff>("err");
    replicate_recv_port((recv_ok, recv_err), 3, &mut df);
    df.add_subgraph_source("source", send_ok, |ctx, send| {
        for i in 0..1000 {
            send.give(vec![(Row::new(vec![Value::from(i)]), 0, 1)]);
        }
    });
    for times in 0..10000 {
        df.run_available();
    }
}
