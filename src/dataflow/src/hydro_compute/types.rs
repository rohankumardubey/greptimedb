// TODO: types for batch send/recv

use std::cell::RefCell;
use std::rc::Rc;

use hydroflow::scheduled::handoff::VecHandoff;
use hydroflow::scheduled::port::{Port, RECV, SEND};
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc::UnboundedSender;
use tokio_stream::wrappers::UnboundedReceiverStream;

use crate::expr::error::EvalError;
use crate::expr::GlobalId;
use crate::plan::{Plan, TypedPlan};
use crate::repr::{self, Diff, RelationType, Row};
use crate::utils::DiffMap;

/// (Data, Tick, Diff)
pub type Delta<T> = (T, repr::Timestamp, Diff);
pub type DiffRow = Delta<Row>;
pub type Hoff = VecHandoff<DiffRow>;
pub type Erroff = VecHandoff<Delta<EvalError>>;
pub type VecDiff = Vec<DiffRow>;

/// Send Port for both (ok, err) using `T` as Handoff to store
pub type OkErrSendPort<T = DiffRow> = (Port<SEND, VecHandoff<T>>, Port<SEND, Erroff>);
/// Recv Port for both (ok, err) using `T` as Handoff to store
pub type OkErrRecvPort<T = DiffRow> = (Port<RECV, VecHandoff<T>>, Port<RECV, Erroff>);

pub type RowMap = DiffMap<Row, Row>;

pub type RawRecvOkErr = (
    UnboundedReceiverStream<DiffRow>,
    UnboundedReceiverStream<Delta<EvalError>>,
);

pub type RawSendOkErr = (UnboundedSender<DiffRow>, UnboundedSender<Delta<EvalError>>);

/// An association of a global identifier to an expression.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct BuildDesc<P = TypedPlan> {
    pub id: GlobalId,
    pub plan: P,
}

#[derive(Default, Debug)]
pub struct DataflowDescription {
    pub objects_to_build: Vec<BuildDesc>,
    pub inputs: Vec<GlobalId>,
    pub outputs: Vec<GlobalId>,
    pub name: String
}

impl DataflowDescription {
    pub fn new(name:String) -> Self {
        Self {
            objects_to_build: Vec::new(),
            inputs: Vec::new(),
            outputs: Vec::new(),
            name
        }
    }
    pub fn new_object(&mut self, id: GlobalId, plan: TypedPlan) {
        self.objects_to_build.push(BuildDesc { id, plan })
    }
}
