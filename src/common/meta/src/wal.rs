// Copyright 2023 Greptime Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::default;

use serde::{Deserialize, Serialize};

pub use crate::wal::kafka::KafkaConfig;

pub mod kafka;
pub mod region_wal_options;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, Default)]
#[serde(tag = "provider")]
pub enum WalConfig {
    #[default]
    #[serde(rename = "raft-engine")]
    RaftEngine,
    #[serde(rename = "kafka")]
    Kafka(KafkaConfig),
}
