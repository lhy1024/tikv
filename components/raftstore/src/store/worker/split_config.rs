// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use configuration::{rollback_or, ConfigChange, ConfigManager, Configuration, RollbackCollector};
use std::sync::Arc;
use tikv_util::config::VersionTrack;

const DEFAULT_DETECT_TIMES: u64 = 10;
const DEFAULT_SAMPLE_THRESHOLD: i32 = 100;
pub(crate) const DEFAULT_SAMPLE_NUM: usize = 20;
const DEFAULT_QPS_THRESHOLD: usize = 1500;
const DEFAULT_SPLIT_BALANCE_SCORE: f64 = 0.25;
const DEFAULT_SPLIT_CONTAINED_SCORE: f64 = 0.5;

#[serde(default)]
#[serde(rename_all = "kebab-case")]
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Configuration)]
pub struct SplitHubConfig {
    pub qps_threshold: usize,
    pub split_balance_score: f64,
    pub split_contained_score: f64,
    pub detect_times: u64,
    pub sample_num: usize,
    pub sample_threshold: i32,
}

impl Default for SplitHubConfig {
    fn default() -> SplitHubConfig {
        SplitHubConfig {
            qps_threshold: DEFAULT_QPS_THRESHOLD,
            split_balance_score: DEFAULT_SPLIT_BALANCE_SCORE,
            split_contained_score: DEFAULT_SPLIT_CONTAINED_SCORE,
            detect_times: DEFAULT_DETECT_TIMES,
            sample_num: DEFAULT_SAMPLE_NUM,
            sample_threshold: DEFAULT_SAMPLE_THRESHOLD,
        }
    }
}

impl SplitHubConfig {
    pub fn validate(&self) -> std::result::Result<(), Box<dyn std::error::Error>> {
        self.validate_or_rollback(None)
    }

    pub fn validate_or_rollback(
        &self,
        mut rb_collector: Option<RollbackCollector<SplitHubConfig>>,
    ) -> std::result::Result<(), Box<dyn std::error::Error>> {
        if self.split_balance_score > 1.0
            || self.split_balance_score < 0.0
            || self.split_contained_score > 1.0
            || self.split_contained_score < 0.0
        {
            rollback_or!(rb_collector, split_balance_score, {
                Err(
                    ("split_balance_score or split_contained_score should be between 0 and 1.")
                        .into(),
                )
            })
        }

        Ok(())
    }
}

#[derive(Clone, Default)]
pub struct SplitHubConfigManager(pub Arc<VersionTrack<SplitHubConfig>>);

impl ConfigManager for SplitHubConfigManager {
    fn dispatch(
        &mut self,
        change: ConfigChange,
    ) -> std::result::Result<(), Box<dyn std::error::Error>> {
        {
            let change = change.clone();
            self.0
                .update(move |cfg: &mut SplitHubConfig| cfg.update(change));
        }
        info!(
            "split hub config changed";
            "change" => ?change,
        );
        Ok(())
    }
}

impl std::ops::Deref for SplitHubConfigManager {
    type Target = Arc<VersionTrack<SplitHubConfig>>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
