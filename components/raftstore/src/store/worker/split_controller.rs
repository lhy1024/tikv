// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use std::cmp::{max, Ordering};
use std::collections::HashSet;

use std::slice::Iter;
use std::time::{Duration, SystemTime};

use crate::store::util::{KeyRange, ReservoirSampling, SampleStatus};
use kvproto::metapb::Peer;

use rand::Rng;

use collections::HashMap;
use tikv_util::config::Tracker;

use crate::store::metrics::*;
use crate::store::worker::split_config::DEFAULT_SAMPLE_NUM;
use crate::store::worker::{FlowStatistics, SplitConfig, SplitConfigManager};

pub struct SplitInfo {
    pub region_id: u64,
    pub split_key: Option<Vec<u8>>,
    pub peer: Peer,
}
#[derive(Debug)]
pub struct Sample {
    pub key: Vec<u8>,
    pub left: i32,
    pub contained: i32,
    pub right: i32,
}

impl Sample {
    fn new(key: &[u8]) -> Sample {
        Sample {
            key: key.to_owned(),
            left: 0,
            contained: 0,
            right: 0,
        }
    }
}

// It will return prefix sum of iter. `read` is a function to be used to read data from iter.
fn prefix_sum<F, T>(iter: Iter<T>, read: F) -> Vec<usize>
where
    F: Fn(&T) -> usize,
{
    let mut pre_sum = vec![];
    let mut sum = 0;
    for item in iter {
        sum += read(&item);
        pre_sum.push(sum);
    }
    pre_sum
}

// It will return sample_num numbers by sample from lists.
// The list in the lists has the length of N1, N2, N3 ... Np ... NP in turn.
// Their prefix sum is pre_sum and we can get mut list from lists by get_mut.
// Take a random number d from [1, N]. If d < N1, select a data in the first list with an equal probability without replacement;
// If N1 <= d <(N1 + N2), then select a data in the second list with equal probability without replacement;
// and so on, repeat m times, and finally select sample_num pieces of data from lists.
fn sample<F, T>(
    sample_num: usize,
    pre_sum: &[usize],
    mut lists: Vec<T>,
    get_mut: F,
) -> Vec<KeyRange>
where
    F: Fn(&mut T) -> &mut Vec<KeyRange>,
{
    let mut rng = rand::thread_rng();
    let mut key_ranges = vec![];
    let high_bound = *pre_sum.last().unwrap();
    for _ in 0..high_bound {
        let d = rng.gen_range(0, high_bound) as usize;
        let i = match pre_sum.binary_search(&d) {
            Ok(i) => i,
            Err(i) => i,
        };
        if i < lists.len() {
            let list = get_mut(&mut lists[i]);
            if !list.is_empty() {
                let j = rng.gen_range(0, list.len()) as usize;
                key_ranges.push(list.remove(j)); // Sampling without replacement
            }
        }
        if key_ranges.len() == sample_num {
            break;
        }
    }
    key_ranges
}

// RegionInfo will maintain key_ranges with sample_num length by reservoir sampling.
// And it will save qps num and peer.
#[derive(Debug, Clone)]
pub struct RegionInfo {
    pub peer: Peer,
    pub approximate_size: u64,
    pub approximate_key: u64,
    pub flow: FlowStatistics,
    pub key_ranges: ReservoirSampling<KeyRange>,
    pub max_processed_keys: usize,
}

impl RegionInfo {
    fn new(sample_num: usize) -> RegionInfo {
        RegionInfo {
            peer: Peer::default(),
            approximate_size: 0,
            approximate_key: 0,
            flow: FlowStatistics::default(),
            key_ranges: ReservoirSampling::new(sample_num),
            max_processed_keys: 0,
        }
    }

    fn get_qps(&self) -> usize {
        self.key_ranges.total
    }

    fn get_key_ranges_mut(&mut self) -> &mut Vec<KeyRange> {
        &mut self.key_ranges.results
    }

    fn add_key_ranges(&mut self, key_ranges: Vec<KeyRange>, status: Option<SampleStatus>) {
        for key_range in key_ranges {
            if let Some(keys) = key_range.processed_keys_num {
                self.update_max_processed_keys(keys);
            }
            match &status {
                Some(status) => self.key_ranges.commit(status, key_range),
                None => self.key_ranges.stream(key_range),
            }
        }
    }

    fn update_max_processed_keys(&mut self, processed_keys: usize) {
        self.max_processed_keys = max(self.max_processed_keys, processed_keys);
    }

    fn set_peer(&mut self, peer: &Peer) {
        if self.peer != *peer {
            self.peer = peer.clone();
        }
    }
}

pub struct RegionInfos {
    pub qps: usize,
    pub infos: Vec<RegionInfo>,
    pub max_processed_keys: usize,
    pub approximate_keys: u64,
    pub approximate_size: u64,
}

impl RegionInfos {
    pub fn new() -> RegionInfos {
        RegionInfos {
            infos: vec![],
            qps: 0,
            max_processed_keys: 0,
            approximate_keys: 0,
            approximate_size: 0,
        }
    }
    pub fn push(&mut self, info: RegionInfo) {
        self.max_processed_keys = max(self.max_processed_keys, info.max_processed_keys);
        self.approximate_size = max(self.approximate_size, info.approximate_size);
        self.approximate_keys = max(self.approximate_keys, info.approximate_key);
        self.qps += info.get_qps();
        self.infos.push(info);
    }

    pub fn get_peer(&self) -> Peer {
        self.infos[0].peer.clone()
    }
}

pub struct Recorder {
    pub detect_num: u64,
    pub key_ranges: Vec<Vec<KeyRange>>,
    pub times: u64,
    pub create_time: SystemTime,
}

impl Recorder {
    fn new(detect_num: u64) -> Recorder {
        Recorder {
            detect_num,
            key_ranges: vec![],
            times: 0,
            create_time: SystemTime::now(),
        }
    }

    fn record(&mut self, key_ranges: Vec<KeyRange>) {
        self.times += 1;
        self.key_ranges.push(key_ranges);
    }

    fn is_ready(&self) -> bool {
        self.times >= self.detect_num
    }

    fn collect(&mut self, config: &SplitConfig) -> Option<Vec<u8>> {
        let pre_sum = prefix_sum(self.key_ranges.iter(), Vec::len);
        let key_ranges = self.key_ranges.clone();
        let mut samples: Vec<Sample> = sample(config.sample_num, &pre_sum, key_ranges, |x| x)
            .iter()
            .fold(HashSet::new(), |mut set, key_range| {
                set.insert(&key_range.start_key);
                set.insert(&key_range.end_key);
                if let Some(scan_sample_keys) = &key_range.scan_sample_keys {
                    for key in scan_sample_keys {
                        set.insert(&key);
                    }
                };
                set
            })
            .into_iter()
            .map(|key| Sample::new(key))
            .collect();
        for key_ranges in &self.key_ranges {
            for key_range in key_ranges {
                Recorder::sample(&mut samples, &key_range);
            }
        }
        Recorder::split_key(
            &samples,
            config.split_balance_score,
            config.split_contained_score,
            config.sample_threshold,
        )
    }

    fn sample(samples: &mut Vec<Sample>, key_range: &KeyRange) {
        for mut sample in samples.iter_mut() {
            let order_start = if key_range.start_key.is_empty() {
                Ordering::Greater
            } else {
                sample.key.cmp(&key_range.start_key)
            };

            let order_end = if key_range.end_key.is_empty() {
                Ordering::Less
            } else {
                sample.key.cmp(&key_range.end_key)
            };

            if order_start == Ordering::Greater && order_end == Ordering::Less {
                sample.contained += 1;
            } else if order_start != Ordering::Greater {
                sample.right += 1;
            } else {
                sample.left += 1;
            }
        }
    }

    fn split_key(
        samples: &[Sample],
        split_balance_score_threshold: f64,
        _split_contained_score: f64,
        sample_threshold: i32,
    ) -> Option<Vec<u8>> {
        let mut best_index: i32 = -1;
        let mut best_score = 2.0;
        for (index, sample) in samples.iter().enumerate() {
            let sampled = sample.contained + sample.left + sample.right;
            if sampled < sample_threshold {
                continue;
            }

            let balance_score = if sample.left + sample.right == 0 {
                0.0
            } else {
                (sample.left - sample.right).abs() as f64 / (sample.left + sample.right) as f64
            };

            if balance_score >= split_balance_score_threshold {
                continue;
            }

            let contained_score = 1.0 - sample.contained as f64 / sampled as f64;

            let final_score = balance_score + contained_score;

            if best_score > final_score
                || ((best_score - final_score).abs() < f64::EPSILON
                    && sample.key.cmp(&samples[best_index as usize].key) == Ordering::Less)
            {
                best_index = index as i32;
                best_score = final_score;
            }
        }
        if best_index >= 0 {
            return Some(samples[best_index as usize].key.clone());
        }
        None
    }
}

#[derive(Clone, Debug)]
pub struct ReadStats {
    pub region_infos: HashMap<u64, RegionInfo>,
    pub sample_num: usize,
}

impl ReadStats {
    fn get_or_insert(&mut self, region_id: u64) -> &mut RegionInfo {
        let num = self.sample_num;
        self.region_infos
            .entry(region_id)
            .or_insert_with(|| RegionInfo::new(num))
    }

    pub fn add_qps(
        &mut self,
        region_id: u64,
        peer: &Peer,
        key_range: KeyRange,
        status: Option<SampleStatus>,
    ) {
        self.add_qps_batch(region_id, peer, vec![key_range], status);
    }

    pub fn add_qps_batch(
        &mut self,
        region_id: u64,
        peer: &Peer,
        key_ranges: Vec<KeyRange>,
        status: Option<SampleStatus>,
    ) {
        let region_info = self.get_or_insert(region_id);
        region_info.set_peer(peer);
        region_info.add_key_ranges(key_ranges, status);
    }

    pub fn add_flow(&mut self, region_id: u64, write: &FlowStatistics, data: &FlowStatistics) {
        let region_info = self.get_or_insert(region_id);
        region_info.flow.add(write);
        region_info.flow.add(data);
    }

    pub fn get_sample_status(&mut self, region_id: u64) -> SampleStatus {
        let region_info = self.get_or_insert(region_id);
        region_info.key_ranges.prepare()
    }

    pub fn update_max_processed_keys(&mut self, region_id: u64, processed_keys: usize) {
        let region_info = self.get_or_insert(region_id);
        region_info.update_max_processed_keys(processed_keys)
    }

    pub fn is_empty(&self) -> bool {
        self.region_infos.is_empty()
    }

    // for test
    pub fn set_all_key(&mut self, approximate_key: u64) {
        for (_, region_info) in self.region_infos.iter_mut() {
            region_info.approximate_key = approximate_key;
        }
    }

    // for test
    pub fn set_all_size(&mut self, approximate_size: u64) {
        for (_, region_info) in self.region_infos.iter_mut() {
            region_info.approximate_size = approximate_size;
        }
    }
}

impl Default for ReadStats {
    fn default() -> ReadStats {
        ReadStats {
            sample_num: DEFAULT_SAMPLE_NUM,
            region_infos: HashMap::default(),
        }
    }
}

pub struct AutoSplitController {
    pub recorders: HashMap<u64, Recorder>,
    cfg: SplitConfig,
    cfg_tracker: Tracker<SplitConfig>,
}

impl AutoSplitController {
    pub fn new(config_manager: SplitConfigManager) -> AutoSplitController {
        AutoSplitController {
            recorders: HashMap::default(),
            cfg: config_manager.value().clone(),
            cfg_tracker: config_manager.0.clone().tracker("split_hub".to_owned()),
        }
    }

    pub fn default() -> AutoSplitController {
        AutoSplitController::new(SplitConfigManager::default())
    }

    fn collect_read_stats(&self, read_stats_vec: Vec<ReadStats>) -> HashMap<u64, RegionInfos> {
        // collect from different thread
        let mut region_infos_map = HashMap::default(); // regionID-regionInfos
        for read_stats in read_stats_vec {
            for (region_id, region_info) in read_stats.region_infos {
                let region_infos = region_infos_map
                    .entry(region_id)
                    .or_insert_with(RegionInfos::new);
                region_infos.push(region_info);
            }
        }
        region_infos_map
    }

    pub fn flush(&mut self, read_stats_vec: Vec<ReadStats>) -> Vec<SplitInfo> {
        let mut split_infos = Vec::default();
        let region_infos_map = self.collect_read_stats(read_stats_vec);

        for (region_id, region_infos) in region_infos_map {
            let pre_sum = prefix_sum(region_infos.infos.iter(), RegionInfo::get_qps); // region_infos is not empty
            let qps = region_infos.qps;
            for num in &pre_sum {
                info!("qps pre_sum";"pre_sum"=>*num);
            }
            info!("qps";"qps"=>qps);
            if qps < self.cfg.qps_threshold {
                if self.recorders.contains_key(&region_id) {
                    READ_QPS_TOPN
                        .with_label_values(&[&region_id.to_string()])
                        .set(0.0);
                }
                self.recorders.remove(&region_id);
                continue;
            }
            LOAD_BASE_SPLIT_EVENT.with_label_values(&["qps_fit"]).inc();
            READ_QPS_TOPN
                .with_label_values(&[&region_id.to_string()])
                .set(qps as f64);

            let approximate_keys = region_infos.approximate_keys;
            let approximate_size = region_infos.approximate_size;
            let max_processed_keys = region_infos.max_processed_keys;
            info!(
                "split params";
                "region_id"=>region_id,
                "approximate size"=>approximate_size,
                "processed keys"=>max_processed_keys,
                "approximate keys"=>approximate_keys,
                "qps"=>qps
            );
            if max_processed_keys > (region_infos.approximate_keys / 10) as usize {
                LOAD_BASE_SPLIT_EVENT
                    .with_label_values(&["hit_copr_keys"])
                    .inc();
            } else if approximate_size < self.cfg.size_threshold
                && approximate_keys < self.cfg.key_threshold
            {
                LOAD_BASE_SPLIT_EVENT
                    .with_label_values(&["size_or_keys_too_small"])
                    .inc();
                continue;
            }

            let peer = region_infos.get_peer(); //todo peer 的隐患
                                                //let pre_sum = prefix_sum(region_infos.infos.iter(), RegionInfo::get_qps);
            let num = self.cfg.detect_times;
            let recorder = self
                .recorders
                .entry(region_id)
                .or_insert_with(|| Recorder::new(num));

            let key_ranges = sample(
                self.cfg.sample_num,
                &pre_sum,
                region_infos.infos,
                RegionInfo::get_key_ranges_mut,
            );

            recorder.record(key_ranges);
            if recorder.is_ready() {
                info!(
                    "load base split region";
                    "region_id"=>region_id,
                    "approximate size"=>approximate_size,
                    "processed keys"=>max_processed_keys,
                    "approximate keys"=>approximate_keys,
                    "qps"=>qps
                );
                let split_key = recorder.collect(&self.cfg);
                if split_key.is_none() {
                    LOAD_BASE_SPLIT_EVENT
                        .with_label_values(&["split_by_half"])
                        .inc();
                };
                split_infos.push(SplitInfo {
                    region_id,
                    peer,
                    split_key,
                });
                self.recorders.remove(&region_id);
                READ_QPS_TOPN
                    .with_label_values(&[&region_id.to_string()])
                    .set(0.0);
                LOAD_BASE_SPLIT_EVENT
                    .with_label_values(&["prepare_to_split"])
                    .inc();
            }
        }
        split_infos
    }

    pub fn clear(&mut self) {
        let interval = Duration::from_secs(self.cfg.detect_times * 2);
        self.recorders
            .retain(|_, recorder| match recorder.create_time.elapsed() {
                Ok(life_time) => life_time < interval,
                Err(_) => true,
            });
    }

    pub fn refresh_cfg(&mut self) {
        if let Some(incoming) = self.cfg_tracker.any_new() {
            self.cfg = incoming.clone();
        }
    }
}

// #[cfg(test)]
// mod tests {
//     use super::*;
//     use crate::store::util::build_key_range;
//     use txn_types::Key;

//     enum Position {
//         Left,
//         Right,
//         Contained,
//     }

//     impl Sample {
//         fn num(&self, pos: Position) -> i32 {
//             match pos {
//                 Position::Left => self.left,
//                 Position::Right => self.right,
//                 Position::Contained => self.contained,
//             }
//         }
//     }

//     struct SampleCase {
//         key: Vec<u8>,
//     }

//     impl SampleCase {
//         fn sample_key(&self, start_key: &[u8], end_key: &[u8], pos: Position) {
//             let mut samples = vec![Sample::new(&self.key)];
//             let key_range = build_key_range(start_key, end_key, false);
//             Recorder::sample(&mut samples, &key_range);
//             assert_eq!(
//                 samples[0].num(pos),
//                 1,
//                 "start_key is {:?}, end_key is {:?}",
//                 String::from_utf8(Vec::from(start_key)).unwrap(),
//                 String::from_utf8(Vec::from(end_key)).unwrap()
//             );
//         }
//     }

//     #[test]
//     fn test_pre_sum() {
//         let v = vec![1, 2, 3, 4, 5, 6, 7, 8, 9];
//         let expect = vec![1, 3, 6, 10, 15, 21, 28, 36, 45];
//         let pre = prefix_sum(v.iter(), |x| *x);
//         for i in 0..v.len() {
//             assert_eq!(expect[i], pre[i]);
//         }
//     }

//     #[test]
//     fn test_sample() {
//         let sc = SampleCase { key: vec![b'c'] };

//         // limit scan
//         sc.sample_key(b"a", b"b", Position::Left);
//         sc.sample_key(b"a", b"c", Position::Left);
//         sc.sample_key(b"a", b"d", Position::Contained);
//         sc.sample_key(b"c", b"d", Position::Right);
//         sc.sample_key(b"d", b"e", Position::Right);

//         // point get
//         sc.sample_key(b"a", b"a", Position::Left);
//         sc.sample_key(b"c", b"c", Position::Right); // when happened 100 times (a,a) and 100 times (c,c), we will split from c.
//         sc.sample_key(b"d", b"d", Position::Right);

//         // unlimited scan
//         sc.sample_key(b"", b"", Position::Contained);
//         sc.sample_key(b"a", b"", Position::Contained);
//         sc.sample_key(b"c", b"", Position::Right);
//         sc.sample_key(b"d", b"", Position::Right);
//         sc.sample_key(b"", b"a", Position::Left);
//         sc.sample_key(b"", b"c", Position::Left);
//         sc.sample_key(b"", b"d", Position::Contained);
//     }

//     fn gen_read_stats(region_id: u64, key_ranges: Vec<KeyRange>) -> ReadStats {
//         let mut qps_stats = ReadStats::default();
//         for key_range in &key_ranges {
//             qps_stats.add_qps(region_id, &Peer::default(), key_range.clone());
//         }
//         for (_, region_info) in qps_stats.region_infos.iter_mut() {
//             region_info.approximate_key = SplitConfig::default().key_threshold;
//             region_info.approximate_size = SplitConfig::default().size_threshold;
//         }
//         qps_stats
//     }

//     #[test]
//     fn test_split() {
//         // check_split_key(
//         //     vec![
//         //         build_key_range(b"a", b"b", false),
//         //         build_key_range(b"b", b"c", false),
//         //     ],
//         //     b"b",
//         //     "average split by raw key",
//         // );

//         // let key_a = Key::from_raw(b"0080").append_ts(2.into());
//         // let key_b = Key::from_raw(b"0160").append_ts(2.into());
//         // let key_c = Key::from_raw(b"0240").append_ts(2.into());
//         // check_split_key(
//         //     vec![
//         //         build_key_range(key_a.as_encoded(), key_b.as_encoded(), false),
//         //         build_key_range(key_b.as_encoded(), key_c.as_encoded(), false),
//         //     ],
//         //     key_b.as_encoded(),
//         //     "average split by encoded key",
//         // );

//         for _i in 0..100 {
//             // test unstable
//             // check_split_key(
//             //     vec![
//             //         build_key_range(b"a", b"k", false),
//             //         build_key_range(b"b", b"j", false),
//             //         build_key_range(b"c", b"i", false),
//             //         build_key_range(b"d", b"h", false),
//             //         build_key_range(b"e", b"g", false),
//             //         build_key_range(b"f", b"f", false),
//             //     ],
//             //     b"f",
//             //     "isosceles triangle",
//             // );

//             // check_split_key(
//             //     vec![
//             //         build_key_range(b"a", b"f", false),
//             //         build_key_range(b"b", b"g", false),
//             //         build_key_range(b"c", b"h", false),
//             //         build_key_range(b"d", b"i", false),
//             //         build_key_range(b"e", b"j", false),
//             //         build_key_range(b"f", b"k", false),
//             //     ],
//             //     b"f",
//             //     "parallelogram",
//             // );

//             // check_split_key(
//             //     vec![
//             //         build_key_range(b"a", b"l", false),
//             //         build_key_range(b"a", b"m", false),
//             //     ],
//             //     b"l",
//             //     "right-angle trapezoid 1",
//             // );

//             // check_split_key(
//             //     vec![
//             //         build_key_range(b"a", b"l", false),
//             //         build_key_range(b"b", b"l", false),
//             //     ],
//             //     b"b",
//             //     "right-angle trapezoid 2",
//             // );

//             // check_split_key(
//             //     vec![
//             //         build_key_range(b"a", b"a", false),
//             //         build_key_range(b"a", b"b", false),
//             //         build_key_range(b"a", b"c", false),
//             //         build_key_range(b"a", b"d", false),
//             //         build_key_range(b"a", b"e", false),
//             //         build_key_range(b"a", b"f", false),
//             //         build_key_range(b"a", b"g", false),
//             //         build_key_range(b"a", b"h", false),
//             //         build_key_range(b"a", b"i", false),
//             //     ], // todo no suitable key in less lines
//             //     b"b",
//             //     "right-angle triangle 1",
//             // );

//             // check_split_key(
//             //     vec![
//             //         build_key_range(b"a", b"f", false),
//             //         build_key_range(b"b", b"f", false),
//             //         build_key_range(b"c", b"f", false),
//             //         build_key_range(b"d", b"f", false),
//             //         build_key_range(b"e", b"f", false),
//             //         build_key_range(b"f", b"f", false),
//             //     ],
//             //     b"e",
//             //     "right-angle triangle 2",
//             // );
//         }
//     }

//     #[test]
//     fn test_sample_key_num() {
//         let mut hub = AutoSplitController::new(SplitConfigManager::default());
//         hub.cfg.qps_threshold = 2000;
//         hub.cfg.sample_num = 2000;
//         hub.cfg.sample_threshold = 0;
//         hub.cfg.key_threshold = 0;
//         hub.cfg.size_threshold = 0;

//         for _ in 0..100 {
//             // qps_stats_vec contains 2000 qps and a readStats with a key range;
//             let mut qps_stats_vec = vec![];

//             let mut qps_stats = ReadStats::default();
//             qps_stats.add_qps(1, &Peer::default(), build_key_range(b"a", b"b", false));
//             qps_stats_vec.push(qps_stats);

//             let mut qps_stats = ReadStats::default();
//             for _ in 0..2000 {
//                 qps_stats.add_qps(1, &Peer::default(), build_key_range(b"b", b"c", false));
//             }
//             qps_stats_vec.push(qps_stats);
//             hub.flush(qps_stats_vec);
//         }
//     }

//     // fn check_split_key(key_ranges: Vec<KeyRange>, split_key: &[u8], case_name: &str) {
//     //     let mut hub = AutoSplitController::new(SplitConfigManager::default());
//     #[test]
//     fn test_hub() {
//         // raw key mode
//         let raw_key_ranges = vec![
//             build_key_range(b"a", b"b", false),
//             build_key_range(b"b", b"c", false),
//         ];
//         check_split(
//             b"raw key",
//             vec![gen_read_stats(1, raw_key_ranges.clone())],
//             vec![b"b"],
//             false,
//         );

//         // encoded key mode
//         let key_a = Key::from_raw(b"0080").append_ts(2.into());
//         let key_b = Key::from_raw(b"0160").append_ts(2.into());
//         let key_c = Key::from_raw(b"0240").append_ts(2.into());
//         let encoded_key_ranges = vec![
//             build_key_range(key_a.as_encoded(), key_b.as_encoded(), false),
//             build_key_range(key_b.as_encoded(), key_c.as_encoded(), false),
//         ];
//         check_split(
//             b"encoded key",
//             vec![gen_read_stats(1, encoded_key_ranges.clone())],
//             vec![key_b.as_encoded()],
//             false,
//         );

//         // mix mode
//         check_split(
//             b"mix key",
//             vec![
//                 gen_read_stats(1, raw_key_ranges),
//                 gen_read_stats(2, encoded_key_ranges),
//             ],
//             vec![b"b", key_b.as_encoded()],
//             false,
//         );
//     }

//     fn check_split(mode: &[u8], qps_stats: Vec<ReadStats>, split_keys: Vec<&[u8]>, failed: bool) {
//         let mut hub = AutoSplitController::default();
//         hub.cfg.qps_threshold = 1;
//         hub.cfg.sample_threshold = 0;

//         for i in 0..10 {
//             let split_infos = hub.flush(qps_stats.clone());
//             if (i + 1) % hub.cfg.detect_times == 0 {
//                 if failed {
//                     assert!(split_infos.is_empty());
//                 } else {
//                     for obtain in &split_infos {
//                         let mut equal = false;
//                         for expect in &split_keys {
//                             if let Some(obtain) = &obtain.split_key {
//                                 if obtain.cmp(&expect.to_vec()) == Ordering::Equal {
//                                     equal = true;
//                                     break;
//                                 }
//                             }
//                         }
//                         assert!(
//                             equal,
//                             "mode: {:?}",
//                             String::from_utf8(Vec::from(mode)).unwrap()
//                         );
//                     }
//                     // assert_eq!(
//                     //     key.clone(),
//                     //     split_key,
//                     //     "case {:?}, obtained key is {:?}, expect key is {:?}",
//                     //     case_name,
//                     //     String::from_utf8(key.clone()).unwrap(),
//                     //     String::from_utf8(Vec::from(split_key)).unwrap()
//                     // );
//                 }
//             }
//         }
//     }
//     #[test]
//     fn test_threshold() {
//         // test size or key threshold
//         let cfg = SplitConfig::default();
//         let raw_key_ranges = vec![
//             build_key_range(b"a", b"b", false),
//             build_key_range(b"b", b"c", false),
//         ];
//         let mut qps_stats = gen_read_stats(1, raw_key_ranges);
//         check_split(b"raw key", vec![qps_stats.clone()], vec![b"b"], false);

//         qps_stats.set_all_key(cfg.key_threshold);
//         qps_stats.set_all_size(cfg.size_threshold - 1);
//         check_split(b"raw key", vec![qps_stats.clone()], vec![b"b"], false);

//         qps_stats.set_all_key(cfg.key_threshold - 1);
//         qps_stats.set_all_size(cfg.size_threshold);
//         check_split(b"raw key", vec![qps_stats.clone()], vec![b"b"], false);

//         qps_stats.set_all_key(cfg.key_threshold - 1);
//         qps_stats.set_all_size(cfg.size_threshold - 1);
//         check_split(b"raw key", vec![qps_stats], vec![], true);
//     }

//     const REGION_NUM: u64 = 1000;
//     const KEY_RANGE_NUM: u64 = 1000;

//     fn default_qps_stats() -> ReadStats {
//         let mut qps_stats = ReadStats::default();
//         for i in 0..REGION_NUM {
//             for _j in 0..KEY_RANGE_NUM {
//                 qps_stats.add_qps(i, &Peer::default(), build_key_range(b"a", b"b", false))
//             }
//         }
//         qps_stats
//     }

//     #[bench]
//     fn recorder_sample(b: &mut test::Bencher) {
//         let mut samples = vec![Sample::new(b"c")];
//         let key_range = build_key_range(b"a", b"b", false);
//         b.iter(|| {
//             Recorder::sample(&mut samples, &key_range);
//         });
//     }

//     #[bench]
//     fn hub_flush(b: &mut test::Bencher) {
//         let mut other_qps_stats = vec![];
//         for _i in 0..10 {
//             other_qps_stats.push(default_qps_stats());
//         }
//         b.iter(|| {
//             let mut hub = AutoSplitController::default();
//             hub.flush(other_qps_stats.clone());
//         });
//     }

//     #[bench]
//     fn qps_scan(b: &mut test::Bencher) {
//         let mut qps_stats = default_qps_stats();
//         let start_key = Key::from_raw(b"a");
//         let end_key = Some(Key::from_raw(b"b"));

//         b.iter(|| {
//             if let Ok(start_key) = start_key.to_owned().into_raw() {
//                 let mut key = vec![];
//                 if let Some(end_key) = &end_key {
//                     if let Ok(end_key) = end_key.to_owned().into_raw() {
//                         key = end_key;
//                     }
//                 }
//                 qps_stats.add_qps(
//                     1,
//                     &Peer::default(),
//                     build_key_range(&start_key, &key, false),
//                 );
//             }
//         });
//     }

//     #[bench]
//     fn qps_add(b: &mut test::Bencher) {
//         let mut qps_stats = default_qps_stats();
//         b.iter(|| {
//             qps_stats.add_qps(1, &Peer::default(), build_key_range(b"a", b"b", false));
//         });
//     }
// }
