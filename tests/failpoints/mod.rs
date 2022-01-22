use fail::{fail_point, FailScenario};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::io;
use std::rc::Rc;
use tiempodb::partition::PartitionManager;
use tiempodb::storage::DataPoint;

fn generate_metric(metric_name: &str) -> HashMap<Rc<str>, Vec<DataPoint>> {
    let mut data = HashMap::new();
    let metric_name: Rc<str> = Rc::from(metric_name);
    data.insert(
        metric_name.clone(),
        vec![DataPoint::new(metric_name.clone(), 100u64, 200i64)],
    );
    data
}

#[test]
fn test_recoverable_partition_failure() -> io::Result<()> {
    let scenario = FailScenario::setup();
    fail::cfg("pm-roll-rename-step", "return").unwrap();

    let tempdir = tempfile::tempdir().unwrap();
    let mut manager = PartitionManager::new(&tempdir.path())?;
    let mut metrics = generate_metric("first");
    assert_eq!(true, manager.roll_new_partition(&mut metrics).is_err());

    fail::cfg("pm-roll-rename-step", "off").unwrap();

    let mut metrics = generate_metric("second");
    manager.roll_new_partition(&mut metrics)?;

    assert_eq!(2, manager.partitions.len());

    let metrics: HashSet<String> = manager
        .partitions
        .iter()
        .flat_map(|p| p.metrics.iter().map(|m| m.metric_name.clone()))
        .collect();

    assert_eq!(
        metrics,
        vec!["first".to_string(), "second".to_string()]
            .into_iter()
            .collect()
    );

    scenario.teardown();
    Ok(())
}

#[test]
fn test_partition_failure_data_loss() -> io::Result<()> {
    let scenario = FailScenario::setup();
    fail::cfg("pm-roll-write-meta-step", "return").unwrap();

    let tempdir = tempfile::tempdir().unwrap();
    let mut manager = PartitionManager::new(&tempdir.path())?;
    let mut metrics = generate_metric("first");
    assert_eq!(true, manager.roll_new_partition(&mut metrics).is_err());

    fail::cfg("pm-roll-write-meta-step", "off").unwrap();

    let mut metrics = generate_metric("second");
    manager.roll_new_partition(&mut metrics)?;

    let metrics: HashSet<String> = manager
        .partitions
        .iter()
        .flat_map(|p| p.metrics.iter().map(|m| m.metric_name.clone()))
        .collect();

    assert_eq!(metrics, vec!["second".to_string()].into_iter().collect());
    scenario.teardown();
    Ok(())
}
