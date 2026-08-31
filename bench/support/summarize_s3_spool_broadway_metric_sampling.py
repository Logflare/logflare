#!/usr/bin/env python3
"""Validate and summarize mirrored Broadway per-message metric sampling logs."""

import argparse
import json
import math
import re
import statistics
from collections import defaultdict
from pathlib import Path

FIELD_RE = re.compile(r"(?:^|\s)([a-z_]+)=([^\s]+)")
FILE_RE = re.compile(r"^sampling_(none|10000|1000|100|10|1)_([a-z])\.log$")
CONDITIONS = ("none", "10000", "1000", "100", "10", "1")
LOW_FREQUENCY_EVENTS = (
    "broadway.batch_processor.stop",
    "broadway.batcher.stop",
    "broadway.processor.stop",
)
MESSAGE_EVENT = "broadway.processor.message.stop"
LOW_FREQUENCY_METRICS = tuple(f"{event}.duration" for event in LOW_FREQUENCY_EVENTS)
MESSAGE_METRIC = f"{MESSAGE_EVENT}.duration"
CONFIG_KEYS = (
    "condition",
    "compiled_variant",
    "handler_store",
    "handler_backend",
    "manual_batch_flush",
    "broadway",
    "telemetry",
    "otp",
    "schedulers",
    "scheduler_wall_time",
    "lock_counting",
    "events",
    "warmups",
    "trials",
    "payload_bytes",
    "batch_timeout_ms",
    "format",
    "compress",
    "storage",
)
COMPARISON_METRICS = (
    "events_per_second",
    "reductions_per_event",
    "scheduler_utilization_percent",
    "events_per_active_scheduler_second",
)
PERFORMANCE_BUDGET = {
    "aggregate_median_throughput_loss_percent_at_least": -5.0,
    "aggregate_median_reductions_per_event_increase_percent_at_most": 5.0,
}


def fields(line: str) -> dict[str, str]:
    return dict(FIELD_RE.findall(line))


def counts(value: str, path: Path) -> dict[str, int]:
    result: dict[str, int] = {}
    for item in value.split(","):
        try:
            name, count = item.rsplit(":", 1)
            result[name] = int(count)
        except ValueError as error:
            raise ValueError(f"{path}: invalid metric count {item!r}") from error
    if len(result) != len(value.split(",")):
        raise ValueError(f"{path}: duplicate metric count")
    return result


def one(lines: list[str], prefix: str, path: Path) -> str:
    found = [line for line in lines if line.startswith(prefix)]
    if len(found) != 1:
        raise ValueError(f"{path}: expected one {prefix.strip()} line, found {len(found)}")
    return found[0]


def sampling_evidence(
    denominator: str, total_messages: int, message_observations: int, path: Path
) -> dict[str, float | int | None]:
    if denominator == "none":
        if message_observations != 0:
            raise ValueError(f"{path}: no-message condition recorded message observations")
        return {
            "expected_message_observations": 0,
            "observation_tolerance": 0,
            "actual_message_observations": message_observations,
            "expected_message_sample_rate": 0.0,
            "actual_message_sample_rate": 0.0,
        }

    sample_denominator = int(denominator)
    if sample_denominator == 1:
        if message_observations != total_messages:
            raise ValueError(f"{path}: denominator=1 must retain every message")
        return {
            "expected_message_observations": total_messages,
            "observation_tolerance": 0,
            "actual_message_observations": message_observations,
            "expected_message_sample_rate": 1.0,
            "actual_message_sample_rate": 1.0,
        }

    probability = 1.0 / sample_denominator
    expected = total_messages * probability
    tolerance = max(6 * math.sqrt(total_messages * probability * (1 - probability)), expected * 0.05, 1)

    if abs(message_observations - expected) > tolerance:
        raise ValueError(
            f"{path}: sampled message observations={message_observations}, expected={expected:.4f}, "
            f"tolerance={tolerance:.4f}"
        )

    return {
        "expected_message_observations": expected,
        "observation_tolerance": tolerance,
        "actual_message_observations": message_observations,
        "expected_message_sample_rate": probability,
        "actual_message_sample_rate": message_observations / total_messages,
    }


def parse(
    path: Path,
) -> tuple[str, str, dict[str, str], list[dict[str, float | int]], dict[str, float | int | None]]:
    match = FILE_RE.fullmatch(path.name)
    if match is None:
        raise ValueError(f"{path}: unexpected filename")
    denominator, block = match.groups()
    lines = path.read_text().splitlines()
    config = fields(one(lines, "config ", path))
    handlers = fields(one(lines, "handlers ", path))
    store = fields(one(lines, "metric_store ", path))

    required_config = (
        *CONFIG_KEYS,
        "message_sample_denominator",
        "configured_message_sample_rate_percent",
        "order_index",
    )
    missing = [key for key in required_config if key not in config]
    if missing:
        raise ValueError(f"{path}: config missing {missing}")
    if config["message_sample_denominator"] != denominator:
        raise ValueError(f"{path}: filename and configured denominator differ")
    expected_rate = 0.0 if denominator == "none" else 100.0 / int(denominator)
    if abs(float(config["configured_message_sample_rate_percent"]) - expected_rate) > 0.00001:
        raise ValueError(f"{path}: configured sample rate differs from denominator")
    if config["condition"] != "telemetry_on" or config["compiled_variant"] != "telemetry_on":
        raise ValueError(f"{path}: Broadway telemetry must be enabled")
    if config["handler_store"] != "ets" or config["handler_backend"] != "telemetry_ets":
        raise ValueError(f"{path}: benchmark requires ETS telemetry handlers")
    if config["manual_batch_flush"] != "true" or config["scheduler_wall_time"] != "true":
        raise ValueError(f"{path}: deterministic flushing and scheduler wall time are required")

    expected_events = (
        LOW_FREQUENCY_EVENTS
        if denominator == "none"
        else (LOW_FREQUENCY_EVENTS[0], LOW_FREQUENCY_EVENTS[1], MESSAGE_EVENT, LOW_FREQUENCY_EVENTS[2])
    )
    expected_metrics = tuple(f"{event}.duration" for event in expected_events)
    if int(handlers.get("broadway_total", "-1")) != len(expected_events):
        raise ValueError(f"{path}: wrong Broadway handler count")
    if int(handlers.get("telemetry_total", "-1")) != len(expected_events):
        raise ValueError(f"{path}: wrong isolated handler count")
    if int(handlers.get("exporter_metrics", "-1")) != len(expected_events):
        raise ValueError(f"{path}: wrong exporter metric count")
    if tuple(handlers.get("events", "").split(",")) != expected_events:
        raise ValueError(f"{path}: unexpected production Broadway event set")

    required_store = (
        "message_sample_denominator",
        "rows",
        "metrics",
        "observations",
        "message_observations",
        "counts",
    )
    missing = [key for key in required_store if key not in store]
    if missing:
        raise ValueError(f"{path}: metric store missing {missing}")
    if store["message_sample_denominator"] != denominator:
        raise ValueError(f"{path}: metric-store denominator differs from config")
    observed_counts = counts(store["counts"], path)
    if not set(LOW_FREQUENCY_METRICS).issubset(observed_counts) or not set(observed_counts).issubset(expected_metrics):
        raise ValueError(f"{path}: unexpected metric-store metric set")
    if any(observed_counts[metric] <= 0 for metric in LOW_FREQUENCY_METRICS):
        raise ValueError(f"{path}: a retained low-frequency metric has no observations")
    message_observations = int(store["message_observations"])
    if message_observations != observed_counts.get(MESSAGE_METRIC, 0):
        raise ValueError(f"{path}: message observation count differs from metric store")
    total_messages = int(config["events"]) * (int(config["warmups"]) + int(config["trials"]))
    observation_evidence = sampling_evidence(denominator, total_messages, message_observations, path)
    if int(store["observations"]) != sum(observed_counts.values()):
        raise ValueError(f"{path}: metric observation total differs from counts")
    if int(store["metrics"]) != len(observed_counts):
        raise ValueError(f"{path}: metric-store metric count differs")

    samples: list[dict[str, float | int]] = []
    seen: set[int] = set()
    for line in lines:
        if not line.startswith("sample kind=measurement "):
            continue
        sample = fields(line)
        required_sample = (
            "run",
            "events",
            "elapsed_us",
            "events_per_second",
            "reductions_per_event",
            "bytes",
            "files",
            "scheduler_active_us",
            "scheduler_total_us",
            "scheduler_utilization_percent",
            "average_active_schedulers",
            "events_per_active_scheduler_second",
        )
        missing = [key for key in required_sample if key not in sample]
        if missing:
            raise ValueError(f"{path}: sample missing {missing}")
        run = int(sample["run"])
        if run in seen:
            raise ValueError(f"{path}: duplicate measurement run")
        seen.add(run)
        if int(sample["events"]) != int(config["events"]):
            raise ValueError(f"{path}: sample event count differs from config")
        active, total = int(sample["scheduler_active_us"]), int(sample["scheduler_total_us"])
        utilization, schedulers, efficiency = (
            float(sample[key])
            for key in (
                "scheduler_utilization_percent",
                "average_active_schedulers",
                "events_per_active_scheduler_second",
            )
        )
        if active <= 0 or total <= 0 or active > total or not 0 < utilization <= 100:
            raise ValueError(f"{path}: invalid scheduler wall-time sample")
        if not 0 < schedulers <= int(config["schedulers"]) or efficiency <= 0:
            raise ValueError(f"{path}: invalid active-scheduler sample")
        samples.append(
            {
                "events_per_second": float(sample["events_per_second"]),
                "reductions_per_event": float(sample["reductions_per_event"]),
                "elapsed_us": int(sample["elapsed_us"]),
                "bytes": int(sample["bytes"]),
                "files": int(sample["files"]),
                "scheduler_utilization_percent": utilization,
                "average_active_schedulers": schedulers,
                "events_per_active_scheduler_second": efficiency,
            }
        )
    if seen != set(range(1, int(config["trials"]) + 1)):
        raise ValueError(f"{path}: incomplete measurement runs")
    return denominator, block, config, samples, observation_evidence


def median(samples: list[dict[str, float | int]], key: str) -> float:
    return statistics.median(float(sample[key]) for sample in samples)


def percent_delta(candidate: float, baseline: float) -> float:
    return (candidate / baseline - 1) * 100


def metric_medians(samples: list[dict[str, float | int]]) -> dict[str, float]:
    return {metric: median(samples, metric) for metric in COMPARISON_METRICS}


def cycle_deltas(
    cycle_entries: list[dict[str, object]], cycle_index: int
) -> dict[str, dict[str, object]]:
    endpoint_samples = list(cycle_entries[0]["samples"]) + list(cycle_entries[-1]["samples"])
    baseline = metric_medians(endpoint_samples)
    by_condition: dict[str, list[dict[str, object]]] = defaultdict(list)
    for entry in cycle_entries:
        by_condition[str(entry["denominator"])].append(entry)

    result: dict[str, dict[str, object]] = {}
    for condition in CONDITIONS:
        entries = by_condition[condition]
        if len(entries) != 2:
            raise ValueError(f"cycle {cycle_index}: expected two {condition} blocks, found {len(entries)}")
        candidate_samples = list(entries[0]["samples"]) + list(entries[1]["samples"])
        candidate = metric_medians(candidate_samples)
        result[condition] = {
            "cycle": cycle_index,
            "blocks": [str(entry["block"]) for entry in entries],
            "baseline_none_blocks": [str(cycle_entries[0]["block"]), str(cycle_entries[-1]["block"])],
            "percent_deltas_vs_cycle_none": {
                metric: percent_delta(candidate[metric], baseline[metric]) for metric in COMPARISON_METRICS
            },
        }
    return result


def delta_dispersion(cycle_values: list[dict[str, object]]) -> dict[str, dict[str, float]]:
    return {
        metric: {
            "min_percent": min(float(value["percent_deltas_vs_cycle_none"][metric]) for value in cycle_values),
            "max_percent": max(float(value["percent_deltas_vs_cycle_none"][metric]) for value in cycle_values),
        }
        for metric in COMPARISON_METRICS
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("logs", nargs="+", type=Path)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()

    grouped: dict[str, list[dict[str, float | int]]] = defaultdict(list)
    observation_evidence: dict[str, list[dict[str, float | int | None]]] = defaultdict(list)
    blocks: dict[str, list[str]] = defaultdict(list)
    execution: dict[int, dict[str, object]] = {}
    expected_config: dict[str, str] | None = None
    files: list[int] = []

    for path in args.logs:
        denominator, block, config, samples, evidence = parse(path)
        if block in blocks[denominator]:
            raise ValueError(f"{path}: duplicate condition/block")
        blocks[denominator].append(block)
        order = int(config["order_index"])
        if order <= 0 or order in execution:
            raise ValueError(f"{path}: invalid or duplicate order index")
        execution[order] = {
            "denominator": denominator,
            "block": block,
            "samples": samples,
            "sampling_evidence": evidence,
        }
        comparable = {key: config[key] for key in CONFIG_KEYS}
        if expected_config is None:
            expected_config = comparable
        elif comparable != expected_config:
            raise ValueError(f"{path}: workload/configuration differs")
        grouped[denominator].extend(samples)
        observation_evidence[denominator].append(evidence)
        files.extend(int(sample["files"]) for sample in samples)

    if set(grouped) != set(CONDITIONS):
        raise SystemExit(f"expected all conditions {CONDITIONS}, found {tuple(sorted(grouped))}")
    block_count = len(blocks["none"])
    if block_count < 2 or block_count % 2:
        raise SystemExit("each condition needs an equal, positive even number of blocks")
    if any(len(blocks[condition]) != block_count for condition in CONDITIONS):
        raise SystemExit("conditions must have equal file counts")
    if set(execution) != set(range(1, len(execution) + 1)):
        raise SystemExit("order indexes must be contiguous")

    ordered_entries = [execution[index] for index in sorted(execution)]
    ordered_conditions = [str(entry["denominator"]) for entry in ordered_entries]
    pattern = list(CONDITIONS) + list(reversed(CONDITIONS))
    if len(ordered_conditions) % len(pattern) or any(
        ordered_conditions[offset : offset + len(pattern)] != pattern
        for offset in range(0, len(ordered_conditions), len(pattern))
    ):
        raise SystemExit("execution order is not repeated mirrored condition order")
    if min(files) != max(files):
        raise SystemExit(f"spool file count varied across samples: {min(files)}..{max(files)}")

    cycles = [
        ordered_entries[offset : offset + len(pattern)]
        for offset in range(0, len(ordered_entries), len(pattern))
    ]
    cycle_results: dict[str, list[dict[str, object]]] = defaultdict(list)
    for index, cycle in enumerate(cycles, start=1):
        for condition, result in cycle_deltas(cycle, index).items():
            cycle_results[condition].append(result)

    none_medians = metric_medians(grouped["none"])
    result: dict[str, object] = {
        "config": expected_config,
        "file_count_per_condition": block_count,
        "cycle_count": len(cycles),
        "execution_order": ordered_conditions,
        "performance_budget": PERFORMANCE_BUDGET,
        "conditions": {},
    }

    for condition in CONDITIONS:
        samples = grouped[condition]
        evidence = observation_evidence[condition]
        observation_total = sum(int(value["actual_message_observations"]) for value in evidence)
        messages_per_block = int(expected_config["events"]) * (
            int(expected_config["warmups"]) + int(expected_config["trials"])
        )
        total_messages = messages_per_block * len(evidence)
        aggregate_sampling_evidence = sampling_evidence(
            condition,
            total_messages,
            observation_total,
            Path(f"aggregate_sampling_condition_{condition}"),
        )
        expected_observation_total = float(
            aggregate_sampling_evidence["expected_message_observations"]
        )
        expected_rate = float(aggregate_sampling_evidence["expected_message_sample_rate"])
        actual_rate = float(aggregate_sampling_evidence["actual_message_sample_rate"])
        medians = metric_medians(samples)
        aggregate_deltas = {
            metric: percent_delta(medians[metric], none_medians[metric]) for metric in COMPARISON_METRICS
        }
        budget = {
            **PERFORMANCE_BUDGET,
            "aggregate_median_throughput_loss_percent": aggregate_deltas["events_per_second"],
            "aggregate_median_reductions_per_event_increase_percent": aggregate_deltas[
                "reductions_per_event"
            ],
            "performance_acceptable": (
                aggregate_deltas["events_per_second"]
                >= PERFORMANCE_BUDGET["aggregate_median_throughput_loss_percent_at_least"]
                and aggregate_deltas["reductions_per_event"]
                <= PERFORMANCE_BUDGET[
                    "aggregate_median_reductions_per_event_increase_percent_at_most"
                ]
            ),
            "decision_aid_only": True,
        }
        summary = {
            "configured_denominator": None if condition == "none" else int(condition),
            "blocks": len(evidence),
            "samples": len(samples),
            "message_observations": observation_total,
            "expected_message_observations": expected_observation_total,
            "message_observation_rate": actual_rate,
            "expected_message_observation_rate": expected_rate,
            "sampling_blocks": evidence,
            "aggregate_sampling_evidence": aggregate_sampling_evidence,
            "median_events_per_second": medians["events_per_second"],
            "median_reductions_per_event": medians["reductions_per_event"],
            "median_scheduler_utilization_percent": medians["scheduler_utilization_percent"],
            "median_events_per_active_scheduler_second": medians[
                "events_per_active_scheduler_second"
            ],
            "aggregate_percent_deltas_vs_none": aggregate_deltas,
            "cycle_percent_deltas_vs_cycle_none": cycle_results[condition],
            "cycle_delta_dispersion": delta_dispersion(cycle_results[condition]),
            "performance_budget": budget,
            "files_per_sample": min(files),
        }
        result["conditions"][condition] = summary
        print(
            "aggregate message_sample_denominator={} samples={} message_observations={} "
            "expected_message_observations={:.4f} message_observation_rate={:.8f} "
            "expected_message_observation_rate={:.8f} median_events_per_second={:.2f} "
            "median_reductions_per_event={:.2f} median_scheduler_utilization_percent={:.2f} "
            "median_events_per_active_scheduler_second={:.2f} throughput_delta_vs_none_percent={:.2f} "
            "reductions_delta_vs_none_percent={:.2f} performance_acceptable={} files_per_sample={}".format(
                condition,
                summary["samples"],
                observation_total,
                expected_observation_total,
                actual_rate,
                expected_rate,
                medians["events_per_second"],
                medians["reductions_per_event"],
                medians["scheduler_utilization_percent"],
                medians["events_per_active_scheduler_second"],
                aggregate_deltas["events_per_second"],
                aggregate_deltas["reductions_per_event"],
                budget["performance_acceptable"],
                min(files),
            )
        )
        for cycle in cycle_results[condition]:
            deltas = cycle["percent_deltas_vs_cycle_none"]
            print(
                "cycle message_sample_denominator={} cycle={} blocks={} baseline_none_blocks={} "
                "throughput_delta_percent={:.2f} reductions_delta_percent={:.2f} "
                "scheduler_utilization_delta_percent={:.2f} active_scheduler_efficiency_delta_percent={:.2f}".format(
                    condition,
                    cycle["cycle"],
                    ",".join(cycle["blocks"]),
                    ",".join(cycle["baseline_none_blocks"]),
                    deltas["events_per_second"],
                    deltas["reductions_per_event"],
                    deltas["scheduler_utilization_percent"],
                    deltas["events_per_active_scheduler_second"],
                )
            )
        dispersion = summary["cycle_delta_dispersion"]
        print(
            "cycle_dispersion message_sample_denominator={} throughput_percent_min={:.2f} "
            "throughput_percent_max={:.2f} reductions_percent_min={:.2f} reductions_percent_max={:.2f} "
            "scheduler_utilization_percent_min={:.2f} scheduler_utilization_percent_max={:.2f} "
            "active_scheduler_efficiency_percent_min={:.2f} active_scheduler_efficiency_percent_max={:.2f}".format(
                condition,
                dispersion["events_per_second"]["min_percent"],
                dispersion["events_per_second"]["max_percent"],
                dispersion["reductions_per_event"]["min_percent"],
                dispersion["reductions_per_event"]["max_percent"],
                dispersion["scheduler_utilization_percent"]["min_percent"],
                dispersion["scheduler_utilization_percent"]["max_percent"],
                dispersion["events_per_active_scheduler_second"]["min_percent"],
                dispersion["events_per_active_scheduler_second"]["max_percent"],
            )
        )

    args.output.write_text(json.dumps(result, indent=2) + "\n")


if __name__ == "__main__":
    main()
