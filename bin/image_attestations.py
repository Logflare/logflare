#!/usr/bin/env python3
"""Verify SBOM and provenance attestations published for container images."""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
import time
from collections.abc import Callable

EXPECTED_PREDICATES = frozenset(
    {"https://slsa.dev/provenance/v1", "https://spdx.dev/Document"}
)
REQUIRED_PLATFORMS = frozenset({("linux", "amd64"), ("linux", "arm64")})


class AttestationError(RuntimeError):
    """Raised when an image does not contain the required attestations."""


def repository_from_reference(reference: str) -> str:
    repository = reference.split("@", maxsplit=1)[0]
    last_slash = repository.rfind("/")
    last_colon = repository.rfind(":")
    if last_colon > last_slash:
        repository = repository[:last_colon]
    return repository


def predicate_types(manifest: dict[str, object]) -> frozenset[str]:
    predicates: set[str] = set()
    for layer in manifest.get("layers") or []:
        if not isinstance(layer, dict):
            continue
        annotations = layer.get("annotations") or {}
        if not isinstance(annotations, dict):
            continue
        predicate = annotations.get("in-toto.io/predicate-type")
        if isinstance(predicate, str):
            predicates.add(predicate)
    return frozenset(predicates)


def validate_attestations(
    index: dict[str, object], load_manifest: Callable[[str], dict[str, object]]
) -> None:
    platform_digests: dict[str, tuple[str, str]] = {}
    attestation_digests: dict[str, list[str]] = {}

    for descriptor in index.get("manifests") or []:
        if not isinstance(descriptor, dict):
            continue
        digest = descriptor.get("digest")
        if not isinstance(digest, str):
            continue
        platform = descriptor.get("platform") or {}
        annotations = descriptor.get("annotations") or {}
        if not isinstance(platform, dict) or not isinstance(annotations, dict):
            continue
        if annotations.get("vnd.docker.reference.type") == "attestation-manifest":
            subject = annotations.get("vnd.docker.reference.digest")
            if isinstance(subject, str):
                attestation_digests.setdefault(subject, []).append(digest)
            continue
        os_name = platform.get("os")
        architecture = platform.get("architecture")
        if isinstance(os_name, str) and isinstance(architecture, str):
            platform_digests[digest] = (os_name, architecture)

    available_platforms = set(platform_digests.values())
    missing_platforms = REQUIRED_PLATFORMS - available_platforms
    if missing_platforms:
        rendered = ", ".join(f"{os_name}/{architecture}" for os_name, architecture in sorted(missing_platforms))
        raise AttestationError(f"missing image platforms: {rendered}")

    for subject_digest, platform in platform_digests.items():
        if platform not in REQUIRED_PLATFORMS:
            continue
        predicates: set[str] = set()
        for attestation_digest in attestation_digests.get(subject_digest, []):
            manifest = load_manifest(attestation_digest)
            subject = manifest.get("subject")
            if subject is not None and (
                not isinstance(subject, dict) or subject.get("digest") != subject_digest
            ):
                raise AttestationError(
                    f"attestation {attestation_digest} has the wrong subject for {platform[0]}/{platform[1]}"
                )
            predicates.update(predicate_types(manifest))

        missing_predicates = EXPECTED_PREDICATES - predicates
        if missing_predicates:
            rendered = ", ".join(sorted(missing_predicates))
            raise AttestationError(
                f"{platform[0]}/{platform[1]} is missing attestation predicates: {rendered}"
            )


def inspect_raw(reference: str) -> dict[str, object]:
    result = subprocess.run(
        ["docker", "buildx", "imagetools", "inspect", "--raw", reference],
        check=True,
        capture_output=True,
        text=True,
        timeout=60,
    )
    payload = json.loads(result.stdout)
    if not isinstance(payload, dict):
        raise AttestationError(f"invalid manifest returned for {reference}")
    return payload


def check_image(reference: str) -> None:
    repository = repository_from_reference(reference)
    index = inspect_raw(reference)
    validate_attestations(index, lambda digest: inspect_raw(f"{repository}@{digest}"))


def positive_int(value: str) -> int:
    parsed = int(value)
    if parsed < 1:
        raise argparse.ArgumentTypeError("must be at least 1")
    return parsed


def nonnegative_float(value: str) -> float:
    parsed = float(value)
    if parsed < 0:
        raise argparse.ArgumentTypeError("must be non-negative")
    return parsed


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("images", nargs="+")
    parser.add_argument("--attempts", type=positive_int, default=5)
    parser.add_argument("--delay-seconds", type=nonnegative_float, default=3.0)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    for image in args.images:
        for attempt in range(1, args.attempts + 1):
            try:
                check_image(image)
                print(f"verified SBOM and provenance attestations: {image}")
                break
            except (AttestationError, json.JSONDecodeError, OSError, subprocess.SubprocessError) as error:
                if attempt == args.attempts:
                    print(f"error: failed to verify {image}: {error}", file=sys.stderr)
                    return 1
                print(
                    f"waiting for attestations on {image} ({attempt}/{args.attempts}): {error}",
                    file=sys.stderr,
                )
                time.sleep(args.delay_seconds)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
