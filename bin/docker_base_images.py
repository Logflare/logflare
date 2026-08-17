#!/usr/bin/env python3
"""Validate and update the Docker base images used by Logflare."""

from __future__ import annotations

import argparse
import dataclasses
import datetime as dt
import json
import pathlib
import re
import sys
import tempfile
import urllib.parse
import urllib.request
from collections.abc import Iterable

DOCKERFILES = ("Dockerfile", "Dockerfile.base", "Dockerfile.runner")
REQUIRED_ARCHITECTURES = frozenset({"amd64", "arm64"})
DOCKER_HUB_API = "https://hub.docker.com/v2/repositories"


class BaseImageError(RuntimeError):
    """Raised when the base-image configuration is invalid or unavailable."""


@dataclasses.dataclass(frozen=True)
class Versions:
    elixir: str
    otp: str
    rust: str
    debian: str

    @property
    def debian_series(self) -> str:
        match = re.fullmatch(r"(?P<series>[a-z]+)-\d{8}-slim", self.debian)
        if not match:
            raise BaseImageError(f"unsupported Debian version format: {self.debian}")
        return match.group("series")

    @property
    def debian_date(self) -> dt.date:
        match = re.fullmatch(r"[a-z]+-(?P<date>\d{8})-slim", self.debian)
        if not match:
            raise BaseImageError(f"unsupported Debian version format: {self.debian}")
        return dt.datetime.strptime(match.group("date"), "%Y%m%d").date()

    @property
    def builder_tag(self) -> str:
        return f"{self.elixir}-erlang-{self.otp}-debian-{self.debian}"


@dataclasses.dataclass(frozen=True)
class TagInfo:
    name: str
    architectures: frozenset[str]

    @classmethod
    def from_api(cls, payload: dict[str, object]) -> "TagInfo":
        images = payload.get("images") or []
        architectures = {
            image.get("architecture")
            for image in images
            if isinstance(image, dict) and isinstance(image.get("architecture"), str)
        }
        name = payload.get("name")
        if not isinstance(name, str):
            raise BaseImageError("Docker Hub returned a tag without a name")
        return cls(name=name, architectures=frozenset(architectures))


class DockerHubClient:
    def __init__(self, timeout: float = 30.0) -> None:
        self.timeout = timeout

    def tags(self, repository: str, name_filter: str) -> list[TagInfo]:
        encoded_repository = "/".join(urllib.parse.quote(part, safe="") for part in repository.split("/"))
        query = urllib.parse.urlencode({"page_size": 100, "name": name_filter})
        url: str | None = f"{DOCKER_HUB_API}/{encoded_repository}/tags?{query}"
        tags: list[TagInfo] = []
        pages = 0

        while url:
            pages += 1
            if pages > 20:
                raise BaseImageError(f"too many Docker Hub result pages for {repository}")
            request = urllib.request.Request(
                url,
                headers={"Accept": "application/json", "User-Agent": "logflare-base-image-check/1"},
            )
            try:
                with urllib.request.urlopen(request, timeout=self.timeout) as response:
                    payload = json.load(response)
            except Exception as error:  # urllib exposes several unrelated error types
                raise BaseImageError(f"failed to query Docker Hub for {repository}: {error}") from error

            results = payload.get("results")
            if not isinstance(results, list):
                raise BaseImageError(f"Docker Hub returned invalid tag results for {repository}")
            tags.extend(TagInfo.from_api(result) for result in results if isinstance(result, dict))
            next_url = payload.get("next")
            url = next_url if isinstance(next_url, str) and next_url else None

        return tags


def _arg_values(path: pathlib.Path) -> dict[str, str]:
    values: dict[str, str] = {}
    for line in path.read_text().splitlines():
        match = re.fullmatch(r"ARG ([A-Z][A-Z0-9_]*)=(.+)", line)
        if match:
            values[match.group(1)] = match.group(2).strip('"')
    return values


def _tool_versions(path: pathlib.Path) -> dict[str, str]:
    values: dict[str, str] = {}
    for line in path.read_text().splitlines():
        if not line.strip():
            continue
        tool, version = line.split(maxsplit=1)
        values[tool] = version
    return values


def load_versions(root: pathlib.Path) -> Versions:
    dockerfile_values = {name: _arg_values(root / name) for name in DOCKERFILES}

    def require_matching(argument: str, files: Iterable[str]) -> str:
        values = {name: dockerfile_values[name].get(argument) for name in files}
        missing = [name for name, value in values.items() if value is None]
        if missing:
            raise BaseImageError(f"{argument} is missing from {', '.join(missing)}")
        unique = set(values.values())
        if len(unique) != 1:
            rendered = ", ".join(f"{name}={value}" for name, value in values.items())
            raise BaseImageError(f"{argument} is inconsistent: {rendered}")
        value = next(iter(unique))
        assert value is not None
        return value

    versions = Versions(
        elixir=require_matching("ELIXIR_VERSION", ("Dockerfile", "Dockerfile.base")),
        otp=require_matching("OTP_VERSION", ("Dockerfile", "Dockerfile.base")),
        rust=require_matching("RUST_VERSION", ("Dockerfile", "Dockerfile.base")),
        debian=require_matching("DEBIAN_VERSION", DOCKERFILES),
    )

    tools = _tool_versions(root / ".tool-versions")
    elixir_tool_version = tools.get("elixir", "")
    elixir_match = re.fullmatch(r"(?P<version>.+)-otp-(?P<otp_major>\d+)", elixir_tool_version)
    if not elixir_match:
        raise BaseImageError(f"unsupported .tool-versions Elixir format: {elixir_tool_version}")
    otp_major = versions.otp.split(".", maxsplit=1)[0]
    if elixir_match.group("otp_major") != otp_major:
        raise BaseImageError(
            ".tool-versions Elixir OTP suffix is inconsistent: "
            f"elixir={elixir_match.group('otp_major')}, erlang={otp_major}"
        )

    expected_tools = {
        "elixir": elixir_match.group("version"),
        "erlang": tools.get("erlang"),
        "rust": tools.get("rust"),
    }
    actual_tools = {"elixir": versions.elixir, "erlang": versions.otp, "rust": versions.rust}
    mismatches = [
        f"{tool}: Dockerfile={actual_tools[tool]}, .tool-versions={expected_tools[tool]}"
        for tool in actual_tools
        if actual_tools[tool] != expected_tools[tool]
    ]
    if mismatches:
        raise BaseImageError("tool versions are inconsistent: " + "; ".join(mismatches))

    # Validate the date format while loading so every command gets the same guarantees.
    versions.debian_date
    return versions


def _tag_map(tags: Iterable[TagInfo]) -> dict[str, TagInfo]:
    return {tag.name: tag for tag in tags}


def compatible_debian_versions(
    versions: Versions, hex_tags: Iterable[TagInfo], debian_tags: Iterable[TagInfo]
) -> list[str]:
    hex_by_name = _tag_map(hex_tags)
    debian_by_name = _tag_map(debian_tags)
    prefix = f"{versions.elixir}-erlang-{versions.otp}-debian-"
    pattern = re.compile(rf"^{re.escape(prefix)}({re.escape(versions.debian_series)}-\d{{8}}-slim)$")
    compatible: list[str] = []

    for hex_name, hex_tag in hex_by_name.items():
        match = pattern.fullmatch(hex_name)
        if not match:
            continue
        debian_version = match.group(1)
        debian_tag = debian_by_name.get(debian_version)
        if not debian_tag:
            continue
        if REQUIRED_ARCHITECTURES.issubset(hex_tag.architectures) and REQUIRED_ARCHITECTURES.issubset(
            debian_tag.architectures
        ):
            compatible.append(debian_version)

    return sorted(compatible, key=lambda value: dt.datetime.strptime(value.split("-")[1], "%Y%m%d"))


def fetch_compatible_versions(versions: Versions, client: DockerHubClient) -> list[str]:
    builder_prefix = f"{versions.elixir}-erlang-{versions.otp}-debian-{versions.debian_series}-"
    hex_tags = client.tags("hexpm/elixir", builder_prefix)
    debian_tags = client.tags("library/debian", f"{versions.debian_series}-")
    compatible = compatible_debian_versions(versions, hex_tags, debian_tags)
    if not compatible:
        raise BaseImageError(
            "no compatible amd64/arm64 Debian and Hex builder tags were found for "
            f"Elixir {versions.elixir} / OTP {versions.otp}"
        )
    return compatible


def _write_atomic(path: pathlib.Path, contents: str, mode: int) -> None:
    temporary_path: pathlib.Path | None = None
    try:
        with tempfile.NamedTemporaryFile("w", dir=path.parent, delete=False) as temporary:
            temporary.write(contents)
            temporary_path = pathlib.Path(temporary.name)
        temporary_path.chmod(mode)
        temporary_path.replace(path)
        temporary_path = None
    finally:
        if temporary_path and temporary_path.exists():
            temporary_path.unlink()


def replace_debian_version(root: pathlib.Path, current: str, replacement: str) -> None:
    originals: dict[pathlib.Path, tuple[str, int]] = {}
    rendered: dict[pathlib.Path, str] = {}
    pattern = re.compile(rf"^ARG DEBIAN_VERSION={re.escape(current)}$", re.MULTILINE)

    for name in DOCKERFILES:
        path = root / name
        contents = path.read_text()
        updated, count = pattern.subn(f"ARG DEBIAN_VERSION={replacement}", contents)
        if count != 1:
            raise BaseImageError(f"expected exactly one DEBIAN_VERSION={current} in {name}, found {count}")
        originals[path] = (contents, path.stat().st_mode)
        rendered[path] = updated

    try:
        for path, contents in rendered.items():
            _write_atomic(path, contents, originals[path][1])
    except Exception as error:
        rollback_errors: list[str] = []
        for path, (contents, mode) in originals.items():
            try:
                _write_atomic(path, contents, mode)
            except Exception as rollback_error:
                rollback_errors.append(f"{path.name}: {rollback_error}")
        details = f"; rollback failures: {', '.join(rollback_errors)}" if rollback_errors else ""
        raise BaseImageError(f"failed to update Dockerfiles: {error}{details}") from error


def _age_days(versions: Versions, today: dt.date | None = None) -> int:
    return ((today or dt.datetime.now(dt.UTC).date()) - versions.debian_date).days


def check(root: pathlib.Path, client: DockerHubClient, warn_age_days: int, fail_age_days: int | None) -> int:
    versions = load_versions(root)
    compatible = fetch_compatible_versions(versions, client)
    if versions.debian not in compatible:
        raise BaseImageError(
            f"current Debian snapshot {versions.debian} is not available for amd64/arm64 in both Debian and Hex images"
        )

    age = _age_days(versions)
    latest = compatible[-1]
    print(f"current Debian snapshot: {versions.debian} ({age} days old)")
    print(f"latest compatible snapshot: {latest}")
    print(f"builder image: hexpm/elixir:{versions.builder_tag}")
    print(f"runner image: debian:{versions.debian}")

    if age > warn_age_days:
        print(f"warning: Debian snapshot exceeds {warn_age_days} days", file=sys.stderr)
    if fail_age_days is not None and age > fail_age_days:
        raise BaseImageError(f"Debian snapshot is {age} days old; maximum allowed age is {fail_age_days} days")
    if latest != versions.debian:
        print(f"update available: {versions.debian} -> {latest}")
    return 0


def apply(root: pathlib.Path, client: DockerHubClient, dry_run: bool) -> int:
    versions = load_versions(root)
    compatible = fetch_compatible_versions(versions, client)
    latest = compatible[-1]
    if latest == versions.debian:
        print(f"Docker base snapshot is current: {versions.debian}")
        return 0

    print(f"Docker base snapshot update: {versions.debian} -> {latest}")
    if not dry_run:
        replace_debian_version(root, versions.debian, latest)
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=pathlib.Path, default=pathlib.Path.cwd())
    subparsers = parser.add_subparsers(dest="command", required=True)

    check_parser = subparsers.add_parser("check", help="validate configured images and report freshness")
    check_parser.add_argument("--warn-age-days", type=int, default=45)
    check_parser.add_argument("--fail-age-days", type=int)

    apply_parser = subparsers.add_parser("apply", help="update Dockerfiles to the latest compatible snapshot")
    apply_parser.add_argument("--dry-run", action="store_true")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    root = args.root.resolve()
    try:
        if args.command == "check":
            return check(root, DockerHubClient(), args.warn_age_days, args.fail_age_days)
        return apply(root, DockerHubClient(), args.dry_run)
    except BaseImageError as error:
        print(f"error: {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
