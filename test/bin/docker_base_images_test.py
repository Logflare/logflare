import datetime as dt
import pathlib
import sys
import tempfile
import unittest
from unittest import mock

ROOT = pathlib.Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "bin"))

import docker_base_images as subject


class DockerBaseImagesTest(unittest.TestCase):
    def setUp(self):
        self.temporary_directory = tempfile.TemporaryDirectory()
        self.root = pathlib.Path(self.temporary_directory.name)
        self.write_fixture()

    def tearDown(self):
        self.temporary_directory.cleanup()

    def write_fixture(self, debian="trixie-20260112-slim"):
        shared = (
            "ARG ELIXIR_VERSION=1.19.5\n"
            "ARG OTP_VERSION=27.3.4.6\n"
            f"ARG DEBIAN_VERSION={debian}\n"
            "ARG RUST_VERSION=1.96.0\n"
        )
        (self.root / "Dockerfile").write_text(shared + "FROM example\n")
        (self.root / "Dockerfile.base").write_text(shared + "FROM example\n")
        (self.root / "Dockerfile.runner").write_text(
            f"ARG DEBIAN_VERSION={debian}\nFROM example\n"
        )
        (self.root / ".tool-versions").write_text(
            "elixir 1.19.5-otp-27\nerlang 27.3.4.6\nrust 1.96.0\n"
        )

    def test_load_versions_requires_synchronized_toolchains(self):
        versions = subject.load_versions(self.root)

        self.assertEqual("1.19.5", versions.elixir)
        self.assertEqual("27.3.4.6", versions.otp)
        self.assertEqual("1.96.0", versions.rust)
        self.assertEqual("trixie-20260112-slim", versions.debian)
        self.assertEqual(dt.date(2026, 1, 12), versions.debian_date)

    def test_load_versions_rejects_dockerfile_drift(self):
        runner = self.root / "Dockerfile.runner"
        runner.write_text(runner.read_text().replace("20260112", "20260202"))

        with self.assertRaisesRegex(subject.BaseImageError, "DEBIAN_VERSION is inconsistent"):
            subject.load_versions(self.root)

    def test_load_versions_rejects_tool_versions_drift(self):
        (self.root / ".tool-versions").write_text(
            "elixir 1.19.4-otp-27\nerlang 27.3.4.6\nrust 1.96.0\n"
        )

        with self.assertRaisesRegex(subject.BaseImageError, "tool versions are inconsistent"):
            subject.load_versions(self.root)

    def test_load_versions_rejects_elixir_otp_suffix_drift(self):
        (self.root / ".tool-versions").write_text(
            "elixir 1.19.5-otp-28\nerlang 27.3.4.6\nrust 1.96.0\n"
        )

        with self.assertRaisesRegex(subject.BaseImageError, "Elixir OTP suffix is inconsistent"):
            subject.load_versions(self.root)

    def test_compatible_versions_require_both_architectures_and_images(self):
        versions = subject.load_versions(self.root)
        both = frozenset({"amd64", "arm64"})
        hex_tags = [
            subject.TagInfo(
                "1.19.5-erlang-27.3.4.6-debian-trixie-20260112-slim", both
            ),
            subject.TagInfo(
                "1.19.5-erlang-27.3.4.6-debian-trixie-20260202-slim", both
            ),
            subject.TagInfo(
                "1.19.5-erlang-27.3.4.6-debian-trixie-20260303-slim",
                frozenset({"amd64"}),
            ),
            subject.TagInfo(
                "1.20.0-erlang-28.0-debian-trixie-20260404-slim", both
            ),
        ]
        debian_tags = [
            subject.TagInfo("trixie-20260112-slim", both),
            subject.TagInfo("trixie-20260202-slim", both),
            subject.TagInfo("trixie-20260303-slim", both),
        ]

        self.assertEqual(
            ["trixie-20260112-slim", "trixie-20260202-slim"],
            subject.compatible_debian_versions(versions, hex_tags, debian_tags),
        )

    def test_compatible_versions_enforce_release_soak(self):
        versions = subject.load_versions(self.root)
        both = frozenset({"amd64", "arm64"})
        now = dt.datetime(2026, 3, 10, tzinfo=dt.UTC)
        old = now - dt.timedelta(days=4)
        new = now - dt.timedelta(days=1)
        hex_prefix = "1.19.5-erlang-27.3.4.6-debian-"
        hex_tags = [
            subject.TagInfo(f"{hex_prefix}trixie-20260202-slim", both, old),
            subject.TagInfo(f"{hex_prefix}trixie-20260303-slim", both, new),
        ]
        debian_tags = [
            subject.TagInfo("trixie-20260202-slim", both, old),
            subject.TagInfo("trixie-20260303-slim", both, old),
        ]

        self.assertEqual(
            ["trixie-20260202-slim"],
            subject.compatible_debian_versions(
                versions, hex_tags, debian_tags, minimum_age_days=3, now=now
            ),
        )

    def test_tag_info_parses_docker_hub_timestamp(self):
        tag = subject.TagInfo.from_api(
            {
                "name": "trixie-20260202-slim",
                "images": [{"architecture": "amd64"}, {"architecture": "arm64"}],
                "last_updated": "2026-02-03T04:05:06.123456Z",
            }
        )

        self.assertEqual(dt.datetime(2026, 2, 3, 4, 5, 6, 123456, tzinfo=dt.UTC), tag.last_updated)

    def test_replace_debian_version_updates_all_dockerfiles(self):
        subject.replace_debian_version(
            self.root, "trixie-20260112-slim", "trixie-20260202-slim"
        )

        for name in subject.DOCKERFILES:
            contents = (self.root / name).read_text()
            self.assertIn("ARG DEBIAN_VERSION=trixie-20260202-slim", contents)
            self.assertNotIn("trixie-20260112-slim", contents)

    def test_replace_debian_version_rolls_back_partial_updates(self):
        write_atomic = subject._write_atomic
        failed = False

        def fail_second_update(path, contents, mode):
            nonlocal failed
            if path.name == "Dockerfile.base" and "20260202" in contents and not failed:
                failed = True
                raise OSError("injected write failure")
            write_atomic(path, contents, mode)

        with mock.patch.object(subject, "_write_atomic", side_effect=fail_second_update):
            with self.assertRaisesRegex(subject.BaseImageError, "failed to update Dockerfiles"):
                subject.replace_debian_version(
                    self.root, "trixie-20260112-slim", "trixie-20260202-slim"
                )

        for name in subject.DOCKERFILES:
            contents = (self.root / name).read_text()
            self.assertIn("ARG DEBIAN_VERSION=trixie-20260112-slim", contents)
            self.assertNotIn("trixie-20260202-slim", contents)

    def test_age_days_uses_snapshot_date(self):
        versions = subject.load_versions(self.root)

        self.assertEqual(49, subject._age_days(versions, dt.date(2026, 3, 2)))


if __name__ == "__main__":
    unittest.main()
