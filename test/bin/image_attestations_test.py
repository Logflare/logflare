import contextlib
import io
import pathlib
import sys
import unittest

ROOT = pathlib.Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "bin"))

import image_attestations as subject


class ImageAttestationsTest(unittest.TestCase):
    def setUp(self):
        self.index = {
            "manifests": [
                {
                    "digest": "sha256:amd64",
                    "platform": {"os": "linux", "architecture": "amd64"},
                },
                {
                    "digest": "sha256:amd64-attestation",
                    "platform": {"os": "unknown", "architecture": "unknown"},
                    "annotations": {
                        "vnd.docker.reference.type": "attestation-manifest",
                        "vnd.docker.reference.digest": "sha256:amd64",
                    },
                },
                {
                    "digest": "sha256:arm64",
                    "platform": {"os": "linux", "architecture": "arm64"},
                },
                {
                    "digest": "sha256:arm64-attestation",
                    "platform": {"os": "unknown", "architecture": "unknown"},
                    "annotations": {
                        "vnd.docker.reference.type": "attestation-manifest",
                        "vnd.docker.reference.digest": "sha256:arm64",
                    },
                },
            ]
        }
        self.manifests = {
            "sha256:amd64-attestation": self.attestation("sha256:amd64"),
            "sha256:arm64-attestation": self.attestation("sha256:arm64"),
        }

    def attestation(self, image_digest):
        return {
            "subject": {"digest": image_digest},
            "layers": [
                {
                    "annotations": {
                        "in-toto.io/predicate-type": "https://slsa.dev/provenance/v1"
                    }
                },
                {
                    "annotations": {
                        "in-toto.io/predicate-type": "https://spdx.dev/Document"
                    }
                },
            ],
        }

    def test_validate_attestations_accepts_sbom_and_provenance_for_both_platforms(self):
        subject.validate_attestations(self.index, self.manifests.__getitem__)

    def test_validate_attestations_rejects_missing_platform(self):
        self.index["manifests"] = self.index["manifests"][:2]

        with self.assertRaisesRegex(subject.AttestationError, "missing image platforms: linux/arm64"):
            subject.validate_attestations(self.index, self.manifests.__getitem__)

    def test_validate_attestations_rejects_missing_sbom(self):
        self.manifests["sha256:amd64-attestation"]["layers"] = [
            {
                "annotations": {
                    "in-toto.io/predicate-type": "https://slsa.dev/provenance/v1"
                }
            }
        ]

        with self.assertRaisesRegex(subject.AttestationError, "https://spdx.dev/Document"):
            subject.validate_attestations(self.index, self.manifests.__getitem__)

    def test_validate_attestations_accepts_legacy_manifest_without_subject(self):
        del self.manifests["sha256:amd64-attestation"]["subject"]

        subject.validate_attestations(self.index, self.manifests.__getitem__)

    def test_validate_attestations_rejects_wrong_subject(self):
        self.manifests["sha256:arm64-attestation"]["subject"]["digest"] = "sha256:wrong"

        with self.assertRaisesRegex(subject.AttestationError, "wrong subject"):
            subject.validate_attestations(self.index, self.manifests.__getitem__)

    def test_attempts_must_be_positive(self):
        with contextlib.redirect_stderr(io.StringIO()), self.assertRaises(SystemExit):
            subject.build_parser().parse_args(["--attempts", "0", "example/image:latest"])

    def test_delay_must_be_nonnegative(self):
        with contextlib.redirect_stderr(io.StringIO()), self.assertRaises(SystemExit):
            subject.build_parser().parse_args(
                ["--delay-seconds", "-1", "example/image:latest"]
            )

    def test_repository_from_reference_handles_registry_ports_and_tags(self):
        self.assertEqual(
            "localhost:5000/logflare",
            subject.repository_from_reference("localhost:5000/logflare:dev"),
        )
        self.assertEqual(
            "supabase/logflare",
            subject.repository_from_reference("supabase/logflare@sha256:abc"),
        )


if __name__ == "__main__":
    unittest.main()
