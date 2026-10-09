import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import unittest


SCRIPT = Path(__file__).with_name("bump-release")
CHART = '''apiVersion: v2
name: logflare
# Chart version comment.
version: 0.6.1 # keep this comment
appVersion: "1.53.0"
annotations:
  version: unrelated
'''


class BumpReleaseTest(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        (self.root / "scripts").mkdir()
        (self.root / "helm").mkdir()
        self.script = self.root / "scripts" / "bump-release"
        shutil.copy2(SCRIPT, self.script)
        self.version = self.root / "VERSION"
        self.chart = self.root / "helm" / "Chart.yaml"
        self.version.write_text("1.53.0\n", encoding="utf-8")
        self.chart.write_text(CHART, encoding="utf-8")

    def run_script(self, *args):
        return subprocess.run(
            [sys.executable, str(self.script), *args],
            cwd=self.root / "helm", capture_output=True, text=True,
        )

    def contents(self):
        return self.version.read_bytes(), self.chart.read_bytes()

    def assert_rejected(self, *args):
        before = self.contents()
        result = self.run_script(*args)
        self.assertNotEqual(result.returncode, 0, result.stdout)
        self.assertTrue(result.stderr)
        self.assertEqual(self.contents(), before)
        return result

    def test_release_bumps_application_and_chart_patch(self):
        result = self.run_script("1.54.0")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(self.version.read_text(), "1.54.0\n")
        self.assertEqual(
            self.chart.read_text(),
            CHART.replace("version: 0.6.1", "version: 0.6.2")
            .replace('appVersion: "1.53.0"', 'appVersion: "1.54.0"'),
        )
        self.assertEqual(self.run_script("--check").returncode, 0)

    def test_explicit_chart_version(self):
        result = self.run_script("1.54.0", "--chart-version", "0.7.0")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("version: 0.7.0 # keep this comment", self.chart.read_text())

    def test_chart_only_default_and_explicit_version(self):
        original_version = self.version.read_bytes()
        for args, expected in [((), "0.6.2"), (("--chart-version", "0.7.0"), "0.7.0")]:
            with self.subTest(args=args):
                result = self.run_script("--chart-only", *args)
                self.assertEqual(result.returncode, 0, result.stderr)
                self.assertEqual(self.version.read_bytes(), original_version)
                self.assertEqual(
                    self.chart.read_text(), CHART.replace("version: 0.6.1", f"version: {expected}")
                )

    def test_dry_runs_show_diff_without_writing(self):
        for args in [("1.54.0",), ("--chart-only", "--chart-version", "0.7.0")]:
            with self.subTest(args=args):
                before = self.contents()
                result = self.run_script(*args, "--dry-run")
                self.assertEqual(result.returncode, 0, result.stderr)
                self.assertIn("--- a/helm/Chart.yaml", result.stdout)
                self.assertEqual("--- a/VERSION" in result.stdout, args[0] != "--chart-only")
                self.assertEqual(self.contents(), before)

    def test_check_is_read_only(self):
        before = self.contents()
        result = self.run_script("--check")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("Chart appVersion matches VERSION: 1.53.0", result.stdout)
        self.assertEqual(self.contents(), before)

    def test_drift_rejects_checks_and_bumps(self):
        self.chart.write_text(CHART.replace('"1.53.0"', '"1.52.0"'))
        for args in [("--check",), ("1.54.0",), ("--chart-only",)]:
            with self.subTest(args=args):
                self.assertIn("does not match VERSION", self.assert_rejected(*args).stderr)

    def test_invalid_versions_are_rejected_before_writing(self):
        for version in [
            "v1.54.0", "1.54", "01.54.0", "1.054.0", "1.54.00",
            "1.54.0-rc.1", "1.54.0+build", "1.54.0\n", "1.54.0;echo bad", "",
        ]:
            with self.subTest(version=version):
                self.assert_rejected(version)
                self.assert_rejected("1.54.0", "--chart-version", version)

    def test_versions_must_increase_numerically(self):
        for version in ["1.53.0", "1.52.99", "0.99.99"]:
            with self.subTest(app=version):
                self.assert_rejected(version)
        for version in ["0.6.1", "0.6.0", "0.5.99"]:
            with self.subTest(chart=version):
                self.assert_rejected("1.54.0", "--chart-version", version)
        result = self.run_script("1.100.0", "--chart-version", "0.10.0")
        self.assertEqual(result.returncode, 0, result.stderr)

    def test_repeated_release_is_rejected(self):
        self.assertEqual(self.run_script("1.54.0").returncode, 0)
        self.assert_rejected("1.54.0")

    def test_invalid_argument_combinations(self):
        for args in [
            (), ("--chart-version", "0.7.0"), ("1.54.0", "--chart-only"),
            ("--check", "1.54.0"), ("--check", "--chart-only"), ("--check", "--dry-run"),
            ("--check", "--chart-version", "0.7.0"), ("1.54.0", "--unknown"),
            ("--check", ""), ("--check", "--chart-version", ""), ("--chart-only", ""),
        ]:
            with self.subTest(args=args):
                self.assert_rejected(*args)

    def test_missing_or_malformed_chart_fields(self):
        for text in [
            CHART.replace("version: 0.6.1", "version: nope"),
            CHART + "version: 0.7.0\n",
            CHART + "appVersion: nope\n",
            CHART.replace('appVersion: "1.53.0"\n', ""),
            CHART.replace('"1.53.0"', "\"1.53.0'"),
        ]:
            with self.subTest(text=text):
                self.chart.write_text(text)
                self.assert_rejected("1.54.0")
                self.assert_rejected("--check")

    def test_invalid_current_version(self):
        self.version.write_text("not-a-version\n")
        self.assert_rejected("1.54.0")
        self.assert_rejected("--check")

    def test_supported_yaml_quotes_and_whitespace(self):
        for quote in ["", "'", '"']:
            with self.subTest(quote=quote):
                self.version.write_text("1.53.0\n")
                self.chart.write_text(
                    CHART.replace("version: 0.6.1", f"version:  {quote}0.6.1{quote}")
                    .replace('appVersion: "1.53.0"', f"appVersion: {quote}1.53.0{quote} # app comment")
                )
                result = self.run_script("1.54.0")
                self.assertEqual(result.returncode, 0, result.stderr)
                self.assertIn(f"version:  {quote}0.6.2{quote} # keep this comment", self.chart.read_text())
                self.assertIn('appVersion: "1.54.0" # app comment', self.chart.read_text())

    def test_missing_file_reports_error(self):
        self.chart.unlink()
        result = self.run_script("1.54.0")
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("bump-release:", result.stderr)
        self.assertEqual(self.version.read_text(), "1.53.0\n")

    @unittest.skipUnless(os.name == "posix", "Executable shebang is POSIX-specific")
    def test_direct_executable_invocation(self):
        result = subprocess.run([str(self.script), "--check"], cwd=self.root, capture_output=True, text=True)
        self.assertEqual(result.returncode, 0, result.stderr)


if __name__ == "__main__":
    unittest.main()
