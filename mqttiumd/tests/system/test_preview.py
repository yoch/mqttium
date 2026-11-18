"""Packaging keeps Cargo progress separate from machine-readable metadata."""
import json
import tarfile
import tempfile
from pathlib import Path
import subprocess
import sys
import unittest
import platform
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "tools"))
from build_preview import command
import build_preview
import check_preview


class PreviewCommandTests(unittest.TestCase):
    def test_execution_identity_is_distinct_from_build_identity(self):
        build={"platform":"different-build-host","machine":"different-build-machine"}
        with patch.object(platform,"platform",return_value="actual-execution-host"), patch.object(platform,"machine",return_value="actual-execution-machine"):
            observed=check_preview.platforms(build)
        self.assertEqual(observed["build_platform"],"different-build-host")
        self.assertEqual(observed["execution_platform"],"actual-execution-host","build_host_misreported_as_execution_host")
        self.assertEqual(observed["execution_machine"],"actual-execution-machine")

    def test_normal_package_contains_will_demo_and_explicit_profile(self):
        with tempfile.TemporaryDirectory() as temporary:
            root=Path(temporary)/"repo"/"mqttiumd"
            (root/"preview").mkdir(parents=True)
            for name in ("README.md", "start.sh", "demo.sh", "expiry-demo.sh", "will-demo.sh",
                         "SECURE.md", "start-tls.sh", "secure-demo.sh", "demo-pki.py", "identities.example.json",
                         "POLICY.md", "policy-demo.py", "identities.next.example.json", "retained-will-demo.py", "PILOT.md", "FORWARD.md", "SPLIT.md"):
                (root/"preview"/name).write_text("fixture "+name)
            (root/"docs/pilot").mkdir(parents=True)
            (root/"docs/pilot/decision.md").write_text("identified qualification fixture\n")
            (root/"preview/pilot").mkdir()
            (root/"preview/pilot/load.py").write_text("# normal pilot fixture\n")
            (root/"preview/pilot/forward_client.py").write_text("# store-and-forward fixture\n")
            (root/"tools").mkdir()
            (root/"tools/run_prepilot_reception.sh").write_text("#!/bin/sh\nexit 0\n")
            (root/"tools/run_forward_reception.sh").write_text("#!/bin/sh\nexit 0\n")
            (root/"tools/run_split_reception.sh").write_text("#!/bin/sh\nexit 0\n")
            (root/"tests/system").mkdir(parents=True)
            (root/"tests/system/policy_transition_reception.py").write_text("from normal_helper import recovery\n")
            (root/"tests/system/normal_helper.py").write_text("recovery = True\n")
            (root/"contracts/v1").mkdir(parents=True)
            (root/"contracts/v1/product-profile.json").write_text('{"receipt_slots":8192}\n')
            for name in ("LICENSE","NOTICE"):
                (root.parent/name).write_text("license fixture")
            product=root/"crates"/"mqttiumd"
            product.mkdir(parents=True)
            (product/"Cargo.toml").write_text("fixture")
            target=Path(temporary)/"target"
            (target/"release").mkdir(parents=True)
            (target/"release"/"mqttiumd").write_bytes(b"identified-normal-fixture")
            def describe(*args,cwd=None):
                if args[:2]==("cargo","tree"):
                    return "mqttiumd v0.0.0 features=\n"
                if args[:2]==("cargo","metadata"):
                    return json.dumps({"packages":[{"id":"product","name":"mqttiumd","version":"0.0.0","license":"Apache-2.0","manifest_path":str(product/"Cargo.toml")}],"resolve":{"nodes":[{"id":"product","deps":[]}]}})
                if args[0]=="ldd":
                    return "compatible-test-runtime\n"
                if args[0]=="rustc":
                    return "identified-test-compiler\n"
                self.fail(f"unexpected build command {args!r}")
            with patch.object(build_preview,"command",side_effect=describe),patch.object(build_preview.subprocess,"run"):
                archive=build_preview.build_pinned(Path(temporary)/"out",target,root,"a"*40)
            with tarfile.open(archive) as packed:
                names=packed.getnames()
                self.assertTrue(any(name.endswith("/will-demo.sh") for name in names),"packaged_will_demo_missing")
                for name in ("SECURE.md", "start-tls.sh", "secure-demo.sh", "demo-pki.py", "identities.example.json",
                             "POLICY.md", "policy-demo.py", "identities.next.example.json", "retained-will-demo.py"):
                    self.assertTrue(any(member.endswith("/" + name) for member in names), name)
                for name in ("PILOT.md", "run-qualification.sh", "pilot/load.py", "FORWARD.md",
                             "run-forward-reception.sh", "pilot/forward_client.py", "SPLIT.md", "run-split-reception.sh",
                             "tests/system/policy_transition_reception.py", "tests/system/normal_helper.py",
                             "preview/demo-pki.py", "preview/policy-demo.py", "qualification/decision.md",
                             "contracts/v1/product-profile.json"):
                    self.assertTrue(any(member.endswith("/"+name) for member in names),
                                    "packaged_pilot_dependency_missing: "+name)
                info=next(item for item in packed.getmembers() if item.name.endswith("/BUILD.json"))
                build=json.load(packed.extractfile(info))
            self.assertEqual(build["sqlite_schema"],12,"will_schema_not_named")
            self.assertEqual(build["listener_profiles"]["loopback_cleartext"], {"sqlite_schema": 12, "tls": False})
            self.assertEqual(build["listener_profiles"]["private_mtls"], {
                "sqlite_schema": 13, "tls_version": "TLS1.3", "client_certificate": "required",
                "tls_resumption": False, "early_data": False,
            })
            self.assertEqual(build["will_profile"],{"qos":1,"retain":True,"delay_interval":0,"message_expiry":False,"intent_slots":256,"payload_bytes":8388608})
            self.assertEqual(build["features"],[])
            self.assertFalse(build["instrumented"])
            self.assertEqual(build["offline_policy"], {
                "plan_format": "mqttiumd.policy-plan.v1", "sqlite_schema": 13,
                "scope": "monotone", "tool": "mqttiumd policy",
            })

    def test_dependency_download_progress_cannot_prefix_metadata_json(self):
        output = command(sys.executable, "-c",
            "import sys; print('Downloading dependency',file=sys.stderr); print('{\"packages\":[]}')")
        self.assertEqual(json.loads(output), {"packages": []}, "cargo_progress_polluted_metadata")

    def test_failure_keeps_its_diagnostic_separate(self):
        with self.assertRaises(subprocess.CalledProcessError) as caught:
            command(sys.executable, "-c", "import sys; print('fetch refused',file=sys.stderr); sys.exit(7)")
        self.assertEqual(caught.exception.returncode, 7)
        self.assertIn("fetch refused", caught.exception.stderr or "", "packaging_failure_cause_lost")


if __name__ == "__main__":
    unittest.main()
