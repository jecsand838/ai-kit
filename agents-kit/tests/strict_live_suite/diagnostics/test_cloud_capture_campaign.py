"""Offline contracts for the bounded cloud capture campaign; never invokes Cargo."""

import importlib.util
import json
from pathlib import Path
import tempfile
import unittest


SPEC = importlib.util.spec_from_file_location(
    "cloud_capture_campaign", Path(__file__).with_name("cloud_capture_campaign.py")
)
CAMPAIGN = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(CAMPAIGN)


class CampaignPolicyContract(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.original = self.root / "old-evidence.json"
        self.original.write_text('{"state":"passed"}\n')
        self.identity = {"revision": "reviewed", "runner_sha256": "runner"}
        self.ledger = CAMPAIGN.CampaignLedger(
            self.root / "campaign", self.identity, [self.original]
        )
        self.ledger.prepare()

    def passing(self):
        return {
            "case_outcome": "passed", "capture_complete": True,
            "first_divergence": None, "cleanup_complete": True,
            "provenance_complete": True, "child_reaped": True,
            "process_group_settled": True, "primary_error": None,
            "cleanup_errors": [], "evidence_errors": [],
        }

    def finish(self, report=None):
        directory, reservation = self.ledger.reserve()
        report = self.passing() if report is None else report
        self.ledger.finish(directory, reservation, report, [])
        return directory

    def test_reserved_but_unsettled_slot_blocks_continuation(self):
        directory, _ = self.ledger.reserve()
        before = (directory / "reservation.json").read_bytes()
        with self.assertRaisesRegex(RuntimeError, "unsettled"):
            self.ledger.reserve()
        self.assertEqual(before, (directory / "reservation.json").read_bytes())
        self.assertEqual(["attempt-01"], [path.name for path in self.ledger.attempts.iterdir()])

    def test_finalized_slots_are_immutable(self):
        directory = self.finish()
        before = (directory / "receipt.json").read_bytes()
        with self.assertRaises(FileExistsError):
            self.ledger.finish(directory, {}, self.passing(), [])
        self.assertEqual(before, (directory / "receipt.json").read_bytes())

    def test_three_invocations_exhaust_budget(self):
        for number in range(1, 4):
            directory = self.finish()
            self.assertEqual(f"attempt-{number:02d}", directory.name)
            receipt = json.loads((directory / "receipt.json").read_text())
            self.assertEqual(number < 3, receipt["continue_allowed"])
        with self.assertRaisesRegex(RuntimeError, "budget"):
            self.ledger.reserve()

    def test_missing_slot_rejects_noncontiguous_inventory(self):
        directory = self.finish()
        directory.rename(self.ledger.attempts / "attempt-02")
        with self.assertRaisesRegex(RuntimeError, "contiguous"):
            self.ledger.reserve()

    def test_reserved_launch_failure_consumes_slot_and_stops(self):
        report = self.passing()
        report.update(case_outcome="not_started", primary_error={"phase": "launch"})
        self.finish(report)
        with self.assertRaisesRegex(RuntimeError, "stop"):
            self.ledger.reserve()

    def test_pass_with_incomplete_capture_stops(self):
        report = self.passing()
        report["capture_complete"] = False
        self.finish(report)
        with self.assertRaisesRegex(RuntimeError, "stop"):
            self.ledger.reserve()

    def test_pass_with_observed_disagreement_stops(self):
        report = self.passing()
        report["first_divergence"] = "provider_delta_to_final"
        self.finish(report)
        with self.assertRaisesRegex(RuntimeError, "stop"):
            self.ledger.reserve()

    def test_original_evidence_drift_blocks_before_reservation(self):
        self.original.write_text('{"state":"changed"}\n')
        with self.assertRaisesRegex(RuntimeError, "evidence changed"):
            self.ledger.reserve()
        self.assertEqual([], list(self.ledger.attempts.iterdir()))

    def test_previous_attempt_artifact_drift_blocks(self):
        directory = self.finish()
        (directory / "receipt.json").write_text("{}\n")
        with self.assertRaisesRegex(RuntimeError, "evidence changed"):
            self.ledger.reserve()

    def test_retained_case_artifact_drift_blocks(self):
        evidence = self.root / "retained.json"
        evidence.write_text("{}\n")
        directory, reservation = self.ledger.reserve()
        self.ledger.finish(directory, reservation, self.passing(), [evidence])
        evidence.write_text("[]\n")
        with self.assertRaisesRegex(RuntimeError, "evidence changed"):
            self.ledger.reserve()

    def test_cleanup_and_evidence_errors_preserve_primary_failure(self):
        report = self.passing()
        original = {"phase": "case", "type": "NonzeroExit"}
        report["primary_error"] = original.copy()
        CAMPAIGN.CampaignPolicy.failure(report, "cleanup", RuntimeError("cleanup failed"))
        CAMPAIGN.CampaignPolicy.failure(report, "evidence", RuntimeError("capture failed"))
        self.assertEqual(original, report["primary_error"])
        self.assertEqual(1, len(report["cleanup_errors"]))
        self.assertEqual(1, len(report["evidence_errors"]))
        self.assertFalse(CAMPAIGN.CampaignPolicy.can_continue(report))

    def test_manifest_cannot_be_rebound_to_new_source(self):
        other = CAMPAIGN.CampaignLedger(self.ledger.root, {"revision": "other"}, [self.original])
        with self.assertRaisesRegex(RuntimeError, "identity"):
            other.prepare()
        manifest = json.loads((self.ledger.root / "manifest.json").read_text())
        self.assertEqual(self.identity, manifest["identity"])


class CaseEvidenceContract(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.directory = self.root / "target/cloud-output-evidence/observation-fixture"
        self.pending = self.directory / "private-pending"
        self.foreground = self.directory / "foreground"
        self.pending.mkdir(parents=True)
        self.foreground.mkdir()
        self.model = {"providerID": "openai", "modelID": "configured-fixture"}
        self.attempt_id = "unique-campaign-attempt"
        self.text = {"version": 1, "campaign_attempt_id": self.attempt_id,
                     "model": self.model, "phases": {}}
        phases = []
        for phase in ("unary", "stream"):
            expected = f"{phase}_012345678901234567890123456789"
            prompt = f"Do not call tools. Return only this exact token: {expected}"
            self.text["phases"][phase] = {"expected": expected, "prompt": prompt,
                "public_terminal": expected, "public_deltas": expected}
            info = {"id": f"assistant-{phase}", "sessionID": "session", "parentID": f"user-{phase}",
                    "role": "assistant", "finish": "stop", "time": {"completed": 1},
                    "providerID": "openai", "modelID": self.model["modelID"]}
            part = {"id": f"part-{phase}", "sessionID": "session", "messageID": info["id"], "type": "text", "text": expected}
            bodies = {
                "request": {"model": self.model, "parts": [{"type": "text", "text": prompt}]},
                "response": {"info": info, "parts": [part]},
                "parent": {"info": {"id": info["parentID"], "sessionID": "session", "role": "user", "model": self.model},
                           "parts": [{"type": "text", "text": prompt}]},
            }
            row = {"phase": phase, "complete": True, "request_forwarded": True,
                   "response_forwarded": True, "parent_forwarded": True,
                   "correlation_complete": True, "response_status": 200,
                   "response_framing_complete": True, "parent_framing_complete": True}
            for kind, body in bodies.items():
                filename = f"{phase}.{kind}.json"
                CAMPAIGN.CampaignPolicy.write_new(self.foreground / filename, body)
                row[f"{kind}_file"] = filename
                row[f"{kind}_body_bytes"] = (self.foreground / filename).stat().st_size
            phases.append(row)
        CAMPAIGN.CampaignPolicy.write_new(self.foreground / "manifest.json", {
            "version": 1, "capture_complete": True, "transport_settled": True,
            "reasons": [], "phases": phases})
        expected = self.text["phases"]["stream"]["expected"]
        digest = CAMPAIGN.CaseEvidence.text_hash
        self.comparison = {
            "event": "fresh_comparison", "capture": "complete", "receipt_persistence_complete": True,
            "matching_provider_flows": 1, "matching_assistants": 1,
            "provider_delta_complete": True, "provider_done_complete": True, "provider_final_complete": True,
            "provider_reported_model_matches_requested": True, "provider_text_parts": 1,
            "native_session_sha256": digest("session"), "native_user_sha256": digest("user-stream"),
            "native_assistant_sha256": digest("assistant-stream"), "native_text_sha256": digest(expected),
            "native_parts": [{"part_sha256": digest("part-stream"), "text_sha256": digest(expected), "text_bytes": len(expected)}],
        }
        part = {"event": "fresh_provider_part", "capture": "complete", "flow": 1,
                "output_index": 0, "content_index": 0}
        for kind in ("delta", "done", "content", "item", "final"):
            part[f"{kind}_sha256"] = digest(expected)
        for kind in ("delta", "done", "final"):
            self.comparison[f"provider_{kind}_sha256"] = digest(expected)
        for before, after in (("delta", "done"), ("done", "content"), ("content", "item"), ("item", "final")):
            self.comparison[f"provider_parts_{before}_equal_{after}"] = True
            part[f"{before}_equals_{after}"] = True
        self.summary = {"version": 3, "campaign_attempt_id": self.attempt_id,
            "case": CAMPAIGN.CampaignPolicy.CASE, "case_passed": True, "cleanup_passed": True,
            "diagnostic_errors": [], "public": {"unary": {"capture": "complete"},
                "stream": {"capture": "complete", "deltas": 1, "invalid_delta": False, "stream_fused": True}, "native": {"capture": "complete", "native_equals_portable": True}},
            "provider": {"receipts": [
                {"event": "fresh_user", "capture": "complete", "prompt_matches_expected": True},
                {"event": "fresh_request", "capture": "complete", "flow": 1,
                 "model_matches_configured": True, "session_matches_fresh_user": True,
                 "exact_prompt_parts": 1, "other_user_text_parts": 0},
                {"event": "fresh_provider_text", "capture": "complete", "flow": 1}, part, self.comparison]}}
        for row in self.summary["provider"]["receipts"]:
            row["capture_id"] = "fixture-capture"
        request = b'{"model":"configured-fixture"}'
        response = b"captured streaming bytes; decoding belongs to the independently tested observer"
        CAMPAIGN.CampaignPolicy.write_new(self.pending / "flow.request.json", request)
        CAMPAIGN.CampaignPolicy.write_new(self.pending / "flow.response.body", response)
        CAMPAIGN.CampaignPolicy.write_new(self.pending / "flow.manifest.json", {
            "version": 1, "protocol": "openai_responses", "flow": 1, "capture_id": "fixture-capture",
            "request_file": "flow.request.json", "response_file": "flow.response.body",
            "request_bytes": len(request), "response_bytes": len(response),
            "eof": True, "body_capture_complete": True, "provider_correlation_complete": True,
            "receipt_persistence_complete": True, "response_bound_exceeded": False, "status": 200})

    def inspect(self):
        (self.directory / "summary.json").write_text(json.dumps(self.summary))
        (self.pending / "case-text.json").write_text(json.dumps(self.text))
        report = {"case_outcome": "passed", "configured_model_sha256": CAMPAIGN.CaseEvidence.text_hash(self.model["modelID"])}
        observer = CAMPAIGN.CaseEvidence(self.root, self.attempt_id)
        observer.inspect("AGENTS_KIT_CLOUD_OUTPUT_DIAGNOSTIC=" + json.dumps({"evidence_directory": str(self.directory)}), report)
        return observer, report

    def test_complete_variable_length_literal_capture_allows_classification(self):
        observer, report = self.inspect()
        self.assertTrue(report["capture_complete"])
        self.assertIsNone(report["first_divergence"])
        self.assertTrue(self.pending.is_dir(), "Inspection alone must never delete private bodies")
        self.assertEqual(self.directory, observer.directory)

    def test_delta_final_disagreement_remains_complete_and_informative(self):
        self.comparison["provider_final_sha256"] = CAMPAIGN.CaseEvidence.text_hash("truncated")
        _, report = self.inspect()
        self.assertTrue(report["capture_complete"])
        self.assertEqual("provider_done_to_final", report["first_divergence"])
        self.assertTrue(report["informative_failure"])

    def test_stale_attempt_receipt_cannot_be_reused(self):
        self.summary["campaign_attempt_id"] = "old-attempt"
        with self.assertRaisesRegex(RuntimeError, "stale"):
            self.inspect()

    def test_correlated_receipt_with_missing_terminal_capture_is_inconclusive(self):
        self.comparison["provider_final_complete"] = False
        with self.assertRaisesRegex(RuntimeError, "incomplete"):
            self.inspect()
        self.assertTrue(self.pending.is_dir())

    def test_provider_model_disagreement_prevents_configured_model_attribution(self):
        self.comparison["provider_reported_model_matches_requested"] = False
        with self.assertRaisesRegex(RuntimeError, "model attribution"):
            self.inspect()

    def test_observed_part_identity_must_match_foreground(self):
        self.comparison["native_assistant_sha256"] = CAMPAIGN.CaseEvidence.text_hash("other")
        with self.assertRaisesRegex(RuntimeError, "identities differ"):
            self.inspect()

    def test_equal_aggregate_does_not_hide_part_disagreement(self):
        part = self.summary["provider"]["receipts"][3]
        part["done_sha256"] = CAMPAIGN.CaseEvidence.text_hash("changed")
        part["delta_equals_done"] = False
        part["done_equals_content"] = False
        self.comparison["provider_parts_delta_equal_done"] = False
        self.comparison["provider_parts_done_equal_content"] = False
        _, report = self.inspect()
        self.assertTrue(report["capture_complete"])
        self.assertEqual("provider_parts_delta_equal_done", report["first_divergence"])

    def test_unrelated_unary_protocol_capture_does_not_invalidate_stream_capture(self):
        for row in self.summary["provider"]["receipts"]:
            if "flow" in row:
                row["flow"] = 2
        manifest_path = self.pending / "flow.manifest.json"
        manifest = json.loads(manifest_path.read_text())
        manifest["flow"] = 2
        manifest_path.write_text(json.dumps(manifest))
        self.summary["provider"]["receipts"].insert(0, {
            "event": "response_end", "flow": 1, "capture": "incomplete", "outcome": "eof"})
        _, report = self.inspect()
        self.assertTrue(report["capture_complete"])
        self.assertIsNone(report["first_divergence"])

    def test_missing_comparison_is_incomplete_not_a_disagreement(self):
        del self.summary["public"]["native"]["native_equals_portable"]
        with self.assertRaisesRegex(RuntimeError, "missing or malformed"):
            self.inspect()

    def test_malformed_fingerprint_is_incomplete(self):
        self.comparison["provider_done_sha256"] = "invalid"
        with self.assertRaisesRegex(RuntimeError, "Fingerprint"):
            self.inspect()

    def test_contradictory_part_facts_are_incomplete(self):
        self.summary["provider"]["receipts"][3]["delta_equals_done"] = False
        with self.assertRaisesRegex(RuntimeError, "contradict"):
            self.inspect()

    def test_pass_must_include_stream_fusion_observation(self):
        self.summary["public"]["stream"]["stream_fused"] = False
        with self.assertRaisesRegex(RuntimeError, "unchanged streaming assertions"):
            self.inspect()

    def test_mismatched_provider_capture_id_is_inconclusive(self):
        path = self.pending / "flow.manifest.json"
        manifest = json.loads(path.read_text())
        manifest["capture_id"] = "different-observer"
        path.write_text(json.dumps(manifest))
        with self.assertRaisesRegex(RuntimeError, "capture identity"):
            self.inspect()

    def test_earlier_part_disagreement_precedes_downstream_native_disagreement(self):
        part = self.summary["provider"]["receipts"][3]
        part["done_sha256"] = CAMPAIGN.CaseEvidence.text_hash("changed")
        part["delta_equals_done"] = False
        part["done_equals_content"] = False
        self.comparison["provider_parts_delta_equal_done"] = False
        self.comparison["provider_parts_done_equal_content"] = False
        self.comparison["native_text_sha256"] = CAMPAIGN.CaseEvidence.text_hash("later changed hook")
        _, report = self.inspect()
        self.assertTrue(report["capture_complete"])
        self.assertEqual("provider_parts_delta_equal_done", report["first_divergence"])

    def test_completed_foreground_does_not_require_stop_finish_reason(self):
        manifest_path = self.foreground / "manifest.json"
        manifest = json.loads(manifest_path.read_text())
        for row in manifest["phases"]:
            path = self.foreground / row["response_file"]
            body = json.loads(path.read_text())
            del body["info"]["finish"]
            path.write_text(json.dumps(body))
            row["response_body_bytes"] = path.stat().st_size
        manifest_path.write_text(json.dumps(manifest))
        _, report = self.inspect()
        self.assertTrue(report["capture_complete"])
        self.assertIsNone(report["first_divergence"])

    def test_partial_foreground_framing_cannot_certify_complete_capture(self):
        path = self.foreground / "manifest.json"
        manifest = json.loads(path.read_text())
        manifest["phases"][0]["response_framing_complete"] = False
        path.write_text(json.dumps(manifest))
        with self.assertRaisesRegex(RuntimeError, "complete forwarded"):
            self.inspect()


if __name__ == "__main__":
    unittest.main()
