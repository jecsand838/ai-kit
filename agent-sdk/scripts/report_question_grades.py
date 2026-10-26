"""Validate the two approved strict-live Question receipts, not model quality."""
import json
import os
from pathlib import Path
import re
import sys


class QuestionGrades:
    PREFIX = "AGENT_SDK_QUESTION_GRADE="
    CASES = (
        "strict_live_suite::delivery_and_events::opencode_question_delivery_replays_resolves_once_and_drains",
        "strict_live_suite::prompt_and_io::opencode_native_idle_terminal_waiter_reopens_in_one_build",
    )
    FIELDS = {"version", "case", "sdk_contract", "model_exact_copy",
              "model_yes_description_exact_copy", "model_no_description_exact_copy", "runtime_version",
              "provider_id", "model_id", "configured_model"}

    @staticmethod
    def unique_object(pairs):
        result = {}
        for name, value in pairs:
            if name in result:
                raise ValueError("duplicate JSON field")
            result[name] = value
        return result

    @classmethod
    def report(cls, path, selected_case=None):
        if selected_case is not None and selected_case not in cls.CASES:
            raise ValueError("selected Question case is not approved")
        expected = cls.CASES if selected_case is None else (selected_case,)
        spec = Path(__file__).resolve().parents[1] / "specs/SPECIFICATIONS.md"
        versions = re.findall(r"^- OpenCode Server: `v([^`]+)`$", spec.read_text(), re.M)
        if len(versions) != 1:
            raise ValueError("selected OpenCode version is missing or ambiguous")
        configured = os.environ.get("TEST_OLLAMA_MODEL_OPENCODE_SHELL", "")
        if not configured.strip():
            raise ValueError("required configured Question model role is missing")
        receipts = {}
        with path.open(encoding="utf-8") as log:
            for line in log:
                if not line.startswith(cls.PREFIX):
                    continue
                if len(line) > 4096:
                    raise ValueError("receipt exceeds its bounded schema")
                receipt = json.loads(line[len(cls.PREFIX):], object_pairs_hook=cls.unique_object)
                if not isinstance(receipt, dict) or set(receipt) != cls.FIELDS:
                    raise ValueError("receipt fields do not match schema version 2")
                if type(receipt["version"]) is not int or receipt["version"] != 2:
                    raise ValueError("unsupported receipt version")
                case = receipt["case"]
                if not isinstance(case, str) or case not in expected or case in receipts:
                    raise ValueError("unknown or duplicate Question case")
                if receipt["sdk_contract"] is not True:
                    raise ValueError("Question SDK contract did not pass")
                for field in ("model_exact_copy", "model_yes_description_exact_copy", "model_no_description_exact_copy"):
                    if type(receipt[field]) is not bool:
                        raise ValueError("model-copy grades must be booleans")
                for field in ("runtime_version", "provider_id", "model_id", "configured_model"):
                    if not isinstance(receipt[field], str) or not receipt[field].strip():
                        raise ValueError("native runtime/model provenance is missing")
                if receipt["runtime_version"] != versions[0]:
                    raise ValueError("native runtime differs from the selected protocol version")
                if receipt["configured_model"] != configured:
                    raise ValueError("Question model differs from its unchanged configured role")
                receipts[case] = receipt
        if set(receipts) != set(expected):
            raise ValueError("expected exactly one receipt from each approved Question case")
        print(f"Question SDK receipts: PASS ({len(expected)}/{len(expected)}); emitted after successful scenario cleanup")
        if selected_case is not None:
            print("Question receipt scope: selected case only; full two-case gate not implied")
        failures = [case for case in expected if not receipts[case]["model_exact_copy"]]
        for case in expected:
            receipt = receipts[case]
            outcome = "EXACT" if receipt["model_exact_copy"] else "FAILED (nonblocking)"
            print(f"Model raw-copy quality: {outcome}: {case}")
            for label, field in (("Yes", "model_yes_description_exact_copy"), ("No", "model_no_description_exact_copy")):
                outcome = "EXACT" if receipt[field] else "FAILED (nonblocking)"
                print(f"Model description-copy quality [{label}]: {outcome}: {case}")
        if failures:
            print(f"Model raw-copy quality: FAILED ({len(failures)}/{len(expected)}); nonblocking; no model-quality acceptance")
        else:
            print(f"Model raw-copy quality: observed exact copies {len(expected)}/{len(expected)}; no general quality acceptance")


if __name__ == "__main__":
    try:
        if len(sys.argv) != 2 and not (len(sys.argv) == 4 and sys.argv[2] == "--case"):
            raise ValueError("expected one private log path and optional selected case")
        QuestionGrades.report(Path(sys.argv[1]), sys.argv[3] if len(sys.argv) == 4 else None)
    except (OSError, UnicodeError, ValueError, RecursionError) as error:
        # Never echo malformed log/native content into the diagnostic.
        print(f"Question SDK receipt report: FAILED ({type(error).__name__})", file=sys.stderr)
        raise SystemExit(1) from None
