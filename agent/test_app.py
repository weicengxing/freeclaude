import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from fastapi import HTTPException

from agent.app import (
    AgentSession,
    UpdatePermissionsRequest,
    api_update_permissions,
    build_shell_invocation,
    iter_agent_turn,
    parse_agent_actions,
    run_agent_turn,
    run_command_tool,
    validate_workspace_root,
)
from agent.llm_client import DEFAULT_MODEL


class ParseAgentActionsTests(unittest.TestCase):
    def test_parses_multiple_concatenated_tool_actions(self) -> None:
        raw = (
            '{"type":"tool","tool":"list_dir","args":{"relative_path":"."},"reason":"inspect"}'
            '{"type":"tool","tool":"search_text","args":{"query":"FastAPI","relative_path":"."},"reason":"find"}'
        )

        actions = parse_agent_actions(raw)

        self.assertEqual(len(actions), 2)
        self.assertEqual(actions[0]["tool"], "list_dir")
        self.assertEqual(actions[1]["tool"], "search_text")


class RunAgentTurnTests(unittest.TestCase):
    def test_executes_multiple_tool_actions_from_one_completion(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            workspace = Path(tmpdir)
            (workspace / "README.md").write_text("FastAPI demo\n", encoding="utf-8")
            session = AgentSession(
                id="session-1",
                workspace_root=str(workspace),
                model=DEFAULT_MODEL,
            )

            responses = iter(
                [
                    {
                        "text": (
                            '{"type":"tool","tool":"list_dir","args":{"relative_path":"."},"reason":"inspect"}'
                            '{"type":"tool","tool":"search_text","args":{"query":"FastAPI","relative_path":"."},"reason":"find"}'
                        )
                    },
                    {"text": '{"type":"final","answer":"done"}'},
                ]
            )

            with patch("agent.app.send_chat", side_effect=lambda *args, **kwargs: next(responses)):
                assistant_message = run_agent_turn(session, "Analyze this workspace")

        self.assertEqual(assistant_message.content, "done")
        self.assertEqual(len(assistant_message.tool_steps), 2)
        self.assertEqual(assistant_message.tool_steps[0]["tool"], "list_dir")
        self.assertEqual(assistant_message.tool_steps[1]["tool"], "search_text")

    def test_stream_events_include_tool_updates_and_done(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            workspace = Path(tmpdir)
            (workspace / "README.md").write_text("FastAPI demo\n", encoding="utf-8")
            session = AgentSession(
                id="session-2",
                workspace_root=str(workspace),
                model=DEFAULT_MODEL,
            )

            responses = iter(
                [
                    {
                        "text": (
                            '{"type":"tool","tool":"list_dir","args":{"relative_path":"."},"reason":"inspect"}'
                            '{"type":"tool","tool":"search_text","args":{"query":"FastAPI","relative_path":"."},"reason":"find"}'
                        )
                    },
                    {"text": '{"type":"final","answer":"done"}'},
                ]
            )

            with patch("agent.app.send_chat", side_effect=lambda *args, **kwargs: next(responses)):
                events = list(iter_agent_turn(session, "Analyze this workspace"))

        event_names = [name for name, _payload in events]

        self.assertIn("tool_call", event_names)
        self.assertIn("tool_result", event_names)
        self.assertEqual(event_names[-1], "done")
        self.assertEqual(events[-1][1]["assistant"]["content"], "done")


class DeploymentSafetyTests(unittest.TestCase):
    def test_validate_workspace_root_respects_allowlist(self) -> None:
        with tempfile.TemporaryDirectory() as allowed_dir, tempfile.TemporaryDirectory() as blocked_dir:
            allowed_root = Path(allowed_dir)
            nested_allowed = allowed_root / "workspace"
            nested_allowed.mkdir()

            with patch.dict("os.environ", {"AGENT_ALLOWED_WORKSPACE_ROOTS": str(allowed_root)}, clear=False):
                resolved = validate_workspace_root(str(nested_allowed))
                self.assertEqual(resolved, nested_allowed.resolve())

                with self.assertRaises(HTTPException) as blocked_exc:
                    validate_workspace_root(str(blocked_dir))

        self.assertEqual(blocked_exc.exception.status_code, 403)

    def test_run_command_tool_rejects_shell_when_not_configured(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            session = AgentSession(
                id="session-shell-disabled",
                workspace_root=str(root),
                model=DEFAULT_MODEL,
            )
            session.permissions.shell_enabled = True

            with patch.dict("os.environ", {"AGENT_SHELL_MODE": "disabled"}, clear=False):
                with self.assertRaises(HTTPException) as exc_info:
                    run_command_tool(session, root, {"command": "echo hello"})

        self.assertEqual(exc_info.exception.status_code, 403)
        self.assertIn("Shell execution is disabled", str(exc_info.exception.detail))

    def test_permission_update_rejects_enabling_shell_without_executor(self) -> None:
        session = AgentSession(
            id="session-permission-check",
            workspace_root="D:/workspace",
            model=DEFAULT_MODEL,
        )

        with patch("agent.app.get_session_or_404", return_value=session):
            with patch.dict("os.environ", {"AGENT_SHELL_MODE": "disabled"}, clear=False):
                with self.assertRaises(HTTPException) as exc_info:
                    api_update_permissions(session.id, UpdatePermissionsRequest(shell_enabled=True))

        self.assertEqual(exc_info.exception.status_code, 403)

    def test_build_shell_invocation_uses_docker_isolation(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            cwd = root / "src"
            cwd.mkdir()

            env = {
                "AGENT_SHELL_MODE": "docker",
                "AGENT_SHELL_DOCKER_IMAGE": "agent-sandbox:latest",
            }
            with patch.dict("os.environ", env, clear=False):
                invocation, execution_cwd = build_shell_invocation(root, cwd, "pytest -q")

        self.assertEqual(invocation[0], "docker")
        self.assertIn("--network", invocation)
        self.assertIn("none", invocation)
        self.assertIn("--read-only", invocation)
        self.assertIn("agent-sandbox:latest", invocation)
        self.assertIsNone(execution_cwd)


if __name__ == "__main__":
    unittest.main()
