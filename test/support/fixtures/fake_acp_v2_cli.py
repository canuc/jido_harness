#!/usr/bin/env python3
# A scripted agent that speaks draft ACP v2, and v1 when it is asked to
# (HARNESS_FIXTURE_V1_ONLY=1): what a turn does is chosen by its prompt.
import json
import os
import sys

session_id = "acp-v2-fixture-session"
version = 2
waiting = False
pending_permission = None


def send(value):
    sys.stdout.write(json.dumps(value, separators=(",", ":")) + "\n")
    sys.stdout.flush()


def record(message):
    path = os.environ.get("HARNESS_FIXTURE_SESSION_LOG")
    if path:
        with open(path, "a") as log:
            log.write(json.dumps({"method": message["method"], "params": message.get("params", {})}) + "\n")


def update(body):
    send({"jsonrpc": "2.0", "method": "session/update", "params": {"sessionId": session_id, "update": body}})


def state(name, **extra):
    update({"sessionUpdate": "state_update", "state": name, **extra})


def say(text):
    update({"sessionUpdate": "agent_message_chunk", "messageId": "m-1", "content": {"type": "text", "text": text}})


BACKGROUND = [
    {"type": "terminal", "terminalId": "term-1"},
    {"type": "subagent", "sessionId": "child-1"},
]


def turn(text):
    global waiting, pending_permission
    state("running")
    if "tool" in text:
        update({"sessionUpdate": "tool_call_update", "toolCallId": "call-1", "title": "Read a file", "status": "pending"})
        update({"sessionUpdate": "tool_call_update", "toolCallId": "call-1", "status": "completed"})
    if "plan" in text:
        update({"sessionUpdate": "plan_update", "plan": {"type": "items", "items": [
            {"id": "one", "content": "First step", "status": "in_progress"}]}})
    if "approval" in text:
        pending_permission = 99
        state("requires_action")
        send({"jsonrpc": "2.0", "id": 99, "method": "session/request_permission", "params": {
            "sessionId": session_id,
            "title": "Run a command",
            "subject": {"type": "tool_call", "toolCallId": "call-2"},
            "options": [
                {"optionId": "allow-once", "name": "Allow once", "kind": "allow_once"},
                {"optionId": "reject-once", "name": "Reject", "kind": "reject_once"},
            ],
        }})
        return
    if "wait" in text:
        waiting = True
        return
    say("fixture-v2-ok")
    if "background" in text:
        state("idle", stopReason="end_turn", background=BACKGROUND)
        if "ends" in text:
            state("idle", background=[])
    else:
        state("idle", stopReason="end_turn")


for line in sys.stdin:
    try:
        message = json.loads(line)
    except json.JSONDecodeError:
        continue

    method = message.get("method")
    request_id = message.get("id")
    params = message.get("params", {})

    if method == "initialize":
        record(message)
        if os.environ.get("HARNESS_FIXTURE_V1_ONLY") == "1" or params.get("protocolVersion") != 2:
            version = 1
            send({"jsonrpc": "2.0", "id": request_id, "result": {
                "protocolVersion": 1,
                "agentCapabilities": {"loadSession": True, "sessionCapabilities": {"close": {}}},
                "agentInfo": {"name": "fixture-acp-v2", "version": "1.0.0"},
                "authMethods": [],
            }})
        else:
            send({"jsonrpc": "2.0", "id": request_id, "result": {
                "protocolVersion": 2,
                "info": {"name": "fixture-acp-v2", "version": "1.0.0"},
                "capabilities": {"session": {
                    "prompt": {"image": {}, "embeddedContext": {}},
                    "fork": {},
                    "additionalDirectories": {},
                }},
                "authMethods": [],
            }})
    elif method in ["session/new", "session/resume", "session/load", "session/fork"]:
        record(message)
        if method == "session/fork":
            session_id = "fork-of-" + params.get("sessionId", "")
        elif method != "session/new":
            session_id = params.get("sessionId", session_id)
        send({"jsonrpc": "2.0", "id": request_id, "result": {"sessionId": session_id}})
    elif method == "session/set_config_option":
        record(message)
        send({"jsonrpc": "2.0", "id": request_id, "result": {"configOptions": []}})
    elif method == "session/prompt":
        text = " ".join(block.get("text", "") for block in params.get("prompt", []) if isinstance(block, dict))
        if version == 1:
            update({"sessionUpdate": "agent_message_chunk", "content": {"type": "text", "text": "fixture-v1-ok"}})
            send({"jsonrpc": "2.0", "id": request_id, "result": {"stopReason": "end_turn"}})
        else:
            send({"jsonrpc": "2.0", "id": request_id, "result": {"messageId": "prompt-1"}})
            turn(text)
    elif method == "session/cancel" and waiting:
        waiting = False
        state("idle", stopReason="cancelled")
    elif method == "session/close":
        send({"jsonrpc": "2.0", "id": request_id, "result": {}})
    elif request_id == pending_permission and method is None:
        pending_permission = None
        selected = message.get("result", {}).get("outcome", {}).get("optionId", "")
        state("running")
        say("approved" if selected == "allow-once" else "denied")
        state("idle", stopReason="end_turn")
