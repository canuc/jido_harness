defmodule Jido.Harness.ACPV2SessionTest do
  # Draft ACP v2 through a whole session: the version is agreed at
  # initialize, and a host sees the events it sees on v1, plus what a
  # session does in its background.
  use ExUnit.Case, async: false

  alias Jido.Harness.Session

  setup do
    providers = Application.get_env(:jido_harness, :providers)
    config = Application.get_env(:jido_harness, :provider_config)
    fixture = Jido.Harness.TestHelpers.fixture_path("fake_acp_v2_cli.py")
    dir = Path.join(System.tmp_dir!(), "jido-harness-acp-v2-#{System.unique_integer([:positive])}")
    File.mkdir_p!(dir)
    log = Path.join(dir, "session.log")
    base = %{acp_path: fixture, retention: %{journal_dir: dir}, env: %{"HARNESS_FIXTURE_SESSION_LOG" => log}}

    Application.put_env(:jido_harness, :providers, %{
      kimi: Jido.Harness.Adapters.Kimi,
      codex: Jido.Harness.Adapters.Codex
    })

    Application.put_env(:jido_harness, :provider_config, %{
      # asks for v2 and takes v1; codex is left as it is without the setting
      kimi: Map.put(base, :acp_protocol_versions, [2, 1]),
      codex: base
    })

    on_exit(fn ->
      Jido.Harness.TestHelpers.cleanup_sessions()
      Jido.Harness.TestHelpers.cleanup_processes()
      restore(:providers, providers)
      restore(:provider_config, config)
      File.rm_rf!(dir)
    end)

    {:ok, log: log}
  end

  test "a provider that asks for v2 agrees it with an agent that has it, and a turn ends as on v1", %{log: log} do
    assert {:ok, session_id} = Session.start(:kimi, %{})
    assert {:ok, %{provider_session_id: "acp-v2-fixture-session"}} = await_ready(session_id)
    assert [%{"params" => %{"protocolVersion" => 2}}] = logged(log, "initialize")

    assert {:ok, turn_id} = Session.send_message(session_id, "hello")

    assert {:ok, %{status: :completed, text: "fixture-v2-ok", events: events}} =
             Session.await(session_id, turn_id, 2_000)

    assert %{payload: %{"stop_reason" => "end_turn"}} = Enum.find(events, &(&1.type == :turn_completed))
    assert %{"version" => 2} = provider_event(session_id, "acp_protocol")
  end

  test "an agent that only has v1 is spoken to in v1", %{log: log} do
    assert {:ok, session_id} = Session.start(:kimi, %{env: %{"HARNESS_FIXTURE_V1_ONLY" => "1"}})
    assert {:ok, _info} = await_ready(session_id)

    # asked for v2, answered 1, then greeted again as v1 has it
    assert [%{"params" => %{"protocolVersion" => 2}}, %{"params" => %{"protocolVersion" => 1} = v1}] =
             logged(log, "initialize")

    assert is_map(v1["clientCapabilities"])

    assert {:ok, turn_id} = Session.send_message(session_id, "hello")
    assert {:ok, %{status: :completed, text: "fixture-v1-ok"}} = Session.await(session_id, turn_id, 2_000)
    assert provider_event(session_id, "acp_protocol") == nil
  end

  test "a provider that does not ask for v2 is not offered it", %{log: log} do
    assert {:ok, session_id} = Session.start(:codex, %{})
    assert {:ok, _info} = await_ready(session_id)
    assert [%{"params" => %{"protocolVersion" => 1}}] = logged(log, "initialize")
  end

  test "a tool call starts with its first update and goes on with the next" do
    assert {:ok, session_id} = Session.start(:kimi, %{})
    assert {:ok, _info} = await_ready(session_id)
    assert {:ok, turn_id} = Session.send_message(session_id, "tool")
    assert {:ok, %{status: :completed, events: events}} = Session.await(session_id, turn_id, 2_000)

    assert [
             %{type: :tool_call, payload: %{"toolCallId" => "call-1", "status" => "pending"}},
             %{type: :tool_result, payload: %{"toolCallId" => "call-1", "status" => "completed"}}
           ] = Enum.filter(events, &(&1.type in [:tool_call, :tool_result]))
  end

  test "a plan has its entries where v1 has them" do
    assert {:ok, session_id} = Session.start(:kimi, %{})
    assert {:ok, _info} = await_ready(session_id)
    assert {:ok, turn_id} = Session.send_message(session_id, "plan")
    assert {:ok, %{status: :completed, events: events}} = Session.await(session_id, turn_id, 2_000)

    assert %{payload: %{"entries" => [%{"content" => "First step", "status" => "in_progress"}]}} =
             Enum.find(events, &(&1.type == :plan_updated))
  end

  test "work that goes on after a turn is said, and said again when it ends" do
    assert {:ok, session_id} = Session.start(:kimi, %{})
    assert {:ok, _info} = await_ready(session_id)
    assert {:ok, turn_id} = Session.send_message(session_id, "background ends")
    assert {:ok, %{status: :completed}} = Session.await(session_id, turn_id, 2_000)

    assert eventually(fn ->
             states = provider_events(session_id, "acp_session_state")

             match?(
               [
                 %{"state" => "running"},
                 %{
                   "state" => "idle",
                   "stop_reason" => "end_turn",
                   "background" => [
                     %{"type" => "terminal", "terminalId" => "term-1"},
                     %{"type" => "subagent", "sessionId" => "child-1"}
                   ]
                 },
                 %{"state" => "idle", "background" => []}
               ],
               states
             )
           end)
  end

  test "a permission request reaches the host with a tool call, and its answer goes back" do
    assert {:ok, session_id} = Session.start(:kimi, %{})
    assert {:ok, _info} = await_ready(session_id)
    assert {:ok, turn_id} = Session.send_message(session_id, "approval")
    assert eventually(fn -> match?({:ok, %{pending_approvals: 1}}, Session.info(session_id)) end)

    assert {:ok, events} = Session.replay(session_id, limit: 1_000)

    assert %{request_id: request_id, payload: %{"tool_call" => %{"toolCallId" => "call-2", "title" => "Run a command"}}} =
             Enum.find(events, &(&1.type == :approval_requested))

    assert :ok = Session.respond_approval(session_id, request_id, :approve)
    assert {:ok, %{status: :completed, text: "approved"}} = Session.await(session_id, turn_id, 2_000)
  end

  test "an interrupted turn ends, and the session takes the next one" do
    assert {:ok, session_id} = Session.start(:kimi, %{})
    assert {:ok, _info} = await_ready(session_id)
    assert {:ok, turn_id} = Session.send_message(session_id, "wait")
    assert eventually(fn -> provider_events(session_id, "acp_session_state") != [] end)

    assert :ok = Session.interrupt(session_id, turn_id)
    assert {:ok, %{status: :interrupted}} = Session.await(session_id, turn_id, 2_000)

    assert {:ok, next_id} = Session.send_message(session_id, "hello")
    assert {:ok, %{status: :completed, text: "fixture-v2-ok"}} = Session.await(session_id, next_id, 2_000)
  end

  test "a saved session is resumed, since v2 has no load, and a fork is a fork", %{log: log} do
    assert {:ok, resumed} = Session.start(:kimi, %{provider_session_id: "saved-session"})
    assert {:ok, %{provider_session_id: "saved-session"}} = await_ready(resumed)

    assert {:ok, forked} = Session.start(:kimi, %{fork_session_id: "saved-session"})
    assert {:ok, %{provider_session_id: "fork-of-saved-session"}} = await_ready(forked)

    assert [%{"params" => %{"sessionId" => "saved-session"}}] = logged(log, "session/resume")
    assert [%{"params" => %{"sessionId" => "saved-session"}}] = logged(log, "session/fork")
    assert logged(log, "session/load") == []
  end

  test "an unknown version in the setting leaves the provider on v1", %{log: log} do
    config = Application.get_env(:jido_harness, :provider_config)
    Application.put_env(:jido_harness, :provider_config, put_in(config, [:kimi, :acp_protocol_versions], [3, 2]))

    assert {:ok, session_id} = Session.start(:kimi, %{})
    assert {:ok, _info} = await_ready(session_id)
    assert [%{"params" => %{"protocolVersion" => 1}}] = logged(log, "initialize")
  end

  defp logged(log, method) do
    log
    |> File.read!()
    |> String.split("\n", trim: true)
    |> Enum.map(&Jason.decode!/1)
    |> Enum.filter(&(&1["method"] == method))
  end

  defp provider_event(session_id, kind), do: session_id |> provider_events(kind) |> List.first()

  defp provider_events(session_id, kind) do
    {:ok, events} = Session.replay(session_id, limit: 1_000)
    for %{type: :provider_event, payload: %{"kind" => ^kind} = payload} <- events, do: payload
  end

  defp eventually(fun, attempts \\ 100) do
    cond do
      fun.() -> true
      attempts == 0 -> false
      true -> Process.sleep(20) && eventually(fun, attempts - 1)
    end
  end

  defp await_ready(session_id, attempts \\ 100)
  defp await_ready(_session_id, 0), do: {:error, :timeout}

  defp await_ready(session_id, attempts) do
    case Session.info(session_id) do
      {:ok, %{state: :idle, provider_session_id: id}} = result when is_binary(id) ->
        result

      _ ->
        Process.sleep(20)
        await_ready(session_id, attempts - 1)
    end
  end

  defp restore(key, nil), do: Application.delete_env(:jido_harness, key)
  defp restore(key, value), do: Application.put_env(:jido_harness, key, value)
end
