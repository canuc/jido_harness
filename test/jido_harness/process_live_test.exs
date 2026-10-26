defmodule Jido.Harness.ProcessLiveTest do
  use ExUnit.Case, async: false
  alias Jido.Harness.{MemoryHarness, ProcessManager}
  alias Jido.Harness.SessionAdapters.ACP.ArborACPTransport.Bridge

  setup do
    original = Application.get_env(:jido_harness, :process_driver)
    Application.put_env(:jido_harness, :process_driver, MemoryHarness.Driver)
    dir = Path.join(System.tmp_dir!(), "harness-live-#{System.unique_integer([:positive])}")

    on_exit(fn ->
      if original,
        do: Application.put_env(:jido_harness, :process_driver, original),
        else: Application.delete_env(:jido_harness, :process_driver)

      File.rm_rf!(dir)
    end)

    %{dir: dir}
  end

  test "live delivery preserves wire bytes, blocks without polling, and never reads the journal", %{dir: dir} do
    {id, worker, exec, os_pid} = start_memory_process(dir)
    parent = self()
    handler = make_ref()

    :ok =
      :telemetry.attach(
        handler,
        [:jido, :harness, :journal, :replay],
        fn _, _, _, _ -> send(parent, :disk_replay) end,
        nil
      )

    on_exit(fn -> :telemetry.detach(handler) end)

    assert {:ok, stream} = ProcessManager.live_stream_process(id)
    reader = Task.async(fn -> Enum.to_list(stream) end)
    MemoryHarness.await(fn -> :sys.get_state(worker).output_waiter != nil end)
    assert {:ok, %{state: :running}} = ProcessManager.info_process(id)
    assert :ok = ProcessManager.send_input(id, "input")
    assert {:error, :reader_busy} = GenServer.call(worker, {:live_output, 1, 100})

    wire = "{\"token\":\"test-secret-1234\"}\n"
    send(worker, {:stdout, os_pid, wire})
    send(exec, :finish)
    events = Task.await(reader)
    assert Enum.map(events, & &1.type) == [:started, :stdout, :exited]
    assert Enum.at(events, 1).data == wire
    refute_received :disk_replay

    assert {:ok, persisted} = ProcessManager.replay_process(id)
    refute Enum.at(persisted, 1).data =~ "test-secret-1234"
    assert_receive :disk_replay
    assert :ok = ProcessManager.prune_process(id)
  end

  test "dead live callers release their monitor and pending read", %{dir: dir} do
    {_id, worker, _exec, _os_pid} = start_memory_process(dir)
    caller = spawn(fn -> GenServer.call(worker, {:live_output, 1, 100}, :infinity) end)
    MemoryHarness.await(fn -> :sys.get_state(worker).output_waiter != nil end)
    Process.exit(caller, :kill)
    MemoryHarness.await(fn -> :sys.get_state(worker).output_waiter == nil end)
    refute {:process, caller} in elem(Process.info(worker, :monitors), 1)
  end

  test "a live reader that starts after output left the memory buffer gets an explicit gap", %{dir: dir} do
    {id, worker, _exec, os_pid} = start_memory_process(dir, 500)
    send(worker, {:stdout, os_pid, String.duplicate("x", 2_000)})
    assert {:ok, %{output_cursor: 2}} = ProcessManager.info_process(id)
    assert :sys.get_state(worker).buffer.bytes <= 500
    assert {:ok, stream} = ProcessManager.live_stream_process(id)
    assert [%{type: :replay_gap, data: %{"available_from" => 3}}] = Enum.to_list(stream)
  end

  test "without a live reader the buffer keeps only the latest memory_bytes", %{dir: dir} do
    {id, worker, _exec, os_pid} = start_memory_process(dir, 4_096)
    chunks = write_chunks(worker, os_pid, 40)
    assert {:ok, %{output_cursor: 41}} = ProcessManager.info_process(id)
    assert :sys.get_state(worker).buffer.bytes <= 4_096
    assert {:ok, events} = ProcessManager.replay_process(id)
    assert length(events) == length(chunks) + 1
  end

  test "a live reader that falls more than memory_bytes behind still gets all of its output", %{dir: dir} do
    {id, worker, exec, os_pid} = start_memory_process(dir, 4_096)
    reader = start_stalled_reader(id)
    chunks = write_chunks(worker, os_pid, 40)
    assert {:ok, %{output_cursor: 41}} = ProcessManager.info_process(id)

    send(reader.pid, :resume)
    send(exec, :finish)
    events = Task.await(reader)

    assert Enum.map(events, & &1.sequence) == Enum.to_list(1..42)
    assert Enum.map(events, & &1.type) == [:started] ++ List.duplicate(:stdout, 40) ++ [:exited]
    assert events |> Enum.slice(1..40) |> Enum.map(& &1.data) == chunks
    assert :sys.get_state(worker).buffer.bytes <= 4_096
  end

  test "a live reader that falls further behind than unread_limit_bytes gets an explicit gap", %{dir: dir} do
    {id, worker, _exec, os_pid} = start_memory_process(dir, %{memory_bytes: 4_096, unread_limit_bytes: 16_384})
    reader = start_stalled_reader(id)
    write_chunks(worker, os_pid, 40)
    assert {:ok, %{output_cursor: 41}} = ProcessManager.info_process(id)
    assert :sys.get_state(worker).buffer.bytes in 4_097..16_384

    send(reader.pid, :resume)
    assert [%{type: :started}, %{type: :replay_gap, data: %{"available_from" => from}}] = Task.await(reader)
    assert from > 2
    assert %{live_reader: nil, buffer: %{bytes: bytes}} = :sys.get_state(worker)
    assert bytes <= 4_096
  end

  test "output a departed live reader did not read goes back to the memory window", %{dir: dir} do
    {id, worker, _exec, os_pid} = start_memory_process(dir, 4_096)
    reader = start_stalled_reader(id)
    write_chunks(worker, os_pid, 40)
    assert {:ok, %{output_cursor: 41}} = ProcessManager.info_process(id)
    assert :sys.get_state(worker).buffer.bytes > 4_096

    Task.shutdown(reader, :brutal_kill)
    MemoryHarness.await(fn -> :sys.get_state(worker).live_reader == nil end)
    assert :sys.get_state(worker).buffer.bytes <= 4_096
  end

  test "an ACP session whose reader falls behind receives every message", %{dir: dir} do
    Application.put_env(:jido_harness, :acp_max_frame_bytes, 2_048)
    Application.put_env(:jido_harness, :acp_max_buffer_bytes, 8_192)

    on_exit(fn ->
      Application.delete_env(:jido_harness, :acp_max_frame_bytes)
      Application.delete_env(:jido_harness, :acp_max_buffer_bytes)
    end)

    spec = %{
      executable: System.find_executable("elixir"),
      env: %{},
      metadata: %{test_pid: self()},
      retention: %{journal_dir: dir, memory_bytes: 4_096}
    }

    bridge =
      start_supervised!(
        {Bridge, process_manager: ProcessManager, process_owner: self(), process_spec: spec, listener: self()}
      )

    assert_receive {:memory_driver, worker, _exec, os_pid}

    on_exit(fn ->
      if Process.alive?(worker), do: DynamicSupervisor.terminate_child(Jido.Harness.ProcessSupervisor, worker)
    end)

    # An ACP agent writes only once the client has spoken, by which time the
    # bridge's reader is waiting for output.
    MemoryHarness.await(fn -> :sys.get_state(worker).output_waiter != nil end)
    text = String.duplicate("x", 1_000)
    frames = for n <- 1..40, do: ~s({"jsonrpc":"2.0","method":"session/update","params":{"n":#{n},"text":"#{text}"}})
    Enum.each(frames, &send(worker, {:stdout, os_pid, &1 <> "\n"}))

    # The bridge stops taking output while its own queue is half full, so the
    # rest waits in the process buffer until the session reads it.
    MemoryHarness.await(fn ->
      match?(%{reader_ack: {_, _}}, :sys.get_state(bridge)) or not Bridge.connected?(bridge)
    end)

    assert {:ok, %{output_cursor: 41}} = ProcessManager.info_process(:sys.get_state(bridge).process_id)

    for frame <- frames, do: assert({:ok, ^frame} = Bridge.receive_message(bridge))
    refute_received {:acp_process_stopped, _, _}
  end

  test "a normal worker exit during a call returns not_found" do
    id = "stopping-#{System.unique_integer([:positive])}"
    parent = self()

    spawn(fn ->
      {:ok, _} = Registry.register(Jido.Harness.ProcessRegistry, id, nil)
      send(parent, :stopping_worker_ready)

      receive do
        {:"$gen_call", _from, :info} -> :ok
      end
    end)

    assert_receive :stopping_worker_ready
    assert {:error, :not_found} = ProcessManager.info_process(id)
  end

  test "process memory defaults can size ACP output and per-process retention takes priority", %{dir: dir} do
    original = Application.get_env(:jido_harness, :process_manager)
    Application.put_env(:jido_harness, :process_manager, Map.put(Map.new(original || %{}), :memory_bytes, 1_024))

    on_exit(fn ->
      if original,
        do: Application.put_env(:jido_harness, :process_manager, original),
        else: Application.delete_env(:jido_harness, :process_manager)
    end)

    {_id, worker, _exec, _os_pid} = start_memory_process(dir, nil)
    assert :sys.get_state(worker).buffer.max_bytes == 1_024
    {_id, worker, _exec, _os_pid} = start_memory_process(dir, 2_048)
    assert :sys.get_state(worker).buffer.max_bytes == 2_048
  end

  test "the unread output limit defaults to 64 MiB, is configurable, and per-process retention takes priority",
       %{dir: dir} do
    {_id, worker, _exec, _os_pid} = start_memory_process(dir, nil)
    assert :sys.get_state(worker).buffer.max_unread_bytes == 64 * 1_048_576

    original = Application.get_env(:jido_harness, :process_manager)
    Application.put_env(:jido_harness, :process_manager, Map.put(Map.new(original || %{}), :unread_limit_bytes, 8_192))

    on_exit(fn ->
      if original,
        do: Application.put_env(:jido_harness, :process_manager, original),
        else: Application.delete_env(:jido_harness, :process_manager)
    end)

    {_id, worker, _exec, _os_pid} = start_memory_process(dir, nil)
    assert :sys.get_state(worker).buffer.max_unread_bytes == 8_192
    {_id, worker, _exec, _os_pid} = start_memory_process(dir, %{unread_limit_bytes: 16_384})
    assert :sys.get_state(worker).buffer.max_unread_bytes == 16_384
  end

  # Starts a live reader that takes the first event and then stops reading until
  # it is sent `:resume`, as a reader does while its consumer is busy.
  defp start_stalled_reader(id) do
    assert {:ok, stream} = ProcessManager.live_stream_process(id)
    parent = self()
    reader = Task.async(fn -> Enum.map(stream, &stall_after_first(&1, parent)) end)
    assert_receive :reader_stalled
    reader
  end

  defp stall_after_first(%{sequence: 1} = event, parent) do
    send(parent, :reader_stalled)
    receive do: (:resume -> event)
  end

  defp stall_after_first(event, _parent), do: event

  defp write_chunks(worker, os_pid, count) do
    chunks = for n <- 1..count, do: "#{n}:" <> String.duplicate("x", 1_000) <> "\n"
    Enum.each(chunks, &send(worker, {:stdout, os_pid, &1}))
    chunks
  end

  defp start_memory_process(dir, retention \\ 65_536) do
    retention =
      cond do
        is_map(retention) -> Map.put(retention, :journal_dir, dir)
        retention -> %{journal_dir: dir, memory_bytes: retention}
        true -> %{journal_dir: dir}
      end

    assert {:ok, id} =
             ProcessManager.start_process(%{
               executable: System.find_executable("elixir"),
               env: %{"TEST_SECRET" => "test-secret-1234"},
               metadata: %{test_pid: self()},
               retention: retention
             })

    assert_receive {:memory_driver, worker, exec, os_pid}

    on_exit(fn ->
      if Process.alive?(worker), do: DynamicSupervisor.terminate_child(Jido.Harness.ProcessSupervisor, worker)
    end)

    {id, worker, exec, os_pid}
  end
end
