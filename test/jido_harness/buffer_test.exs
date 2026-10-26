defmodule Jido.Harness.BufferTest do
  use ExUnit.Case, async: true

  alias Jido.Harness.Buffer

  @size :erlang.external_size(%{sequence: 1, data: String.duplicate("x", 100)})

  test "without a read mark the buffer keeps only the latest max_bytes" do
    for buffer <- [Buffer.new(3 * @size), Buffer.new(3 * @size, 8 * @size)] do
      buffer = append(buffer, 1..6)
      assert sequences(Buffer.events(buffer)) == [4, 5, 6]
      assert buffer.bytes == 3 * @size
    end
  end

  test "events after the read mark stay past max_bytes and read ones keep the window" do
    buffer = Buffer.new(3 * @size, 8 * @size) |> Buffer.mark_read(0) |> append(1..6)
    assert sequences(Buffer.events_after(buffer, 0, 10)) == [1, 2, 3, 4, 5, 6]

    buffer = Buffer.mark_read(buffer, 4)
    assert sequences(Buffer.events(buffer)) == [4, 5, 6]
    assert sequences(Buffer.events_after(buffer, 4, 10)) == [5, 6]
    assert sequences(Buffer.events_after(buffer, 3, 1)) == [4]
  end

  test "past max_unread_bytes the oldest unread events are dropped" do
    buffer = Buffer.new(3 * @size, 4 * @size) |> Buffer.mark_read(0) |> append(1..6)
    assert sequences(Buffer.events_after(buffer, 0, 10)) == [3, 4, 5, 6]
    assert buffer.bytes == 4 * @size
  end

  test "an unread limit below max_bytes keeps what the window would" do
    buffer = Buffer.new(4 * @size, 2 * @size) |> Buffer.mark_read(0) |> append(1..6)
    assert sequences(Buffer.events_after(buffer, 0, 10)) == [3, 4, 5, 6]
  end

  test "a cursor behind the mark keeps the events after it again" do
    buffer = Buffer.new(3 * @size, 8 * @size) |> Buffer.mark_read(0) |> append(1..6) |> Buffer.mark_read(5)
    assert sequences(Buffer.events(buffer)) == [4, 5, 6]

    buffer = buffer |> Buffer.mark_read(4) |> append(7..8)
    assert sequences(Buffer.events_after(buffer, 4, 10)) == [5, 6, 7, 8]
  end

  test "clearing the mark returns unread events to the window" do
    buffer = Buffer.new(3 * @size, 8 * @size) |> Buffer.mark_read(0) |> append(1..6) |> Buffer.clear_mark()
    assert %{read_mark: nil, unread_bytes: 0, bytes: bytes} = buffer
    assert bytes == 3 * @size
    assert sequences(Buffer.events(append(buffer, 7..7))) == [5, 6, 7]
  end

  test "limits must be positive integers" do
    assert_raise ArgumentError, fn -> Buffer.new(0) end
    assert_raise ArgumentError, fn -> Buffer.new(1_024, 0) end
  end

  defp append(buffer, sequences),
    do: Enum.reduce(sequences, buffer, &Buffer.append(&2, %{sequence: &1, data: String.duplicate("x", 100)}))

  defp sequences(events), do: Enum.map(events, & &1.sequence)
end
