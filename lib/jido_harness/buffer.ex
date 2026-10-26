defmodule Jido.Harness.Buffer do
  @moduledoc false

  # A rolling window of the latest events, at most `max_bytes`. A live reader
  # can set a read mark: events after it are ones the reader has not taken yet,
  # and they are kept past the window until they pass `max_unread_bytes` (never
  # fewer than the window would keep). Only then is the oldest of them dropped,
  # which the reader sees as a gap. Events up to the mark, and every event while
  # there is no mark, keep the window.
  #
  # Read events are always older than unread ones, so each kind has its own
  # queue (`events` and `unread`) and eviction still takes the oldest event.

  @schema Zoi.struct(
            __MODULE__,
            %{
              events: Zoi.any() |> Zoi.default(:queue.new()),
              unread: Zoi.any() |> Zoi.default(:queue.new()),
              bytes: Zoi.integer() |> Zoi.default(0),
              unread_bytes: Zoi.integer() |> Zoi.default(0),
              max_bytes: Zoi.integer() |> Zoi.default(1_048_576),
              max_unread_bytes: Zoi.integer() |> Zoi.default(1_048_576),
              read_mark: Zoi.integer() |> Zoi.nullish()
            },
            coerce: true
          )

  @type t :: unquote(Zoi.type_spec(@schema))
  @enforce_keys Zoi.Struct.enforce_keys(@schema)
  defstruct Zoi.Struct.struct_fields(@schema)

  defguardp positive?(value) when is_integer(value) and value > 0

  @doc "Returns the validation schema for the internal event buffer."
  def schema, do: @schema

  @doc "Creates an empty event buffer with a positive byte limit."
  @spec new(pos_integer()) :: t()
  def new(max_bytes), do: new(max_bytes, max_bytes)

  @doc "Creates an empty event buffer that also keeps up to `max_unread_bytes` of events a reader has not taken."
  @spec new(pos_integer(), pos_integer()) :: t()
  def new(max_bytes, max_unread_bytes) do
    case Zoi.parse(@schema, %{max_bytes: max_bytes, max_unread_bytes: max_unread_bytes}) do
      {:ok, buffer} when positive?(max_bytes) and positive?(max_unread_bytes) -> buffer
      _ -> raise ArgumentError, "max_bytes and max_unread_bytes must be positive integers"
    end
  end

  @doc "Adds an event and drops the oldest events until the byte limits are met."
  @spec append(t(), map()) :: t()
  def append(%__MODULE__{} = buffer, event) do
    entry = {event, :erlang.external_size(event)}
    buffer = %{buffer | bytes: buffer.bytes + elem(entry, 1)}

    buffer =
      if unread?(buffer, event),
        do: %{buffer | unread: :queue.in(entry, buffer.unread), unread_bytes: buffer.unread_bytes + elem(entry, 1)},
        else: %{buffer | events: :queue.in(entry, buffer.events)}

    trim(buffer)
  end

  @doc "Records that a live reader has taken every event up to `sequence`."
  @spec mark_read(t(), non_neg_integer()) :: t()
  def mark_read(%__MODULE__{} = buffer, sequence) when is_integer(sequence) do
    %{buffer | read_mark: sequence}
    |> take_read(sequence)
    |> return_unread(sequence)
    |> trim()
  end

  @doc "Removes the read mark, so unread events fall back to the rolling window."
  @spec clear_mark(t()) :: t()
  def clear_mark(%__MODULE__{} = buffer) do
    trim(%{
      buffer
      | events: :queue.join(buffer.events, buffer.unread),
        unread: :queue.new(),
        unread_bytes: 0,
        read_mark: nil
    })
  end

  @doc "Returns retained events in insertion order."
  @spec events(t()) :: [map()]
  def events(%__MODULE__{} = buffer),
    do: for({event, _size} <- :queue.to_list(buffer.events) ++ :queue.to_list(buffer.unread), do: event)

  @doc "Returns up to `limit` retained events after `cursor`, in order, without scanning read events it can skip."
  @spec events_after(t(), non_neg_integer(), non_neg_integer()) :: [map()]
  def events_after(%__MODULE__{} = buffer, cursor, limit) do
    queue =
      case :queue.peek_r(buffer.events) do
        {:value, {%{sequence: sequence}, _size}} when sequence > cursor -> :queue.join(buffer.events, buffer.unread)
        _ -> buffer.unread
      end

    take_after(queue, cursor, limit, [])
  end

  defp unread?(%{read_mark: nil}, _event), do: false
  defp unread?(%{read_mark: mark}, event), do: event.sequence > mark

  defp take_after(_queue, _cursor, 0, taken), do: Enum.reverse(taken)

  defp take_after(queue, cursor, limit, taken) do
    case :queue.out(queue) do
      {{:value, {%{sequence: sequence}, _size}}, rest} when sequence <= cursor -> take_after(rest, cursor, limit, taken)
      {{:value, {event, _size}}, rest} -> take_after(rest, cursor, limit - 1, [event | taken])
      {:empty, _queue} -> Enum.reverse(taken)
    end
  end

  defp take_read(buffer, mark) do
    case :queue.peek(buffer.unread) do
      {:value, {%{sequence: sequence}, size} = entry} when sequence <= mark ->
        buffer = %{buffer | unread: :queue.drop(buffer.unread), unread_bytes: buffer.unread_bytes - size}
        take_read(%{buffer | events: :queue.in(entry, buffer.events)}, mark)

      _ ->
        buffer
    end
  end

  # A reader whose cursor is behind the mark has not taken the events after it.
  defp return_unread(buffer, mark) do
    case :queue.peek_r(buffer.events) do
      {:value, {%{sequence: sequence}, size} = entry} when sequence > mark ->
        buffer = %{buffer | unread: :queue.in_r(entry, buffer.unread), unread_bytes: buffer.unread_bytes + size}
        return_unread(%{buffer | events: :queue.drop_r(buffer.events)}, mark)

      _ ->
        buffer
    end
  end

  defp trim(%__MODULE__{bytes: bytes, max_bytes: max} = buffer) when bytes <= max, do: buffer

  defp trim(%__MODULE__{} = buffer) do
    case :queue.out(buffer.events) do
      {{:value, {_event, size}}, queue} ->
        trim(%{buffer | events: queue, bytes: buffer.bytes - size})

      {:empty, _queue} when buffer.unread_bytes > buffer.max_unread_bytes ->
        {{:value, {_event, size}}, queue} = :queue.out(buffer.unread)
        trim(%{buffer | unread: queue, bytes: buffer.bytes - size, unread_bytes: buffer.unread_bytes - size})

      {:empty, _queue} ->
        %{buffer | bytes: buffer.unread_bytes}
    end
  end
end
