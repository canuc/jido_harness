defmodule Jido.Harness.RetentionOptions do
  @moduledoc false

  alias Jido.Harness.Error

  @keys [:journal_dir, :memory_bytes, :segment_bytes, :disk_limit_bytes]
  # Only a managed process has a live reader whose unread output is kept.
  @process_keys @keys ++ [:unread_limit_bytes]
  @default_segment_bytes 8 * 1_024 * 1_024
  @default_disk_limit_bytes 256 * 1_024 * 1_024

  @doc """
  Normalizes retention keys and validates byte limits and the journal directory.
  `:process` retention also accepts `unread_limit_bytes`.
  """
  @spec normalize(map(), :process | :resource) :: {:ok, map()} | {:error, Error.t()}
  def normalize(options, kind \\ :resource)

  def normalize(options, kind) when is_map(options) do
    keys = if kind == :process, do: @process_keys, else: @keys

    with {:ok, options} <- normalize_keys(options, keys),
         options = fit_default_segment(options),
         :ok <- validate_values(options) do
      {:ok, options}
    end
  end

  def normalize(_options, _kind), do: {:error, Error.validation("retention must be a map")}

  defp normalize_keys(options, keys) do
    Enum.reduce_while(options, {:ok, %{}}, fn {key, value}, {:ok, normalized} ->
      normalized_key =
        cond do
          is_atom(key) and key in keys -> key
          is_binary(key) -> Enum.find(keys, &(Atom.to_string(&1) == key))
          true -> nil
        end

      cond do
        is_nil(normalized_key) ->
          {:halt, {:error, Error.validation("unknown retention option", details: %{key: key})}}

        Map.has_key?(normalized, normalized_key) ->
          {:halt, {:error, Error.validation("duplicate retention option", details: %{key: normalized_key})}}

        true ->
          {:cont, {:ok, Map.put(normalized, normalized_key, value)}}
      end
    end)
  end

  defp validate_values(options) do
    byte_fields = [:memory_bytes, :segment_bytes, :disk_limit_bytes, :unread_limit_bytes]
    invalid = Enum.find(byte_fields, &(Map.has_key?(options, &1) and not positive_integer?(Map.get(options, &1))))
    segment = Map.get(options, :segment_bytes, @default_segment_bytes)
    disk_limit = Map.get(options, :disk_limit_bytes, @default_disk_limit_bytes)

    cond do
      invalid ->
        {:error, Error.validation("retention byte limits must be positive integers", details: %{field: invalid})}

      Map.has_key?(options, :journal_dir) and not valid_directory?(options.journal_dir) ->
        {:error, Error.validation("retention journal_dir must be a non-empty path")}

      segment > disk_limit ->
        {:error,
         Error.validation("retention segment_bytes cannot exceed disk_limit_bytes",
           details: %{segment_bytes: segment, disk_limit_bytes: disk_limit}
         )}

      true ->
        :ok
    end
  end

  defp fit_default_segment(%{disk_limit_bytes: disk_limit} = options)
       when is_integer(disk_limit) and disk_limit > 0 do
    Map.put_new(options, :segment_bytes, min(@default_segment_bytes, disk_limit))
  end

  defp fit_default_segment(options), do: options

  defp positive_integer?(value), do: is_integer(value) and value > 0
  defp valid_directory?(value), do: is_binary(value) and String.trim(value) != ""
end
