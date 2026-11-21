defmodule Jido.Harness.Diagnostics do
  @moduledoc false

  # OTP attaches the original exception stack to its report after format_status.
  # Function-clause stack frames can include the entire request and worker state.
  @doc false
  defmacro __before_compile__(env) do
    callbacks = [init: 1, handle_call: 3, handle_cast: 2, handle_info: 2, handle_continue: 2, terminate: 2]

    for {name, arity} <- callbacks, Module.defines?(env.module, {name, arity}, :def) do
      args = Macro.generate_arguments(arity, env.module)

      quote do
        defoverridable [{unquote(name), unquote(arity)}]

        def unquote(name)(unquote_splicing(args)) do
          super(unquote_splicing(args))
        catch
          kind, _reason ->
            Jido.Harness.Diagnostics.raise_callback_failure(kind, __STACKTRACE__)
        end
      end
    end
  end

  @doc "Raises a callback failure using only stack locations and arities."
  @spec raise_callback_failure(:error | :exit | :throw, Exception.stacktrace()) :: no_return()
  def raise_callback_failure(kind, stack) do
    locations =
      Enum.map(stack, fn
        {module, function, args, info} when is_list(args) ->
          {module, function, length(args), Keyword.take(info, [:file, :line])}

        {module, function, arity, info} ->
          {module, function, arity, Keyword.take(info, [:file, :line])}
      end)

    :erlang.raise(kind, :runner_callback_failed, locations)
  end

  # Supervisors report start arguments independently of the child status.
  @doc "Builds a temporary child specification with opaque launch arguments."
  @spec child_spec(module(), term()) :: Supervisor.child_spec()
  def child_spec(module, args) do
    %{id: module, restart: :temporary, start: {:erlang, :apply, [fn -> module.start_link(args) end, []]}}
  end

  @doc "Removes callback data from OTP status and termination reports."
  @spec format_status(map()) :: map()
  def format_status(status) do
    Map.merge(status, Map.take(%{state: :redacted, message: :redacted, reason: :redacted, log: []}, Map.keys(status)))
  end
end
