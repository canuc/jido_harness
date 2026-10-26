defmodule Jido.Harness.Adapters.Claude do
  @moduledoc "Claude Code provider profile using claude-agent-acp."
  @behaviour Jido.Harness.Adapter

  alias Jido.Harness.{AdapterSpec, Adapters.Helpers, Capabilities, Error}

  @impl true
  def spec do
    %AdapterSpec{
      provider: :claude,
      name: "Claude Code",
      executable: "claude",
      docs_url: "https://docs.anthropic.com/en/docs/claude-code",
      capabilities: %Capabilities{
        streaming?: true,
        tool_calls?: true,
        tool_results?: true,
        thinking?: true,
        resume?: true,
        usage?: true,
        native_cancel?: true
      },
      acp_agent:
        Jido.Harness.ACPAgentSpec.adapter(
          "claude-agent-acp",
          "@agentclientprotocol/claude-agent-acp@0.70.0",
          %{
            capabilities: %{load_session: true, multimodal: true, dynamic_model: true, usage: true},
            session_options: [:provider_session_id, :mcp_config, :add_dirs, :disallowed_tools],
            session_provider_options: :adapter,
            turn_options: [:attachments, :content],
            configuration_options: [:model]
          }
        ),
      normalized_options: [:model, :provider_session_id, :mcp_config, :add_dirs, :disallowed_tools, :attachments],
      provider_options: [:strict_mcp_config],
      install: %{npm: "@anthropic-ai/claude-code"}
    }
  end

  @impl true
  def acp_validate_request(request) do
    if strict_mcp_config(request.provider_options) in [nil, true, false],
      do: :ok,
      else:
        {:error,
         Error.validation("strict_mcp_config must be a boolean",
           provider: :claude,
           details: %{option: :strict_mcp_config}
         )}
  end

  # claude-agent-acp reads `_meta.claudeCode.options` on session/new, load,
  # resume and fork and spreads it into the Claude Agent SDK options.
  @impl true
  def acp_session_meta(request) do
    options = Map.merge(disallowed_tools(request.disallowed_tools), strict_mcp(request.provider_options))

    if map_size(options) > 0, do: %{"claudeCode" => %{"options" => options}}
  end

  defp disallowed_tools([_ | _] = tools), do: %{"disallowedTools" => tools}
  defp disallowed_tools(_tools), do: %{}

  defp strict_mcp(provider_options) do
    if strict_mcp_config(provider_options) == true, do: %{"strictMcpConfig" => true}, else: %{}
  end

  defp strict_mcp_config(provider_options),
    do: Map.get(provider_options, :strict_mcp_config, Map.get(provider_options, "strict_mcp_config"))

  @impl true
  def status(config),
    do:
      Helpers.status(
        :claude,
        spec().executable,
        ["ANTHROPIC_AUTH_TOKEN", "ANTHROPIC_API_KEY", "CLAUDE_CODE_API_KEY"],
        config,
        capabilities: spec().capabilities
      )

  @impl true
  def install(_config, options), do: Helpers.install_npm(:claude, "@anthropic-ai/claude-code", options)
end
