defmodule Logflare.Backends.SystemBackend do
  @moduledoc """
  Lifecycle hooks for the system backend, distinct from user-configured backends.

  Hooks are optional; adaptors without a hook require no additional setup.
  """

  alias Logflare.Backends.Adaptor
  alias Logflare.Backends.Backend
  alias Logflare.SingleTenant
  alias Logflare.Sources.Source

  @doc "Runs before single-tenant users and sources are seeded."
  @callback on_system_start() :: :ok

  @doc "Runs after a source supervisor starts."
  @callback on_source_start(Source.t()) :: :ok

  @doc "Runs after Supabase sources and endpoints are seeded and source startup is requested."
  @callback on_supabase_start() :: :ok

  @optional_callbacks on_system_start: 0, on_source_start: 1, on_supabase_start: 0

  @spec on_system_start() :: :ok
  def on_system_start, do: invoke(:on_system_start, [])

  @spec on_source_start(Source.t()) :: :ok
  def on_source_start(%Source{} = source), do: invoke(:on_source_start, [source])

  @spec on_supabase_start() :: :ok
  def on_supabase_start, do: invoke(:on_supabase_start, [])

  @spec invoke(atom(), list()) :: :ok
  defp invoke(callback, args) do
    type = if SingleTenant.single_tenant?(), do: SingleTenant.backend_type(), else: :bigquery
    adaptor = Adaptor.get_adaptor(%Backend{type: type})
    Code.ensure_loaded!(adaptor)

    if function_exported?(adaptor, callback, length(args)) do
      apply(adaptor, callback, args)
    else
      :ok
    end
  end
end
