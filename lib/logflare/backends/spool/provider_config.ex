defmodule Logflare.Backends.Spool.ProviderConfig do
  @moduledoc """
  Resolves shared `:logflare, :spool` config into concrete storage/queue
  modules and a resolved queue ref — used by `PartitionSupervisor`
  regardless of which `Logflare.Backends.Spool.Buffer` its partitions use.
  """

  alias Logflare.Backends.Spool.Queue
  alias Logflare.Backends.Spool.Storage

  require Logger

  @spec resolve_mods(keyword()) :: {module(), module()}
  def resolve_mods(spool_config) do
    provider = Keyword.get(spool_config, :provider, :aws)
    storage_mod = Keyword.get(spool_config, :storage_mod, default_storage_mod(provider))
    queue_mod = Keyword.get(spool_config, :queue_mod, default_queue_mod(provider))
    {storage_mod, queue_mod}
  end

  @spec resolve_queue_ref(keyword(), module()) :: term() | nil
  def resolve_queue_ref(spool_config, queue_mod) do
    name = Keyword.get(spool_config, :pubsub_topic) || Keyword.get(spool_config, :queue_name)

    case name do
      nil ->
        nil

      queue_name ->
        case queue_mod.resolve(queue_name) do
          {:ok, ref} ->
            ref

          {:error, reason} ->
            Logger.warning(
              "spool_provider_config: could not resolve queue ref for #{queue_name}: #{inspect(reason)}"
            )

            nil
        end
    end
  end

  defp default_storage_mod(:gcp), do: Storage.GCS
  defp default_storage_mod(_), do: Storage.S3

  defp default_queue_mod(:gcp), do: Queue.PubSub
  defp default_queue_mod(_), do: Queue.SQS
end
