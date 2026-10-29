defmodule Logflare.Backends.Spool.ProviderConfig do
  @moduledoc """
  Resolves shared `:logflare, :spool` config into concrete storage/queue
  modules and a resolved queue ref — used by both the producer side
  (`DurableBuffer.Supervisor`) and the consumer side (`ConsumerPipeline`).
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
    case queue_name(spool_config, queue_mod) do
      nil ->
        nil

      queue_name ->
        case resolve_queue(spool_config, queue_mod, queue_name) do
          {:ok, ref} -> ref
          {:error, _reason} -> nil
        end
    end
  end

  @doc """
  Resolves the queue, retrying `:queue_resolve_retries` times (default 5)
  `:queue_resolve_interval_ms` apart (default 2s) so a node booting before
  DNS is ready still gets it. Logs a warning per failed attempt. Lookups never
  feed the `:queue` health scope, so exhausting them does not hold `/ready`.
  """
  @spec resolve_queue(keyword(), module(), String.t()) :: {:ok, term()} | {:error, term()}
  def resolve_queue(spool_config, queue_mod, queue_name) do
    retries = Keyword.get(spool_config, :queue_resolve_retries, 5)
    interval_ms = Keyword.get(spool_config, :queue_resolve_interval_ms, 2_000)
    do_resolve_queue(queue_mod, queue_name, retries, interval_ms)
  end

  defp do_resolve_queue(queue_mod, queue_name, retries_left, interval_ms) do
    with {:error, reason} <- queue_mod.resolve(queue_name) do
      Logger.warning(
        "spool_provider_config: could not resolve queue ref for #{queue_name} (#{retries_left} retries left): #{inspect(reason)}"
      )

      if retries_left > 0 do
        Process.sleep(interval_ms)
        do_resolve_queue(queue_mod, queue_name, retries_left - 1, interval_ms)
      else
        {:error, reason}
      end
    end
  end

  @spec queue_name(keyword(), module()) :: String.t() | nil
  def queue_name(spool_config, Queue.PubSub), do: Keyword.get(spool_config, :pubsub_topic)
  def queue_name(spool_config, _queue_mod), do: Keyword.get(spool_config, :queue_name)

  defp default_storage_mod(:gcp), do: Storage.GCS
  defp default_storage_mod(_), do: Storage.S3

  defp default_queue_mod(:gcp), do: Queue.PubSub
  defp default_queue_mod(_), do: Queue.SQS
end
