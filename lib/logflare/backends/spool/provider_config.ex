defmodule Logflare.Backends.Spool.ProviderConfig do
  @moduledoc """
  Resolves shared `:logflare, :spool` config into concrete storage/queue
  modules and a resolved queue ref — used by both the producer side
  (`DurableBuffer.Supervisor`) and the consumer side (`ConsumerPipeline`).
  """

  alias Logflare.Backends.Spool.Health
  alias Logflare.Backends.Spool.Queue
  alias Logflare.Backends.Spool.Storage

  require Logger

  @default_queue_resolve_retries 3
  @default_queue_resolve_interval_ms 2_000

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
  Resolves `queue_name` through `queue_mod`, retrying up to
  `:queue_resolve_retries` times (default #{@default_queue_resolve_retries}),
  `:queue_resolve_interval_ms` apart (default #{@default_queue_resolve_interval_ms}),
  so a node that boots before DNS or the network is ready still gets its
  queue. Marks the `:queue` health scope unhealthy if every attempt fails.
  """
  @spec resolve_queue(keyword(), module(), String.t()) :: {:ok, term()} | {:error, term()}
  def resolve_queue(spool_config, queue_mod, queue_name) do
    retries = Keyword.get(spool_config, :queue_resolve_retries, @default_queue_resolve_retries)

    interval_ms =
      Keyword.get(spool_config, :queue_resolve_interval_ms, @default_queue_resolve_interval_ms)

    do_resolve_queue(queue_mod, queue_name, retries, interval_ms)
  end

  defp do_resolve_queue(queue_mod, queue_name, retries_left, interval_ms) do
    case queue_mod.resolve(queue_name) do
      {:ok, ref} ->
        {:ok, ref}

      {:error, reason} when retries_left > 0 ->
        Logger.warning(
          "spool_provider_config: could not resolve queue ref for #{queue_name}, retrying in #{interval_ms}ms (#{retries_left} left): #{inspect(reason)}"
        )

        Process.sleep(interval_ms)
        do_resolve_queue(queue_mod, queue_name, retries_left - 1, interval_ms)

      {:error, reason} ->
        Health.mark_unhealthy!(:queue)

        Logger.warning(
          "spool_provider_config: could not resolve queue ref for #{queue_name}: #{inspect(reason)}"
        )

        {:error, reason}
    end
  end

  defp queue_name(spool_config, Queue.PubSub), do: Keyword.get(spool_config, :pubsub_topic)
  defp queue_name(spool_config, _queue_mod), do: Keyword.get(spool_config, :queue_name)

  defp default_storage_mod(:gcp), do: Storage.GCS
  defp default_storage_mod(_), do: Storage.S3

  defp default_queue_mod(:gcp), do: Queue.PubSub
  defp default_queue_mod(_), do: Queue.SQS
end
