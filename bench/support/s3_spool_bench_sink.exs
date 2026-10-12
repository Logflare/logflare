defmodule Logflare.Bench.S3SpoolSink do
  @moduledoc false

  @behaviour Logflare.Backends.Spool.Queue
  @behaviour Logflare.Backends.Spool.Storage

  @counter_key {__MODULE__, :counter}

  @spec reset() :: :ok
  def reset do
    :persistent_term.put(@counter_key, :atomics.new(3, signed: false))
    :ok
  end

  @spec snapshot() :: %{
          events: non_neg_integer(),
          bytes: non_neg_integer(),
          files: non_neg_integer()
        }
  def snapshot do
    counter = :persistent_term.get(@counter_key)

    %{
      events: :atomics.get(counter, 1),
      bytes: :atomics.get(counter, 2),
      files: :atomics.get(counter, 3)
    }
  end

  @impl Logflare.Backends.Spool.Storage
  def put(_bucket, _key, body, _opts) do
    counter = :persistent_term.get(@counter_key)
    :atomics.add(counter, 2, byte_size(body))
    :atomics.add(counter, 3, 1)
    {:ok, %{}}
  end

  @impl Logflare.Backends.Spool.Storage
  def get(_bucket, _key), do: {:error, :not_found}

  @impl Logflare.Backends.Spool.Queue
  def resolve(_queue_name), do: {:ok, "benchmark-queue"}

  @impl Logflare.Backends.Spool.Queue
  def receive(_queue_ref, _opts), do: {:ok, []}

  @impl Logflare.Backends.Spool.Queue
  def ack(_queue_ref, _id), do: :ok

  @impl Logflare.Backends.Spool.Queue
  def nack(_queue_ref, _id), do: :ok

  @impl Logflare.Backends.Spool.Queue
  def publish(_queue_ref, body) do
    %{"event_count" => event_count} = Jason.decode!(body)
    counter = :persistent_term.get(@counter_key)
    :atomics.add(counter, 1, event_count)
    :ok
  end
end
