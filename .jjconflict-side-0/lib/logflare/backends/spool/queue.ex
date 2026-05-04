defmodule Logflare.Backends.Spool.Queue do
  @moduledoc false

  @type message :: %{id: String.t(), body: String.t()}

  @callback resolve(queue_name :: String.t()) :: {:ok, queue_ref :: String.t()} | {:error, term()}

  @callback receive(queue_ref :: String.t(), opts :: keyword()) ::
              {:ok, [message()]} | {:error, term()}

  @callback ack(queue_ref :: String.t(), id :: String.t()) :: :ok | {:error, term()}

  @callback nack(queue_ref :: String.t(), id :: String.t()) :: :ok | {:error, term()}

  @callback publish(queue_ref :: String.t(), body :: String.t()) :: :ok | {:error, term()}
end
