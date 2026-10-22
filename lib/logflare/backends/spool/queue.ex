defmodule Logflare.Backends.Spool.Queue do
  @moduledoc false

  alias Logflare.Backends.Spool.Health

  @type message :: %{id: String.t(), body: String.t()}

  @callback resolve(queue_name :: String.t()) :: {:ok, queue_ref :: String.t()} | {:error, term()}

  @callback receive(queue_ref :: String.t(), opts :: keyword()) ::
              {:ok, [message()]} | {:error, term()}

  @callback ack(queue_ref :: String.t(), id :: String.t()) :: :ok | {:error, term()}

  @callback nack(queue_ref :: String.t(), id :: String.t()) :: :ok | {:error, term()}

  @callback publish(queue_ref :: String.t(), body :: String.t()) :: :ok | {:error, term()}

  @doc """
  Feeds a queue call's result into the `:queue` health scope and returns it
  unchanged. Transport failures and 5xx count against it; any other HTTP
  answer proves reachability; other errors are ignored.
  """
  @spec track_health(result) :: result when result: term()
  def track_health(result) do
    case reachability(result) do
      :reachable -> Health.report_recovery!(:queue)
      :unreachable -> Health.report_failure!(:queue)
      :unknown -> :ok
    end

    result
  end

  defp reachability(:ok), do: :reachable
  defp reachability({:ok, _}), do: :reachable
  defp reachability({:error, {:http_error, status, _}}) when status < 500, do: :reachable
  defp reachability({:error, {:http_error, _status, _}}), do: :unreachable
  defp reachability({:error, %Tesla.Env{status: status}}) when status < 500, do: :reachable
  defp reachability({:error, %Tesla.Env{}}), do: :unreachable
  defp reachability({:error, %Mint.TransportError{}}), do: :unreachable
  defp reachability({:error, %Mint.HTTPError{}}), do: :unreachable
  defp reachability({:error, :pool_timeout}), do: :unreachable
  defp reachability(_result), do: :unknown
end
