defmodule Logflare.Backends.Adaptor.HttpBased.FinchPoolTimeoutNormalizer do
  @moduledoc """
  Tesla middleware that turns a Finch HTTP/1 pool checkout timeout into `{:error, :pool_timeout}`.

  Finch raises a `RuntimeError` when an HTTP/1 connection cannot be checked out in
  time. Place this middleware before the adapter and after any retry middleware
  so callers can handle the timeout as an ordinary transport error.

  Finch gives no structured way to tell this exception apart from any other
  `RuntimeError`, so it is matched on a stable fragment of the message and anything
  else is re-raised untouched. `FinchPoolTimeoutNormalizerTest` saturates a real
  single-connection HTTP/1 pool, so a Finch upgrade that changes the wording fails
  there rather than silently restoring the old behaviour.
  """

  @behaviour Tesla.Middleware

  import Logflare.Utils.Guards

  @pool_timeout_marker "unable to provide a connection within the timeout"

  @impl Tesla.Middleware
  def call(env, next, _opts) do
    Tesla.run(env, next)
  rescue
    error in RuntimeError ->
      normalize(pool_timeout?(error), error, __STACKTRACE__)
  end

  @spec pool_timeout?(%RuntimeError{}) :: boolean()
  defp pool_timeout?(%RuntimeError{message: message}) when is_non_empty_binary(message),
    do: String.contains?(message, @pool_timeout_marker)

  defp pool_timeout?(_error), do: false

  @spec normalize(boolean(), %RuntimeError{}, Exception.stacktrace()) ::
          {:error, :pool_timeout} | no_return()
  defp normalize(true, _error, _stacktrace), do: {:error, :pool_timeout}
  defp normalize(false, error, stacktrace), do: reraise(error, stacktrace)
end
