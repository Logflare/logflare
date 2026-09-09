defmodule Logflare.Backends.Adaptor.ClickHouseAdaptor.FinchPoolTimeoutNormalizer do
  @moduledoc """
  Tesla middleware that turns a Finch HTTP/1 pool checkout timeout into `{:error, :pool_timeout}`.

  Finch only returns `%Finch.Error{reason: :pool_timeout}` for HTTP/2 pools. On an
  HTTP/1 pool — which both ClickHouse ingest pools are — a checkout timeout surfaces
  as a `NimblePool` exit that `Finch.HTTP1.Pool` catches and re-raises as a
  `RuntimeError`. An exception is invisible to `Tesla.Middleware.Retry`, which only
  inspects the return value of the stack below it, so a saturated pool would neither
  be retried nor counted as an insert failure.

  Must sit *after* `Tesla.Middleware.Retry` in the middleware list so it runs inside
  the retry loop and its `{:error, :pool_timeout}` is visible to `should_retry`.

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
