defmodule Logflare.Backends.Spool.Storage do
  @moduledoc false

  @type object :: {storage_mod :: module(), bucket :: String.t(), key :: String.t()}

  @callback put(bucket :: String.t(), key :: String.t(), body :: binary(), opts :: keyword()) ::
              {:ok, term()} | {:error, term()}

  @doc """
  Fetches an object's contents. Implementations must normalize a missing
  object to `{:error, :not_found}` (instead of a provider-specific 404
  shape) so callers can detect a stale/already-deleted spool file the same
  way regardless of which storage backend is configured.
  """
  @callback get(bucket :: String.t(), key :: String.t()) ::
              {:ok, binary()} | {:error, :not_found} | {:error, term()}

  @doc """
  Deletes an object. Implementations must treat an already-missing object as
  success, so a repeated delete (e.g. after a redelivered queue message) is
  harmless.
  """
  @callback delete(bucket :: String.t(), key :: String.t()) :: :ok | {:error, term()}
end
