defmodule Logflare.Backends.Spool.Storage do
  @moduledoc false

  @callback put(bucket :: String.t(), key :: String.t(), body :: binary(), opts :: keyword()) ::
              {:ok, term()} | {:error, term()}

  @callback get(bucket :: String.t(), key :: String.t()) ::
              {:ok, binary()} | {:error, term()}
end
