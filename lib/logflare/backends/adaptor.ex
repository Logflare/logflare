defmodule Logflare.Backends.Adaptor do
  @moduledoc """
  An adaptor represents the module responsible for implementing the interface between the backend and the outside world.

  It should be the **only** point of entry for the backend.
  """

  alias Logflare.{LogEvent, Endpoint.Query}

  @doc """
  Ingest many log events.
  """
  @callback ingest([LogEvent.t()]) :: :ok

  @doc """
  Checks if the adaptor can execute queries
  """
  @callback queryable? :: boolean()

  @doc """
  Queries the backend using an endpoint query.
  """
  @callback execute_query([%Query{}]) :: {:ok, term()} | {:error, :not_queryable}
end
