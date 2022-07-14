defmodule Logflare.Backends.Adaptor do
  @moduledoc """
  An adaptor represents the module responsible for implementing the interface between the backend and the outside world.

  It should be the **only** point of entry for the backend.
  """

  alias Logflare.{LogEvent, Endpoints.Query, Backends.Adaptor}
  @type t :: module()
  @doc """
  Ingest many log events.
  """
  @callback ingest(identifier(), [LogEvent.t()]) :: :ok

  @doc """
  Checks if the adaptor can execute queries
  """
  @callback queryable? :: boolean()

  @doc """
  Queries the backend using an endpoint query.
  """
  @callback execute_query(identifier(), [%Query{}]) :: {:ok, term()} | {:error, :not_queryable}

  defmacro __using__(_opts) do
    quote do
      @behaviour Adaptor

      def queryable?(), do: false
      def execute_query(_pid, _query) do
        if function_exported?(__MODULE__, :queryable, 0) do
          raise "queryable?/0 callback implemented but query execution callback has not been implemented yet!"
        else
          {:error, :not_queryable}
        end
      end
      def ingest(_pid, _log_events), do: raise("Ingest callback not implemented!")

      defoverridable Adaptor
    end
  end
end
