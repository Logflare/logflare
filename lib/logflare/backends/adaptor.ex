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
  @typep query :: Query.t() | Ecto.Query.t() | String.t()
  @callback execute_query(identifier(), query()) :: {:ok, [term()]} | {:error, :not_queryable}

  @doc """
  Typecasts config params.
  """
  @callback cast_config(param :: map()) :: Ecto.Changeset.t()

  @doc """
  Validates a given adaptor's configuration, using Ecto.Changeset functions. Accepts a chaangeset
  """
  @callback validate_config(changeset :: Ecto.Changeset.t()) :: Ecto.Changeset.t()

  defmacro __using__(_opts) do
    quote do
      @behaviour Logflare.Backends.Adaptor

      @impl true
      def queryable?(), do: false

      @impl true
      def execute_query(_pid, _query) do
        if function_exported?(__MODULE__, :queryable, 0) do
          raise "queryable?/0 callback implemented but query execution callback has not been implemented yet!"
        else
          {:error, :not_queryable}
        end
      end

      @impl true
      def ingest(_pid, _log_events), do: raise("Ingest callback not implemented!")

      @impl true
      def validate_config(_config_changeset),
        do: raise("Config validation callback not implemented!")

      @impl true
      def cast_config(_config), do: raise("Config casting callback not implemented!")

      def cast_and_validate_config(params) do
        params
        |> cast_config()
        |> validate_config()
      end

      defoverridable Adaptor
    end
  end
end
