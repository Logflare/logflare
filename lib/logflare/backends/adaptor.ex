defmodule Logflare.Backends.Adaptor do
  @moduledoc """
  An adaptor represents the module responsible for implementing the interface between the backend and the outside world.

  It should be the **only** point of entry for the backend.
  """

  alias Logflare.LogEvent
  alias Logflare.Endpoints.Query

  @type t :: module()

  @doc """
  Ingest many log events.
  """
  @callback ingest(identifier(), [LogEvent.t()]) :: :ok

  @doc """
  Queries the backend using an endpoint query.
  """
  @typep query :: Query.t() | Ecto.Query.t() | String.t() | {String.t(), [term()]}
  @callback execute_query(identifier(), query()) :: {:ok, [term()]} | {:error, :not_implemented}

  @doc """
  Typecasts config params.
  """
  @callback cast_config(param :: map()) :: Ecto.Changeset.t()

  @doc """
  Validates a given adaptor's configuration, using Ecto.Changeset functions. Accepts a chaangeset
  """
  @callback validate_config(changeset :: Ecto.Changeset.t()) :: Ecto.Changeset.t()

  @doc """
  Validate configuration for given adaptor implementation
  """
  @spec cast_and_validate_config(module(), map()) :: Ecto.Changeset.t()
  def cast_and_validate_config(mod, params) do
    params
    |> mod.cast_config()
    |> mod.validate_config()
  end
end
