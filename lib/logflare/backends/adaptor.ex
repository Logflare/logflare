defmodule Logflare.Backends.Adaptor do
  @moduledoc """
  An adaptor represents the module responsible for implementing the interface between the backend and the outside world.

  It should be the **only** point of entry for the backend.
  """

  alias Logflare.LogEvent
  alias Logflare.Endpoints.Query
  alias Logflare.Backends.Backend
  alias Logflare.Backends.AdaptorSupervisor
  alias Logflare.Source

  @type t :: module()
  @type query :: Query.t() | Ecto.Query.t() | String.t() | {String.t(), [term()]}
  @type source_backend :: {Source.t(), Backend.t()}

  def child_spec(%Source{} = source, %Backend{} = backend) do
    adaptor_module = get_adaptor(backend)

    %{
      id: {adaptor_module, source.id, backend.id},
      start: {AdaptorSupervisor, :start_link, [{source, backend}]}
    }
  end

  @doc """
  Retrieves the module for a given backend
  """
  @spec get_adaptor(Backend.t()) :: module()
  def get_adaptor(%Backend{type: type}) do
    mapping = Backend.adaptor_mapping()
    mapping[type]
  end

  @callback start_link(source_backend()) ::
              {:ok, pid()} | :ignore | {:error, term()}

  @doc """
  Optional callback to manipulate log events before queueing.
  """
  @callback pre_ingest(Source.t(), Backend.t(), [LogEvent.t()]) :: [LogEvent.t()]

  @doc """
  Queries the backend using an endpoint query.
  """
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
  def cast_and_validate_config(mod, params) when is_atom(mod) do
    params
    |> mod.cast_config()
    |> mod.validate_config()
  end

  @optional_callbacks pre_ingest: 3
end
