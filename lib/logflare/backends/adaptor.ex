defmodule Logflare.Backends.Adaptor do
  @moduledoc """
  An adaptor represents the module responsible for implementing the interface between the backend and the outside world.

  It should be the **only** point of entry for the backend.
  The Adaptor should consume events from the IngestedEventQueue.
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

  @spec get_backend_config(Backend.t()) :: map()
  def get_backend_config(%Backend{config: config} = backend) do
    adaptor = get_adaptor(backend)

    if function_exported?(adaptor, :transform_config, 1) do
      adaptor.transform_config(backend)
    else
      config
    end
  end

  @callback start_link(source_backend()) ::
              {:ok, pid()} | :ignore | {:error, term()}

  @doc """
  Optional callback to transform a stored backend config before usage.
  Example use cases: when an adaptor extends another adaptor by customizing the end configuration.
  """
  @callback transform_config(backend :: Backend.t()) :: map()

  @doc """
  Optional callback to manipulate log events before queueing.
  """
  @callback pre_ingest(Source.t(), Backend.t(), [LogEvent.t()]) :: [LogEvent.t()]

  @doc """
  Optional callback to manipulate a batch before it is sent. This is pipeline specific, and must be handled by the underlying pipeline.
  """
  @callback format_batch([LogEvent.t()]) :: map() | list(map())
  @callback format_batch([LogEvent.t()], config :: map()) :: map() | list(map())

  @doc """
  Optional callback to test the underlying connection for an adaptor. May not be applicable for some adaptors.
  """
  @callback test_connection(Source.t(), Backend.t()) :: :ok | {:error, term()}

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

  @doc """
  Returns true if a given `Backend` supports being used for default ingest.

  Default to false.
  """
  @spec supports_default_ingest?(Backend.t()) :: boolean()
  def supports_default_ingest?(backend) do
    adaptor = get_adaptor(backend)

    if function_exported?(adaptor, :supports_default_ingest?, 0) do
      adaptor.supports_default_ingest?()
    else
      false
    end
  end

  @doc """
  Sends an alert notification for a given backend.
  """
  @callback send_alert(Backend.t(), AlertQuery.t(), [term()]) :: :ok | {:error, term()}

  @doc """
  Indicates if this adaptor supports being a default ingest backend.
  """
  @callback supports_default_ingest?() :: boolean()

  @optional_callbacks pre_ingest: 3,
                      transform_config: 1,
                      format_batch: 1,
                      format_batch: 2,
                      test_connection: 2,
                      send_alert: 3,
                      supports_default_ingest?: 0
end
