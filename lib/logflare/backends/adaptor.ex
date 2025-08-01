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
  Returns the list of query languages supported by this adaptor.
  """
  @callback get_supported_languages() :: [atom()]

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
  Returns the list of supported query languages for a given backend.
  Returns an empty list if the adaptor does not support querying.
  """
  @spec get_supported_languages(Backend.t()) :: [atom()]
  def get_supported_languages(%Backend{} = backend) do
    adaptor = get_adaptor(backend)

    if function_exported?(adaptor, :get_supported_languages, 0) do
      adaptor.get_supported_languages()
    else
      []
    end
  end

  @doc """
  Optional callback to transform a query from one language/dialect to the backend's expected format.

  This allows adaptors to handle query language transformations specific to their backend.
  For example, a PostgreSQL adaptor might transform BigQuery SQL or LQL to PostgreSQL SQL.

  ## Parameters

    * `query` - The query string to transform
    * `from_language` - The source query language (e.g., :bq_sql, :lql, :pg_sql)
    * `context` - Additional context that might be needed for transformation (e.g., schema_prefix)

  ## Returns

  `{:ok, transformed_query}` or `{:error, reason}`. If the callback is not implemented or
  returns `{:error, :not_supported}`, the query will be passed through unchanged.
  """
  @callback transform_query(
              query :: String.t(),
              from_language :: atom(),
              context :: map()
            ) :: {:ok, String.t()} | {:error, term()}

  @doc """
  Optional callback to map query parameters from the original query context to the backend's expected format.

  This is useful for adaptors that need special parameter handling, such as PostgreSQL which needs
  to map `@param` style parameters from the original BigQuery query to $1, $2, etc. style parameters
  in the translated PostgreSQL query.

  ## Parameters

    * `original_query` - The original query string before any translation
    * `transformed_query` - The query string after translation/transformation
    * `declared_params` - List of parameter names declared in the original query
    * `input_params` - Map of parameter names to values provided by the user

  ## Returns

  A list of parameter values in the order expected by the transformed query.
  """
  @callback map_query_parameters(
              original_query :: String.t(),
              transformed_query :: String.t(),
              declared_params :: [String.t()],
              input_params :: map()
            ) :: [term()]

  @doc """
  Sends an alert notification for a given backend.
  """
  @callback send_alert(Backend.t(), AlertQuery.t(), [term()]) :: :ok | {:error, term()}

  @optional_callbacks pre_ingest: 3,
                      transform_config: 1,
                      format_batch: 1,
                      format_batch: 2,
                      test_connection: 2,
                      get_supported_languages: 0,
                      transform_query: 3,
                      map_query_parameters: 4,
                      send_alert: 3
end
