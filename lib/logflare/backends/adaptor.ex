defmodule Logflare.Backends.Adaptor do
  @moduledoc """
  An adaptor represents the module responsible for implementing the interface between the backend and the outside world.

  It should be the **only** point of entry for the backend.
  The Adaptor should consume events from the IngestedEventQueue.
  """

  alias Logflare.Alerting.AlertQuery
  alias Logflare.Backends.AdaptorSupervisor
  alias Logflare.Backends.Backend
  alias Logflare.Endpoints.Query
  alias Logflare.LogEvent
  alias Logflare.Sources.Source

  @type t :: module()
  @type query :: Query.t() | Ecto.Query.t() | String.t() | {String.t(), [term()]}
  @type source_backend :: {Source.t(), Backend.t()}
  @type start_link_arg :: source_backend() | Backend.t()
  @type query_identifier :: identifier() | Backend.t() | tuple()

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
  def supports_default_ingest?(%Backend{} = backend) do
    adaptor = get_adaptor(backend)

    if function_exported?(adaptor, :supports_default_ingest?, 0) do
      adaptor.supports_default_ingest?()
    else
      false
    end
  end

  @doc """
  Returns true if a given `Backend` supports consolidated ingestion.

  Defaults to false.
  """
  @spec consolidated_ingest?(Backend.t()) :: boolean()
  def consolidated_ingest?(%Backend{} = backend) do
    adaptor = get_adaptor(backend)

    if function_exported?(adaptor, :consolidated_ingest?, 0) do
      adaptor.consolidated_ingest?()
    else
      false
    end
  end

  @doc """
  Returns true if a provided `Backend` supports transforming queries.

  Default to false.
  """
  @spec can_transform_query?(Backend.t()) :: boolean()
  def can_transform_query?(%Backend{} = backend) do
    backend
    |> get_adaptor()
    |> function_exported?(:transform_query, 3)
  end

  @doc """
  Returns true if a provided `Backend` supports mapping of query parameters.

  Default to false.
  """
  @spec can_map_query_parameters?(Backend.t()) :: boolean()
  def can_map_query_parameters?(%Backend{} = backend) do
    backend
    |> get_adaptor()
    |> function_exported?(:map_query_parameters, 4)
  end

  @doc """
  Returns true if a provided `Backend` supports executing queries.
  """
  @spec can_query?(Backend.t()) :: boolean()
  def can_query?(%Backend{} = backend) do
    backend
    |> get_adaptor()
    |> function_exported?(:execute_query, 3)
  end

  @callback start_link(start_link_arg()) ::
              {:ok, pid()} | :ignore | {:error, term()}

  @doc """
  Optional callback to manipulate a batch before it is sent. This is pipeline specific, and must be handled by the underlying pipeline.
  """
  @callback format_batch([LogEvent.t()]) :: map() | list(map())
  @callback format_batch([LogEvent.t()], config :: map()) :: map() | list(map())

  @doc """
  Typecasts config params.
  """
  @callback cast_config(param :: map()) :: Ecto.Changeset.t()

  @doc """
  Optional callback to convert an Ecto query to the backend's native SQL format.
  """
  @callback ecto_to_sql(query :: Ecto.Query.t(), opts :: Keyword.t()) ::
              {:ok, {String.t(), [term()]}} | {:error, term()}

  @doc """
  Queries the backend using an endpoint query.

  The `opts` parameter can be used to include backend-specific options.

  Depending on the backend, this will return a list of rows or
  a map with rows and optional metadata (e.g., total_bytes_processed).
  """
  @callback execute_query(query_identifier(), query(), opts :: Keyword.t()) ::
              {:ok, [term()]} | {:ok, map()} | {:error, :not_implemented} | {:error, term()}

  @doc """
  Optional callback to map query parameters from the original query context to the backend's expected format.

  This is useful for adaptors that need special parameter handling, such as PostgreSQL which needs
  to map `@param` style parameters from the original BigQuery query to $1, $2, etc. style parameters
  in the translated PostgreSQL query.

  Returns a list of parameter values in the order expected by the transformed query.

  ## Parameters
    * `original_query` - The original query string before any translation
    * `transformed_query` - The query string after translation/transformation
    * `declared_params` - List of parameter names declared in the original query
    * `input_params` - Map of parameter names to values provided by the user
  """
  @callback map_query_parameters(
              original_query :: String.t(),
              transformed_query :: String.t(),
              declared_params :: [String.t()],
              input_params :: map()
            ) :: [term()]

  @doc """
  Optional callback to manipulate log events before queueing.
  """
  @callback pre_ingest(Source.t(), Backend.t(), [LogEvent.t()]) :: [LogEvent.t()]

  @doc """
  Optional callback to test the underlying connection for an adaptor. May not be applicable for some adaptors.
  """
  @callback test_connection(Backend.t()) :: :ok | {:error, term()}

  @doc """
  Optional callback to transform a stored backend config before usage.
  Example use cases: when an adaptor extends another adaptor by customizing the end configuration.
  """
  @callback transform_config(backend :: Backend.t()) :: map()

  @doc """
  Optional callback to transform a query from one language/dialect to the backend's expected format.
  This allows adaptors to handle query language transformations specific to their backend.

  If this callback is not implemented or returns `{:error, :not_supported}`,
  the query will be passed through unchanged.

  ## Parameters
    * `query` - The query string to transform
    * `from_language` - The source query language (e.g., :bq_sql, :lql, :pg_sql)
    * `context` - Additional context that might be needed for transformation (e.g., schema_prefix)
  """
  @callback transform_query(query :: String.t(), from_language :: atom(), context :: map()) ::
              {:ok, String.t()} | {:error, term()}

  @doc """
  Sends an alert notification for a given backend.
  """
  @callback send_alert(Backend.t(), AlertQuery.t(), [term()]) :: :ok | {:error, term()}

  @doc """
  Indicates if this adaptor supports being a default ingest backend.
  """
  @callback supports_default_ingest?() :: boolean()

  @doc """
  Indicates if this adaptor uses consolidated ingestion.

  When true, all sources using backends of this type will share a single ingestion
  pipeline keyed by `backend_id` only, enabling larger batch sizes.
  """
  @callback consolidated_ingest?() :: boolean()

  @doc """
  Validates a given adaptor's configuration, using Ecto.Changeset functions. Accepts a chaangeset
  """
  @callback validate_config(changeset :: Ecto.Changeset.t()) :: Ecto.Changeset.t()

  @doc """
  Redacts a given adaptor's configuration. Return the config unchanged if there is no redaction needed.
  Always works on atom keys.
  """
  @callback redact_config(config :: map()) :: map()

  @optional_callbacks ecto_to_sql: 2,
                      format_batch: 1,
                      format_batch: 2,
                      execute_query: 3,
                      map_query_parameters: 4,
                      pre_ingest: 3,
                      test_connection: 1,
                      transform_config: 1,
                      transform_query: 3,
                      send_alert: 3,
                      supports_default_ingest?: 0,
                      consolidated_ingest?: 0,
                      redact_config: 1
end
