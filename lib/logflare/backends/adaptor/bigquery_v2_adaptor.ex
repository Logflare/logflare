defmodule Logflare.Backends.Adaptor.BigQueryV2Adaptor do
  @moduledoc """
  Prototype BigQuery backend that writes into fixed-schema OTEL tables
  (`otel_logs`, `otel_metrics`, `otel_traces`) shared by all sources of the
  backend, instead of one schema-evolving table per source.

  Uses consolidated ingestion: one adaptor tree per backend across all sources
  (see `Logflare.Backends.ConsolidatedSup`). Table layout lives in
  `BigQueryV2Adaptor.TableSchema`, ingestion in `BigQueryV2Adaptor.Pipeline`.

  Ingest-only: querying (Endpoints, LQL, search) is not supported.
  """

  @behaviour Logflare.Backends.Adaptor

  use Supervisor

  require Logger

  alias __MODULE__.Pipeline
  alias __MODULE__.Provisioner
  alias __MODULE__.TableSchema
  alias GoogleApi.BigQuery.V2.Api
  alias Logflare.Backends
  alias Logflare.Backends.Adaptor.BigQueryAdaptor
  alias Logflare.Backends.Adaptor.BigQueryAdaptor.ArrowIPC
  alias Logflare.Backends.Adaptor.BigQueryAdaptor.GoogleApiClient
  alias Logflare.Backends.Backend
  alias Logflare.Backends.IngestEventQueue
  alias Logflare.Google.BigQuery.GenUtils
  alias Logflare.LogEvent.TypeDetection

  @doc false
  def child_spec(%Backend{} = backend) do
    %{
      id: __MODULE__,
      start: {__MODULE__, :start_link, [backend]}
    }
  end

  @doc false
  @impl Logflare.Backends.Adaptor
  @spec start_link(Backend.t()) :: Supervisor.on_start()
  def start_link(%Backend{} = backend) do
    Supervisor.start_link(__MODULE__, backend, name: Backends.via_backend(backend, __MODULE__))
  end

  @impl Supervisor
  def init(%Backend{} = backend) do
    backend = Backends.typecast_config_string_map_to_atom_map(backend)

    IngestEventQueue.upsert_tid({:consolidated, backend.id, nil})
    IngestEventQueue.current_generation_tid({:consolidated, backend.id})

    provisioner =
      if Application.get_env(:logflare, :env) != :test,
        do: [Provisioner.child_spec(backend)],
        else: []

    pipeline = {Pipeline, name: Backends.via_backend(backend, Pipeline), backend: backend}

    Supervisor.init(provisioner ++ [pipeline], strategy: :one_for_one)
  end

  @impl Logflare.Backends.Adaptor
  def consolidated_ingest?, do: true

  @impl Logflare.Backends.Adaptor
  def supports_default_ingest?, do: false

  @impl Logflare.Backends.Adaptor
  def cast_config(params, existing_config \\ %{}) do
    BigQueryAdaptor.cast_config(params, existing_config)
  end

  @impl Logflare.Backends.Adaptor
  def validate_config(changeset) do
    changeset
    |> Ecto.Changeset.validate_required([:project_id, :dataset_id])
    |> BigQueryAdaptor.validate_config()
  end

  @impl Logflare.Backends.Adaptor
  def redact_config(config), do: config

  @impl Logflare.Backends.Adaptor
  defdelegate sanitize_config_for_display(config), to: BigQueryAdaptor

  @doc """
  Checks that the configured dataset exists and is readable with Logflare's credentials.
  """
  @impl Logflare.Backends.Adaptor
  @spec test_connection(Backend.t()) :: :ok | {:error, :dataset_unavailable}
  def test_connection(%Backend{} = backend) do
    case check_dataset(backend) do
      :ok -> :ok
      {:error, _message} -> {:error, :dataset_unavailable}
    end
  end

  @doc """
  Like `test_connection/1`, but returns the Google API error message when the dataset is
  missing or not readable.
  """
  @spec check_dataset(Backend.t()) :: :ok | {:error, String.t()}
  def check_dataset(%Backend{config: %{project_id: project_id, dataset_id: dataset_id}}) do
    GenUtils.get_conn()
    |> Api.Datasets.bigquery_datasets_get(project_id, dataset_id)
    |> case do
      {:ok, _dataset} -> :ok
      {:error, reason} -> {:error, GenUtils.get_tesla_error_message(reason)}
    end
  end

  @doc """
  Appends pre-encoded NDJSON rows to the event type's table through the Storage Write API.
  Rows are encoded to Arrow against the table's fixed schema, never an inferred one.
  """
  @spec insert_rows(Backend.t(), TypeDetection.event_type(), iodata()) :: :ok | {:error, term()}
  def insert_rows(%Backend{config: config}, event_type, ndjson) do
    compression = Application.get_env(:logflare, :arrow_ipc_compression, :zstd)
    fields = TableSchema.arrow_fields(event_type)

    case ArrowIPC.get_ipc_bytes_with_schema(IO.iodata_to_binary(ndjson), fields, compression) do
      {:ok, arrow_schema, batch_msgs} ->
        context = [project_id: config.project_id, dataset_id: config.dataset_id]
        arrow_data = GoogleApiClient.to_arrow_data(arrow_schema, batch_msgs)

        case GoogleApiClient.append_rows(
               {:arrow, arrow_data},
               context,
               TableSchema.table_name(event_type)
             ) do
          :ok -> :ok
          {:error, _reason} = error -> error
          _failed_requests -> {:error, :append_failed}
        end

      {:error, message} ->
        {:error, {:encode_failed, message}}
    end
  end

  @doc """
  Creates every table in `TableSchema` that does not exist yet. Existing tables are left
  as they are, with a warning logged when their schema version differs from `TableSchema`.
  """
  @spec provision_tables(Backend.t()) :: :ok | {:error, term()}
  def provision_tables(%Backend{} = backend) do
    Enum.reduce_while(TableSchema.event_types(), :ok, fn event_type, :ok ->
      case ensure_table(backend, event_type) do
        :ok -> {:cont, :ok}
        {:error, _reason} = error -> {:halt, error}
      end
    end)
  end

  @spec ensure_table(Backend.t(), TypeDetection.event_type()) :: :ok | {:error, term()}
  defp ensure_table(%Backend{config: config} = backend, event_type) do
    %{project_id: project_id, dataset_id: dataset_id} = config
    table = TableSchema.to_table(event_type, project_id, dataset_id)

    GenUtils.get_conn()
    |> Api.Tables.bigquery_tables_insert(project_id, dataset_id, body: table)
    |> case do
      {:ok, _table} ->
        Logger.info("BigQuery v2 table created",
          backend_id: backend.id,
          table_name: TableSchema.table_name(event_type)
        )

        :ok

      {:error, %Tesla.Env{status: 409}} ->
        warn_on_schema_drift(backend, event_type)

      {:error, reason} ->
        {:error, GenUtils.get_tesla_error_message(reason)}
    end
  end

  @spec warn_on_schema_drift(Backend.t(), TypeDetection.event_type()) :: :ok
  defp warn_on_schema_drift(%Backend{config: config} = backend, event_type) do
    table_name = TableSchema.table_name(event_type)
    expected_version = TableSchema.schema_version(event_type)

    metadata = [
      backend_id: backend.id,
      backend_uuid: to_string(backend.token),
      user_id: backend.user_id,
      table_name: table_name
    ]

    GenUtils.get_conn()
    |> Api.Tables.bigquery_tables_get(config.project_id, config.dataset_id, table_name)
    |> case do
      {:ok, %{labels: %{} = labels} = table} ->
        if labels[TableSchema.schema_version_label()] != expected_version do
          expected_columns = Enum.map(TableSchema.fields(event_type), & &1.name)
          actual_columns = Enum.map(table.schema.fields, & &1.name)

          Logger.warning(
            "BigQuery v2 table schema mismatch",
            metadata ++
              [
                missing_columns: expected_columns -- actual_columns,
                extra_columns: actual_columns -- expected_columns
              ]
          )
        end

      {:ok, _table} ->
        Logger.warning("BigQuery v2 table has no schema version label", metadata)

      {:error, reason} ->
        Logger.warning(
          "BigQuery v2 table schema check failed",
          metadata ++ [error_string: GenUtils.get_tesla_error_message(reason)]
        )
    end

    :ok
  end
end
