defmodule Logflare.Backends.Adaptor.BigQueryAdaptor do
  @moduledoc false

  alias Logflare.Backends
  alias Logflare.Backends.DynamicPipeline
  alias Logflare.Backends.Backend
  alias Logflare.Backends.IngestEventQueue
  alias Logflare.Source.BigQuery.Pipeline
  alias Logflare.Source.BigQuery.Schema
  alias Logflare.Source.BigQuery.Pipeline
  alias Logflare.Users
  alias Logflare.Sources
  alias Logflare.Billing
  alias Logflare.Backends
  alias Logflare.Google.BigQuery
  alias Logflare.SourceSchemas
  alias Logflare.Backends.Adaptor.BigQueryAdaptor.GoogleApiClient
  require Record

  Record.defrecord(
    :message,
    Record.extract(:message, from: "deps/serde_arrow/src/serde_arrow_ipc_message.hrl")
  )

  Record.defrecord(
    :record_batch,
    Record.extract(:record_batch, from: "deps/serde_arrow/src/serde_arrow_ipc_record_batch.hrl")
  )

  Record.defrecord(
    :schema,
    Record.extract(:schema, from: "deps/serde_arrow/src/serde_arrow_ipc_schema.hrl")
  )

  Record.defrecord(
    :field,
    Record.extract(:field, from: "deps/serde_arrow/src//serde_arrow_ipc_field.hrl")
  )

  use Supervisor
  require Logger

  @behaviour Logflare.Backends.Adaptor

  @impl Logflare.Backends.Adaptor
  def start_link({source, backend} = source_backend) do
    Supervisor.start_link(__MODULE__, source_backend,
      name: Backends.via_source(source, __MODULE__, backend.id)
    )
  end

  @impl true
  def init({source, backend}) do
    backend = backend || %Backend{}

    user = Users.Cache.get(source.user_id)
    plan = Billing.Cache.get_plan_by_user(user)

    project_id = backend.config.project_id
    dataset_id = backend.config.dataset_id
    # TODO: remove source_id metadata to reduce confusion
    Logger.metadata(source_id: source.token, source_token: source.token)

    children = [
      {
        DynamicPipeline,
        # soft limit before a new pipeline is created
        name: Backends.via_source(source, Pipeline, backend.id),
        pipeline: Pipeline,
        pipeline_args: [
          source: source,
          backend: backend,
          bigquery_project_id: project_id,
          bigquery_dataset_id: dataset_id
        ],
        min_pipelines: 0,
        max_pipelines: System.schedulers_online(),
        initial_count: 1,
        resolve_interval: 2_500,
        resolve_count: fn state ->
          source = Sources.refresh_source_metrics_for_ingest(source)

          lens = IngestEventQueue.list_pending_counts({source.id, backend.id})

          handle_resolve_count(state, lens, source.metrics.avg)
        end
      },
      {Schema,
       [
         plan: plan,
         source: source,
         bigquery_project_id: project_id,
         bigquery_dataset_id: dataset_id,
         name: Backends.via_source(source, Schema, backend.id)
       ]}
    ]

    Supervisor.init(children, strategy: :one_for_one, max_restarts: 10)
  end

  def insert_log_events_via_storage_write_api(log_events, opts) do
    # convert log events to table rows
    opts =
      Keyword.validate!(opts, [:project_id, :dataset_id, :source_token, :source_id, :source_token])

    # get table id
    table_id = format_table_name(opts[:source_token])
    # get source schema
    source_schema = SourceSchemas.Cache.get_source_schema_by(source_id: opts[:source_id])

    # convert to arrow schema
    arrow_schema =
      :serde_arrow_ipc_schema.from_erlang([
        :serde_arrow_ipc_field.from_erlang(
          :large_binary,
          "event_message"
        ),
        :serde_arrow_ipc_field.from_erlang(
          {:int, %{bit_width: 64, is_signed: true}},
          "timestamp"
        )
      ])

    # convert log events to proto rows
    event_messages =
      log_events
      |> Enum.map(fn log_event ->
        log_event.body["event_message"]
      end)

    timestamps =
      log_events
      |> Enum.map(fn log_event ->
        log_event.body["timestamp"]
      end)

    columns =
      [
        :serde_arrow_array.from_erlang(:variable_binary, event_messages, {:bin, nil}),
        :serde_arrow_array.from_erlang(:fixed_primitive, timestamps, {:s, 64})
      ]
      |> dbg()

    body =
      :serde_arrow_ipc_message.body_from_erlang(columns)
      |> dbg()

    record_batch =
      :serde_arrow_ipc_record_batch.from_erlang(columns)
      # |> record_batch()
      |> dbg()

    record_batch_msg =
      :serde_arrow_ipc_message.from_erlang(record_batch, body)
      # |> message()
      |> dbg()

    record_batch_emf =
      :serde_arrow_ipc_message.to_ipc(record_batch_msg)
      |> dbg()

    schema =
      :serde_arrow_ipc_schema.from_erlang([
        :serde_arrow_ipc_field.from_erlang(
          :large_binary,
          ~c"event_message"
        ),
        :serde_arrow_ipc_field.from_erlang(
          {:int, %{bit_width: 64, is_signed: true}},
          ~c"timestamp"
        )
      ])

    schema_msg = :serde_arrow_ipc_message.from_erlang(schema)

    schema_emf = :serde_arrow_ipc_message.to_ipc(schema_msg)
    # append rows
    GoogleApiClient.append_rows(
      {:arrow, record_batch_emf, schema_emf},
      opts[:project_id],
      opts[:dataset_id],
      table_id
    )
    |> dbg()
  end

  @spec format_table_name(atom) :: String.t()
  def format_table_name(source_token) when is_atom(source_token) do
    Atom.to_string(source_token)
    |> String.replace("-", "_")
  end

  defp source_schema_to_proto_schema(source_schema) do
    source_schema.bigquery_schema
  end

  @doc """
  Pipeline count resolution logic, separate to a different functino for easier testing.

  """
  def handle_resolve_count(state, lens, avg_rate) do
    startup_size =
      Enum.find_value(lens, 0, fn
        {{_sid, _bid, nil}, val} -> val
        _ -> false
      end)

    lens_no_startup =
      Enum.filter(lens, fn
        {{_sid, _bid, nil}, _val} -> false
        _ -> true
      end)

    lens_no_startup_values = Enum.map(lens_no_startup, fn {_, v} -> v end)
    len = Enum.map(lens, fn {_, v} -> v end) |> Enum.sum()

    last_decr = state.last_count_decrease || NaiveDateTime.utc_now()
    sec_since_last_decr = NaiveDateTime.diff(NaiveDateTime.utc_now(), last_decr)

    any_above_threshold? = Enum.any?(lens_no_startup_values, &(&1 >= 500))

    cond do
      # max out pipelines, overflow risk
      startup_size > 0 ->
        state.pipeline_count + ceil(startup_size / 500)

      any_above_threshold? and len > 0 ->
        state.pipeline_count + ceil(len / 500)

      # gradual decrease
      Enum.all?(lens_no_startup_values, &(&1 < 50)) and len < 500 and state.pipeline_count > 1 and
          (sec_since_last_decr > 60 or state.last_count_decrease == nil) ->
        state.pipeline_count - 1

      len == 0 and avg_rate == 0 and
        state.pipeline_count == 1 and
          (sec_since_last_decr > 60 * 5 or state.last_count_decrease == nil) ->
        # scale to zero only if no items for > 5m
        0

      true ->
        state.pipeline_count
    end
  end

  @impl Logflare.Backends.Adaptor
  def execute_query(_id, _query),
    do: {:error, :not_implemented}

  @impl Logflare.Backends.Adaptor
  def cast_config(params) do
    {%{}, %{project_id: :string, dataset_id: :string}}
    |> Ecto.Changeset.cast(params, [:project_id, :dataset_id])
  end

  @impl Logflare.Backends.Adaptor
  def validate_config(changeset),
    do: changeset
end
