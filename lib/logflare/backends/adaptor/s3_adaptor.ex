defmodule Logflare.Backends.Adaptor.S3Adaptor do
  @moduledoc """
  Backend adaptor that writes batches of logs to S3.
  """

  import Logflare.Utils.Guards

  use Supervisor
  require Logger

  alias __MODULE__.Pipeline
  alias Ecto.Changeset
  alias Explorer.DataFrame
  alias Logflare.Backends
  alias Logflare.Backends.Adaptor
  alias Logflare.Backends.Backend
  alias Logflare.LogEvent
  alias Logflare.Source
  alias Logflare.Sources

  @behaviour Adaptor

  @type source_backend_tuple :: {Source.t(), Backend.t()}
  @type source_id_backend_id_tuple :: {source_id :: pos_integer(), backend_id :: pos_integer()}
  @type via_tuple :: {:via, Registry, {module(), {pos_integer(), {module(), pos_integer()}}}}

  @min_batch_timeout 1_000
  @max_batch_timeout 5_000

  @doc false
  def child_spec(arg) do
    %{
      id: __MODULE__,
      start: {__MODULE__, :start_link, [arg]}
    }
  end

  @doc false
  @impl Adaptor
  @spec start_link(source_backend_tuple()) :: Supervisor.on_start()
  def start_link({%Source{}, %Backend{}} = args) do
    Supervisor.start_link(__MODULE__, args, name: adaptor_via(args))
  end

  @doc false
  @impl Adaptor
  def execute_query(_ident, _query), do: {:error, :not_implemented}

  @doc false
  @impl Adaptor
  def cast_config(%{} = params) do
    {%{},
     %{
       s3_bucket: :string,
       storage_region: :string,
       access_key_id: :string,
       secret_access_key: :string,
       batch_timeout: :integer
     }}
    |> Changeset.cast(params, [
      :s3_bucket,
      :storage_region,
      :access_key_id,
      :secret_access_key,
      :batch_timeout
    ])
  end

  @doc false
  @impl Adaptor
  def validate_config(%Changeset{} = changeset) do
    import Ecto.Changeset

    changeset
    |> validate_required([:s3_bucket, :storage_region, :access_key_id, :secret_access_key])
    |> Changeset.validate_number(:batch_timeout,
      greater_than_or_equal_to: @min_batch_timeout,
      less_than_or_equal_to: @max_batch_timeout
    )
  end

  @impl Adaptor
  def transform_config(config) do
    config
  end

  @impl Adaptor
  def supports_default_ingest?, do: true

  @doc """
  Generates a via tuple based on a `Source` and `Backend` pair for this adaptor instance.

  See `Backends.via_source/3` for more details.
  """
  @spec adaptor_via(source_backend_tuple()) :: via_tuple()
  def adaptor_via({%Source{} = source, %Backend{} = backend}) do
    Backends.via_source(source, __MODULE__, backend)
  end

  @doc """
  Generates a unique Broadway pipeline via tuple based on a `Source` and `Backend` pair.

  See `Backends.via_source/3` for more details.
  """
  @spec pipeline_via(source_backend_tuple()) :: via_tuple()
  def pipeline_via({%Source{} = source, %Backend{} = backend}) do
    Backends.via_source(source, Pipeline, backend)
  end

  @doc """
  Returns the pid for the Broadway pipeline related to a specific `Source` and `Backend` pair.

  If the process is not located in the registry or does not exist, this will return `nil`.
  """
  @spec pipeline_pid(source_backend_tuple()) :: pid() | nil
  def pipeline_pid({%Source{}, %Backend{}} = args) do
    args
    |> pipeline_via()
    |> GenServer.whereis()
  end

  @doc """
  Determines if a particular Broadway pipeline process is alive based on a `Source` and `Backend` pair.
  """
  @spec pipeline_alive?(source_backend_tuple()) :: boolean()
  def pipeline_alive?(args), do: !!pipeline_pid(args)

  @doc """
  Generates the S3 path for a new parquet file based on a `Source` and bucket name.
  """
  @spec new_s3_filename(Source.t(), bucket_name :: String.t()) :: String.t()
  def new_s3_filename(%Source{} = source, bucket_name)
      when is_non_empty_binary(bucket_name) do
    source_token = s3_source_token(source)
    now = DateTime.utc_now(:microsecond) |> DateTime.to_unix(:microsecond)

    "s3://#{bucket_name}/#{source_token}/#{now}.parquet"
  end

  @doc """
  Converts a list of `LogEvent` structs to a parquet file and uploads it to S3.
  """
  @spec push_log_events_to_s3(source_id_backend_id_tuple(), [LogEvent.t()]) ::
          :ok | {:error, any()}
  def push_log_events_to_s3({source_id, backend_id}, events)
      when is_pos_integer(source_id) and is_pos_integer(backend_id) and is_list(events) do
    with %Source{} = source <- Sources.Cache.get_by_id(source_id),
         %Backend{} = backend <- Backends.Cache.get_backend(backend_id),
         config <- Adaptor.get_backend_config(backend),
         s3_file_path <- new_s3_filename(source, config.s3_bucket) do
      event_rows =
        Enum.map(events, fn %LogEvent{} = log_event ->
          flattened_body =
            log_event.body
            |> Map.drop(["id", "event_message", "timestamp"])
            |> Iteraptor.to_flatmap()

          %{
            id: log_event.body["id"],
            event_message: log_event.body["event_message"],
            body: Jason.encode!(flattened_body),
            timestamp: DateTime.from_unix!(log_event.body["timestamp"], :microsecond)
          }
        end)

      event_rows
      |> DataFrame.new(
        dtypes: [
          {:id, :string},
          {:event_message, :string},
          {:body, :string},
          {:timestamp, {:datetime, :microsecond, "Etc/UTC"}}
        ]
      )
      |> DataFrame.to_parquet(s3_file_path,
        streaming: true,
        config: [
          access_key_id: config.access_key_id,
          secret_access_key: config.secret_access_key,
          region: config.storage_region
        ]
      )
    end
  end

  @doc false
  @impl Supervisor
  def init({%Source{} = source, %Backend{} = backend}) do
    config = Adaptor.get_backend_config(backend)

    pipeline_args = [
      pipeline_name: pipeline_via({source, backend}),
      source_id: source.id,
      backend_id: backend.id,
      batch_timeout: config.batch_timeout
    ]

    children = [
      Pipeline.child_spec(pipeline_args)
    ]

    Supervisor.init(children, strategy: :one_for_one)
  end

  @spec s3_source_token(Source.t()) :: String.t()
  defp s3_source_token(%Source{token: token}) do
    token
    |> Atom.to_string()
    |> String.replace("-", "_")
  end
end
