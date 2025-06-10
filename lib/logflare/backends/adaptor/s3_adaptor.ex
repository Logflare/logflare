defmodule Logflare.Backends.Adaptor.S3Adaptor do
  @moduledoc """
  Backend adaptor that writes batches of logs to S3.
  """

  use Supervisor
  use TypedStruct
  require Logger

  alias __MODULE__.Pipeline
  alias Ecto.Changeset
  alias Explorer.DataFrame
  alias Logflare.Backends
  alias Logflare.Backends.Backend
  alias Logflare.Backends.SourceRegistry
  alias Logflare.LogEvent
  alias Logflare.Source

  typedstruct do
    field(:config, %{
      s3_bucket: String.t(),
      storage_region: String.t(),
      access_key_id: String.t(),
      secret_access_key: String.t(),
      batch_timeout: pos_integer()
    })

    field(:source, Source.t())
    field(:backend, Backend.t())
    field(:backend_token, String.t())
    field(:source_token, atom())
    field(:pipeline_name, tuple())
  end

  @behaviour Logflare.Backends.Adaptor

  @type source_backend_tuple :: {Source.t(), Backend.t()}
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
  @impl Logflare.Backends.Adaptor
  @spec start_link(source_backend_tuple()) :: Supervisor.on_start()
  def start_link({%Source{}, %Backend{}} = args) do
    Supervisor.start_link(__MODULE__, args, name: adaptor_via(args))
  end

  @doc false
  @impl Logflare.Backends.Adaptor
  def execute_query(_ident, _query), do: {:error, :not_implemented}

  @doc false
  @impl Logflare.Backends.Adaptor
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
  @impl Logflare.Backends.Adaptor
  def validate_config(%Changeset{} = changeset) do
    import Ecto.Changeset

    changeset
    |> validate_required([:s3_bucket, :storage_region, :access_key_id, :secret_access_key])
    |> Changeset.validate_number(:batch_timeout,
      greater_than_or_equal_to: @min_batch_timeout,
      less_than_or_equal_to: @max_batch_timeout
    )
  end

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
    case find_pipeline_pid_in_source_registry(args) do
      {:ok, pid} -> pid
      _ -> nil
    end
  end

  @doc """
  Determines if a particular Broadway pipeline process is alive based on a `Source` and `Backend` pair.
  """
  @spec pipeline_alive?(source_backend_tuple()) :: boolean()
  def pipeline_alive?({%Source{}, %Backend{}} = args) do
    case pipeline_pid(args) do
      nil -> false
      pid -> Process.alive?(pid)
    end
  end

  @doc """
  Generates the S3 path for a new parquet file based on a `Source` struct.
  """
  @spec new_s3_filename(source_backend_tuple()) :: String.t()
  def new_s3_filename({%Source{} = source, %Backend{} = backend}) do
    bucket_name = backend.config.s3_bucket
    source_token = s3_source_token(source)
    now = DateTime.utc_now(:microsecond) |> DateTime.to_unix(:microsecond)

    "s3://#{bucket_name}/#{source_token}/#{now}.parquet"
  end

  @doc """
  Converts a list of `LogEvent` structs to a parquet file and uploads it to S3.
  """
  @spec push_log_events_to_s3(source_backend_tuple(), [LogEvent.t()]) ::
          :ok | {:error, term()}
  def push_log_events_to_s3({%Source{} = source, %Backend{} = backend}, events)
      when is_list(events) do
    s3_file_path = new_s3_filename({source, backend})

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
        access_key_id: backend.config.access_key_id,
        secret_access_key: backend.config.secret_access_key,
        region: backend.config.storage_region
      ]
    )
  end

  @doc false
  @impl Supervisor
  def init({%Source{} = source, %Backend{config: %{} = config} = backend}) do
    pipeline_state = %__MODULE__{
      config: config,
      backend: backend,
      backend_token: if(backend, do: backend.token, else: nil),
      source_token: source.token,
      source: source,
      pipeline_name: pipeline_via({source, backend})
    }

    children = [
      Pipeline.child_spec(pipeline_state)
    ]

    Supervisor.init(children, strategy: :one_for_one)
  end

  @spec find_pipeline_pid_in_source_registry(source_backend_tuple()) ::
          {:ok, pid()} | {:error, term()}
  defp find_pipeline_pid_in_source_registry({%Source{}, %Backend{}} = args) do
    key = pipeline_via(args)

    case Registry.lookup(SourceRegistry, key) do
      [{pid, _meta}] ->
        {:ok, pid}

      _ ->
        {:error, :not_found}
    end
  end

  @spec s3_source_token(Source.t()) :: String.t()
  defp s3_source_token(%Source{token: token}) do
    token
    |> Atom.to_string()
    |> String.replace("-", "_")
  end
end
