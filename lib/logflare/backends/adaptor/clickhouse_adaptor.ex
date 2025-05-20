defmodule Logflare.Backends.Adaptor.ClickhouseAdaptor do
  @moduledoc """
  ClickHouse backend adaptor that relies on the `:ch` library.
  """

  use GenServer
  use TypedStruct
  require Logger

  alias Ecto.Changeset
  alias Logflare.Backends
  alias Logflare.Backends.Backend
  alias __MODULE__.Client
  alias __MODULE__.Pipeline
  alias __MODULE__.Supervisor

  typedstruct do
    field(:config, %{
      url: String.t(),
      username: String.t(),
      password: String.t(),
      database: String.t(),
      table: String.t(),
      port: non_neg_integer(),
      pool_size: non_neg_integer()
    })

    field(:source, Source.t())
    field(:backend, Backend.t())
    field(:backend_token, String.t())
    field(:source_token, atom())
    field(:pipeline_name, tuple())
  end

  @behaviour Logflare.Backends.Adaptor

  def child_spec(arg) do
    %{
      id: __MODULE__,
      start: {__MODULE__, :start_link, [arg]}
    }
  end

  @impl Logflare.Backends.Adaptor
  def start_link({source, backend}) do
    GenServer.start_link(__MODULE__, {source, backend},
      name: Backends.via_source(source, __MODULE__, backend.id)
    )
  end

  @impl Logflare.Backends.Adaptor
  def execute_query(_ident, _query), do: {:error, :not_implemented}

  @impl Logflare.Backends.Adaptor
  def cast_config(params) do
    {%{},
     %{
       url: :string,
       username: :string,
       password: :string,
       database: :string,
       table: :string,
       port: :integer,
       pool_size: :integer
     }}
    |> Changeset.cast(params, [
      :url,
      :username,
      :password,
      :database,
      :table,
      :port,
      :pool_size
    ])
  end

  @impl Logflare.Backends.Adaptor
  def validate_config(changeset) do
    import Ecto.Changeset

    changeset
    |> validate_required([:url, :database, :table, :port])
    |> Changeset.validate_format(:url, ~r/https?\:\/\/.+/)
    |> validate_user_pass()
  end

  # delegated functions
  defdelegate table_name(source), to: Client

  defdelegate execute_ch_query(backend_or_conn, statement, params \\ [], opts \\ []),
    to: Client

  defdelegate insert_log_event(source, backend, log_event), to: Client
  defdelegate insert_log_events(source, backend, log_events), to: Client
  defdelegate find_or_create_ch_connection(backend), to: Supervisor

  @impl GenServer
  def init({source, backend}) do
    # establish connection
    case find_or_create_ch_connection(backend) do
      {:ok, _pid} ->
        :ok

      res ->
        Logger.error("Failed to create ClickHouse connection: #{inspect(res)} ",
          source_token: source.token,
          backend_id: backend.id
        )
    end

    # maybe handle table creation or permission checks here?

    state = %__MODULE__{
      config: backend.config,
      backend: backend,
      backend_token: if(backend, do: backend.token, else: nil),
      source_token: source.token,
      source: source,
      pipeline_name: Backends.via_source(source, Pipeline, backend.id)
    }

    {:ok, _pipeline_pid} = Pipeline.start_link(state)
    {:ok, state}
  end

  defp validate_user_pass(changeset) do
    user = Changeset.get_field(changeset, :username)
    pass = Changeset.get_field(changeset, :password)
    user_pass = [user, pass]

    if user_pass != [nil, nil] and Enum.any?(user_pass, &is_nil/1) do
      msg = "Both username and password must be provided for auth"

      changeset
      |> Changeset.add_error(:username, msg)
      |> Changeset.add_error(:password, msg)
    else
      changeset
    end
  end
end
