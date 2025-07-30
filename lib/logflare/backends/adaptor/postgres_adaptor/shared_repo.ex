defmodule Logflare.Backends.Adaptor.PostgresAdaptor.SharedRepo do
  @moduledoc """
  Shared repository for all connections in Postgres Adaptor
  """

  use Ecto.Repo,
    otp_app: :logflare,
    adapter: Ecto.Adapters.Postgres

  require Logger

  alias Logflare.Source
  alias Logflare.Backends.Backend

  alias Logflare.Backends.Adaptor.PostgresAdaptor.Supervisor
  alias Logflare.Backends.Adaptor.PostgresAdaptor.Repo.Migrations

  @doc """
  Start new repository for given backend definition
  """
  @spec start(Backend.t()) :: {:ok, pid()} | {:error, term()}
  def start(%Backend{} = backend) do
    config = backend.config
    default_pool_size = Application.fetch_env!(:logflare, :postgres_backend_adapter)[:pool_size]
    schema = Map.get(config, :schema)

    url = Map.get(config, :url)

    opts = [
      name: Supervisor.via(backend),
      pool_size: Map.get(config, :pool_size, default_pool_size),
      # Wait until repo is fully up and running
      sync_connect: true,
      after_connect: {__MODULE__, :__after_connect__, [schema]}
    ]

    opts =
      if url do
        Keyword.put(opts, :url, url)
      else
        fields =
          Map.take(config, [:username, :password, :hostname, :database, :port, :ssl])
          |> Map.to_list()

        opts ++ fields
      end

    if Logflare.SingleTenant.single_tenant?() do
      opts = opts ++ [socket_options: config[:socket_options]]
    else
      opts
    end

    with {:error, {:already_started, pid}} <- Supervisor.start_child({__MODULE__, opts}) do
      {:ok, pid}
    end
  end

  @doc """
  Set repository for current process
  """
  @spec set_repo(Backend.t()) :: {:ok, pid() | atom()} | :error
  def set_repo(%Backend{} = backend) do
    with {:ok, pid} <- start(backend) do
      {:ok, put_dynamic_repo(pid)}
    end
  end

  @doc """
  Run `func` with repository set to one for given backend

  > #### Warning {: .warning}
  >
  > Repository will be set only for current process and will not cross process
  > boundaries.
  """
  @spec with_repo(Backend.t(), func :: (-> a)) :: a when a: term()
  def with_repo(%Backend{} = backend, func) do
    {:ok, old_repo} = set_repo(backend)

    try do
      func.()
    rescue
      _e in DBConnection.ConnectionError ->
        {:error, :cannot_connect}
    catch
      :exit, {:killed, _} -> {:error, :cannot_connect}
      :exit, {:noproc, _} -> {:error, :cannot_connect}
    after
      put_dynamic_repo(old_repo)
    end
  end

  @doc """
  Run migrations for currently selected repository

  > #### Warning {: .warning}
  >
  > This is desctructive operation. Be cautious when you call it.
  """
  @spec migrate!(Source.t()) :: :ok | {:error, term()}
  def migrate!(%Source{} = source) do
    Migrations.migrate(source)
  rescue
    DBConnection.ConnectionError ->
      {:error, :cannot_connect}

    e in Postgrex.Error ->
      Logger.error(%{error: e})

      {:error, {:failed_migration, e}}
  catch
    :exit, {:killed, _} -> {:error, :cannot_connect}
    :exit, {:noproc, _} -> {:error, :cannot_connect}
  end

  @spec down!(Source.t()) :: :ok | {:error, term()}
  def down!(%Source{} = source) do
    Migrations.down(source)
  end

  @doc false
  def __after_connect__(_, nil), do: :ok

  def __after_connect__(conn, schema) do
    Postgrex.query!(conn, "CREATE SCHEMA IF NOT EXISTS #{schema}", [])
    Postgrex.query!(conn, "SET search_path=#{schema}", [])

    :ok
  end
end
