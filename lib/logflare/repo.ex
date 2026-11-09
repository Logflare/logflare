defmodule Logflare.Repo do
  use Ecto.Repo,
    otp_app: :logflare,
    adapter: Ecto.Adapters.Postgres

  use Scrivener

  require Logger

  alias Logflare.Repo.ConnectionOptions
  alias Logflare.Repo.Replicas

  @uptime_timeout 1_000

  defoverridable transact: 2

  @impl true
  def transact(fun_or_multi, opts) do
    key = {__MODULE__, :after_commit, get_dynamic_repo()}
    previous = Process.get(key)
    Process.put(key, [])

    {result, callbacks} =
      try do
        result = super(fun_or_multi, opts)
        {result, Process.get(key)}
      after
        if previous == nil, do: Process.delete(key), else: Process.put(key, previous)
      end

    if match?({:ok, _}, result) do
      callbacks |> Enum.reverse() |> Enum.each(&after_commit/1)
    end

    result
  end

  @doc "Runs a callback after the outer transaction commits, or immediately outside one."
  @spec after_commit((-> term())) :: :ok
  def after_commit(callback) when is_function(callback, 0) do
    if in_transaction?() do
      key = {__MODULE__, :after_commit, get_dynamic_repo()}
      Process.put(key, [callback | Process.get(key, [])])
    else
      callback.()
    end

    :ok
  end

  @impl true
  def init(_context, config) do
    {role, config} = Keyword.pop(config, :logflare_connection_role, :primary)
    {:ok, ConnectionOptions.prepare(config, role)}
  end

  @spec get_uptime() :: non_neg_integer() | Decimal.t()
  def get_uptime do
    repo = get_dynamic_repo()
    query = "SELECT EXTRACT(epoch FROM (current_timestamp - pg_postmaster_start_time()));"

    task =
      Task.Supervisor.async_nolink(
        {:via, PartitionSupervisor, {Logflare.TaskSupervisors, self()}},
        fn ->
          put_dynamic_repo(repo)
          __MODULE__.query(query, [], timeout: @uptime_timeout)
        end
      )

    case Task.yield(task, @uptime_timeout) || Task.shutdown(task, :brutal_kill) do
      {:ok, {:ok, %{rows: [[uptime]]}}} ->
        if is_number(uptime) do
          ceil(uptime)
        else
          # for postgres 15 and up
          Decimal.round(uptime, 0, :ceiling)
        end

      {:ok, {:error, err}} ->
        Logger.warning("Could not get Postgres uptime, error: #{Exception.message(err)}")
        0

      {:exit, reason} ->
        Logger.warning("Could not get Postgres uptime, task exited: #{inspect(reason)}")
        0

      nil ->
        Logger.warning("Could not get Postgres uptime within #{@uptime_timeout}ms")
        0
    end
  end

  @doc """
  Runs a function on a randomly selected read replica when replicas are configured.

  The current dynamic repo is preserved for nested calls and transactions. This
  keeps a multi-query read on one pool and prevents replica routing from escaping
  an existing transaction. When no replicas are configured, the primary is used.
  """
  @spec with_replica((-> result)) :: result when result: term()
  def with_replica(fun) when is_function(fun, 0) do
    current_repo = get_dynamic_repo()

    cond do
      in_transaction?() ->
        emit_replica_route(repo_role(current_repo), :transaction)
        fun.()

      current_repo != __MODULE__ ->
        emit_replica_route(repo_role(current_repo), :nested)
        fun.()

      true ->
        {repo, reason} = pick_replica_repo(Application.fetch_env!(:logflare, :read_replicas))
        emit_replica_route(repo_role(repo), reason)
        with_dynamic_repo(repo, fun)
    end
  end

  @doc """
  Reads authoritative state on the primary, restoring the caller's dynamic repo afterwards.
  """
  @spec with_primary((-> result)) :: result when result: term()
  def with_primary(fun) when is_function(fun, 0), do: with_dynamic_repo(__MODULE__, fun)

  @doc """
  Routes a read to a replica when `replica?` is true, or to primary when false.

  The boolean is explicit so callers retain their own consistency defaults.
  A false value forces primary even within an existing replica scope.
  """
  @spec with_read((-> result), boolean()) :: result when result: term()
  def with_read(fun, true), do: with_replica(fun)
  def with_read(fun, false), do: with_primary(fun)

  @doc """
  Applies the given MFA using `with_replica/1`.
  """
  @spec apply_with_replica(module(), atom(), list()) :: term()
  def apply_with_replica(m, f, a) do
    with_replica(fn -> apply(m, f, a) end)
  end

  defp pick_replica_repo([]), do: {__MODULE__, :not_configured}
  defp pick_replica_repo(replicas), do: {replicas |> Enum.random() |> resolve_repo(), :selected}

  defp repo_role(__MODULE__), do: :primary
  defp repo_role(_repo), do: :replica

  defp emit_replica_route(role, reason) do
    :telemetry.execute(
      [:logflare, :repo, :replica_route],
      %{count: 1},
      %{role: role, reason: reason}
    )
  end

  defp resolve_repo(repo) when is_atom(repo), do: repo
  defp resolve_repo({key, _config}), do: Replicas.lookup!(key)

  defp with_dynamic_repo(new_repo, fun) do
    prev_repo = get_dynamic_repo()
    put_dynamic_repo(new_repo)

    try do
      fun.()
    after
      put_dynamic_repo(prev_repo)
    end
  end
end
