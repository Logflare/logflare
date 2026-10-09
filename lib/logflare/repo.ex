defmodule Logflare.Repo do
  use Ecto.Repo,
    otp_app: :logflare,
    adapter: Ecto.Adapters.Postgres

  use Scrivener

  require Logger

  alias Logflare.Repo.ConnectionOptions
  alias Logflare.Repo.Replicas

  @impl true
  def init(_context, config) do
    {role, config} = Keyword.pop(config, :logflare_connection_role, :primary)
    {:ok, ConnectionOptions.prepare(config, role)}
  end

  @uptime_timeout 500

  @doc """
  Postgres uptime in seconds, or `0` when the primary database cannot be reached.

  `queue: false` fails immediately when no connection is free instead of waiting
  out `queue_target`/`queue_interval`, which a per-call `:timeout` does not bound -
  measured at ~12s with the production settings, long past any probe's own timeout.
  A busy pool therefore reads as unreachable here, which is acceptable: the only
  caller needs one successful answer per node and retries until it gets it.
  """
  @spec get_uptime() :: non_neg_integer() | Decimal.t()
  def get_uptime do
    query = "SELECT EXTRACT(epoch FROM (current_timestamp - pg_postmaster_start_time()));"

    __MODULE__.query(query, [], queue: false, timeout: @uptime_timeout)
    |> case do
      {:ok,
       %{
         rows: [
           [uptime]
         ]
       }} ->
        if is_number(uptime) do
          ceil(uptime)
        else
          # for postgres 15 and up
          Decimal.round(uptime, 0, :ceiling)
        end

      {:error, err} ->
        Logger.warning("Could not get Postgres uptime, error: #{inspect(err)}")
        0
    end
  rescue
    err ->
      Logger.warning("Could not get Postgres uptime, error: #{inspect(err)}")
      0
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
