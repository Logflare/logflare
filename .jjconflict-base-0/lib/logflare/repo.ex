defmodule Logflare.Repo do
  use Ecto.Repo,
    otp_app: :logflare,
    adapter: Ecto.Adapters.Postgres

  use Scrivener
  require Logger
  alias Logflare.Repo.Replicas

  def get_uptime do
    query = "SELECT EXTRACT(epoch FROM (current_timestamp - pg_postmaster_start_time()));"

    __MODULE__.query(query, [])
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
        Logger.warning("Could not get Postgres uptime, error: #{err}")
        0
    end
  end

  @doc """
  Applies the given MFA using a randomly selected repo (primary or read replica).
  """
  @spec apply_with_random_repo(module(), atom(), list()) :: term()
  def apply_with_random_repo(m, f, a) do
    [__MODULE__ | Application.fetch_env!(:logflare, :read_replicas)]
    |> Enum.random()
    |> resolve_repo()
    |> with_dynamic_repo(fn -> apply(m, f, a) end)
  end

  @doc """
  Applies the given MFA on a randomly selected read replica when replicas are
  configured. Always uses a replica when any are set; falls back to the primary
  repo only when no replicas are configured.
  """
  @spec apply_with_replica(module(), atom(), list()) :: term()
  def apply_with_replica(m, f, a) do
    :logflare
    |> Application.fetch_env!(:read_replicas)
    |> pick_replica_repo()
    |> with_dynamic_repo(fn -> apply(m, f, a) end)
  end

  defp pick_replica_repo([]), do: __MODULE__
  defp pick_replica_repo(replicas), do: replicas |> Enum.random() |> resolve_repo()

  defp resolve_repo(repo) when is_atom(repo), do: repo
  defp resolve_repo(hostname) when is_binary(hostname), do: Replicas.lookup!(hostname)

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
