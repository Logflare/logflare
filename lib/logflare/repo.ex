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

  defp fetch_read_replicas! do
    Application.fetch_env!(:logflare, :read_replicas)
  end

  defp random_read_replica do
    case fetch_read_replicas!() do
      [_ | _] = replicas -> Enum.random(replicas)
      [] -> nil
    end
  end

  @doc """
  Returns the current database role based on the dynamic repo state.
  """
  def current_role do
    case get_dynamic_repo() do
      __MODULE__ -> "primary"
      pid when is_pid(pid) -> "replica"
      _ -> "unknown"
    end
  end

  @doc """
  Applies the given MFA using a randomly selected read replica connection pool.
  Uses the primary database if no read replicas are configured.
  """
  def apply_with_read_replica(m, f, a) do
    if replica = random_read_replica() do
      Replicas.apply(replica, m, f, a)
    else
      apply(m, f, a)
    end
  end
end
