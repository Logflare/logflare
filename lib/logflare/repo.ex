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

  defp primary_or_replica do
    case fetch_read_replicas!() do
      [_ | _] = replicas -> Enum.random([__MODULE__ | replicas])
      [] -> __MODULE__
    end
  end

  @doc """
  Applies the given MFA using a randomly selected repo (primary or read replica).
  """
  def apply_with_random_repo(m, f, a) do
    case primary_or_replica() do
      __MODULE__ -> apply(m, f, a)
      replica -> Replicas.apply(replica, m, f, a)
    end
  end
end
