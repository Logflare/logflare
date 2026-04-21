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
  def apply_with_random_repo(m, f, a) do
    choices = [__MODULE__ | Application.fetch_env!(:logflare, :read_replicas)]

    new_repo =
      with replica when is_binary(replica) <- Enum.random(choices) do
        Replicas.lookup!(replica)
      end

    prev_repo = Logflare.Repo.get_dynamic_repo()
    Logflare.Repo.put_dynamic_repo(new_repo)

    try do
      apply(m, f, a)
    after
      Logflare.Repo.put_dynamic_repo(prev_repo)
    end
  end
end
