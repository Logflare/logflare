defmodule Logflare.Repo do
  use Ecto.Repo,
    otp_app: :logflare,
    adapter: Ecto.Adapters.Postgres

  use Scrivener
  require Logger

  def get_uptime do
    query = "SELECT EXTRACT(epoch FROM (current_timestamp - pg_postmaster_start_time()));"

    __MODULE__.query(query, [])
    |> case do
      {:ok,
       %{
         rows: [
           [uptime]
         ]
       }}
      when is_number(uptime) ->
        ceil(uptime)

      {:error, _err} = err ->
        Logger.warning("Could not get Postgres uptime, error: #{err}")
        0
    end
  end
end
