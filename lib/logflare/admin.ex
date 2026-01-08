defmodule Logflare.Admin do
  @moduledoc false
  require Logger

  alias Logflare.Repo
  alias Logflare.User
  alias Logflare.Teams.Team
  alias Logflare.TeamUsers.TeamUser
  import Ecto.Query

  @doc """
  Shuts down a given node asynchronously in a separate process.

  A delay (default is 5s) occurs just before system stop is triggered.
  """
  @spec shutdown(node(), integer()) :: {:ok, Task.t()}
  def shutdown(node \\ Node.self(), delay \\ 5000) when is_atom(node) do
    task =
      Task.async(fn ->
        Logger.warning("Node shutdown initialized, shutting down in #{delay}ms. node=#{node}")
        Process.sleep(delay)

        :rpc.eval_everywhere([node], System, :stop, [])
      end)

    {:ok, task}
  end

  @spec admin?(String.t() | nil) :: boolean()
  def admin?(email) when is_binary(email) do
    from(u in User,
      left_join: t in Team,
      on: t.user_id == u.id,
      left_join: tu in TeamUser,
      on: tu.team_id == t.id,
      where: (u.email == ^email or tu.email == ^email) and u.admin == true,
      limit: 1
    )
    |> Repo.one()
    |> case do
      nil -> false
      %User{} -> true
    end
  end

  def admin?(_), do: false
end
