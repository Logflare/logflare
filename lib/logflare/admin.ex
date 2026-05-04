defmodule Logflare.Admin do
  @moduledoc false
  require Logger

  alias Logflare.Repo
  alias Logflare.User
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

  @spec grant_admin(User.t() | nil, User.t() | nil) ::
          {:ok, User.t()} | {:error, :not_found | :unauthorized} | {:error, Ecto.Changeset.t()}
  def grant_admin(nil, _), do: {:error, :not_found}

  def grant_admin(%User{admin: true} = granter, %User{} = target) do
    Logger.info("Admin privilege granted",
      audit: [
        admin_user_id: granter.id,
        admin_email: granter.email,
        target_user_id: target.id,
        target_user_email: target.email
      ]
    )

    target
    |> Ecto.Changeset.change(admin: true)
    |> Repo.update()
  end

  def grant_admin(%User{}, nil), do: {:error, :not_found}

  def grant_admin(%User{}, %User{}), do: {:error, :unauthorized}

  @spec revoke_admin(User.t() | nil, User.t() | nil) ::
          {:ok, User.t()}
          | {:error, :not_found | :self_revocation | :unauthorized}
          | {:error, Ecto.Changeset.t()}
  def revoke_admin(nil, _), do: {:error, :not_found}

  def revoke_admin(%User{admin: true} = granter, %User{} = target) do
    if granter.id == target.id do
      {:error, :self_revocation}
    else
      Logger.info("Admin privilege revoked",
        audit: [
          admin_user_id: granter.id,
          admin_email: granter.email,
          target_user_id: target.id,
          target_user_email: target.email
        ]
      )

      target
      |> Ecto.Changeset.change(admin: false)
      |> Repo.update()
    end
  end

  def revoke_admin(%User{}, nil), do: {:error, :not_found}

  def revoke_admin(%User{}, %User{}), do: {:error, :unauthorized}

  @spec admin?(String.t() | nil) :: boolean()
  def admin?(email) when is_binary(email) do
    from(u in User,
      where: u.email == ^email and u.admin == true,
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
