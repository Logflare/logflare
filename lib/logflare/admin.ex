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
    Repo.transaction(fn ->
      # Re-verify granter is still an admin from a consistent DB snapshot,
      # closing the TOCTOU window between the controller fetch and this update.
      granter_locked =
        from(u in User, where: u.id == ^granter.id and u.admin == true, lock: "FOR UPDATE")
        |> Repo.one()

      if granter_locked do
        case target |> Ecto.Changeset.change(admin: true) |> Repo.update() do
          {:ok, user} ->
            Logger.info("Admin privilege granted",
              audit: %{
                admin_user_id: granter.id,
                admin_email: granter.email,
                target_user_id: target.id,
                target_user_email: target.email
              }
            )

            user

          {:error, changeset} ->
            Repo.rollback(changeset)
        end
      else
        Repo.rollback(:unauthorized)
      end
    end)
  end

  def grant_admin(%User{}, nil), do: {:error, :not_found}

  def grant_admin(%User{}, %User{}), do: {:error, :unauthorized}

  @spec revoke_admin(User.t() | nil, User.t() | nil) ::
          {:ok, User.t()}
          | {:error, :not_found | :last_admin | :self_revocation | :unauthorized}
          | {:error, Ecto.Changeset.t()}
  def revoke_admin(nil, _), do: {:error, :not_found}

  def revoke_admin(%User{admin: true} = granter, %User{} = target) do
    cond do
      granter.id == target.id ->
        {:error, :self_revocation}

      true ->
        Repo.transaction(fn ->
          # Lock all admin rows so concurrent revocations are serialised and
          # we can re-verify the granter's admin status from a consistent snapshot.
          admins = from(u in User, where: u.admin == true, lock: "FOR UPDATE") |> Repo.all()

          cond do
            not Enum.any?(admins, &(&1.id == granter.id)) ->
              Repo.rollback(:unauthorized)

            length(admins) <= 1 ->
              Repo.rollback(:last_admin)

            true ->
              case target |> Ecto.Changeset.change(admin: false) |> Repo.update() do
                {:ok, user} ->
                  Logger.info("Admin privilege revoked",
                    audit: %{
                      admin_user_id: granter.id,
                      admin_email: granter.email,
                      target_user_id: target.id,
                      target_user_email: target.email
                    }
                  )

                  user

                {:error, changeset} ->
                  Repo.rollback(changeset)
              end
          end
        end)
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
