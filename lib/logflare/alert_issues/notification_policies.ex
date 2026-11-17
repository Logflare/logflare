defmodule Logflare.AlertIssues.NotificationPolicies do
  @moduledoc """
  Per-user notification policies for alert issues. They are managed through the API
  (Terraform) and are read-only in the UIs.
  """

  import Ecto.Query

  alias Logflare.AlertIssues.NotificationPolicy
  alias Logflare.Repo
  alias Logflare.User

  @spec list(User.t()) :: [NotificationPolicy.t()]
  def list(%User{id: user_id}) do
    NotificationPolicy
    |> where([p], p.user_id == ^user_id)
    |> order_by([p], asc: p.name)
    |> Repo.all()
  end

  @spec list_enabled(pos_integer()) :: [NotificationPolicy.t()]
  def list_enabled(user_id) when is_integer(user_id) do
    NotificationPolicy
    |> where([p], p.user_id == ^user_id and p.enabled)
    |> order_by([p], asc: p.name)
    |> Repo.all()
  end

  @doc """
  Enabled policies, for every user, that send repeats.
  """
  @spec list_repeating() :: [NotificationPolicy.t()]
  def list_repeating do
    NotificationPolicy
    |> where([p], p.enabled and not is_nil(p.repeat_interval_seconds))
    |> where([p], "repeat" in p.events)
    |> order_by([p], asc: p.user_id, asc: p.name)
    |> Repo.all()
  end

  @spec get(User.t(), integer() | String.t()) ::
          {:ok, NotificationPolicy.t()} | {:error, :not_found}
  def get(%User{id: user_id}, id) do
    with {:ok, id} <- parse_id(id),
         %NotificationPolicy{} = policy <-
           Repo.get_by(NotificationPolicy, id: id, user_id: user_id) do
      {:ok, policy}
    else
      _ -> {:error, :not_found}
    end
  end

  @spec create(User.t(), map()) :: {:ok, NotificationPolicy.t()} | {:error, Ecto.Changeset.t()}
  def create(%User{id: user_id}, attrs) do
    %NotificationPolicy{user_id: user_id}
    |> NotificationPolicy.changeset(attrs)
    |> Repo.insert()
  end

  @spec update(User.t(), integer() | String.t(), map()) ::
          {:ok, NotificationPolicy.t()} | {:error, :not_found | Ecto.Changeset.t()}
  def update(%User{} = user, id, attrs) do
    with {:ok, policy} <- get(user, id) do
      policy
      |> NotificationPolicy.changeset(attrs)
      |> Repo.update()
    end
  end

  @spec delete(User.t(), integer() | String.t()) :: :ok | {:error, :not_found}
  def delete(%User{} = user, id) do
    with {:ok, policy} <- get(user, id),
         {:ok, _} <- Repo.delete(policy) do
      :ok
    end
  end

  defp parse_id(id) when is_integer(id), do: {:ok, id}

  defp parse_id(id) when is_binary(id) do
    case Integer.parse(id) do
      {id, ""} -> {:ok, id}
      _ -> :error
    end
  end
end
