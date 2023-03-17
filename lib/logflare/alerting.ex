defmodule Logflare.Alerting do
  @moduledoc """
  The Alerting context.
  """

  import Ecto.Query, warn: false
  alias Logflare.Repo

  alias Logflare.Alerting.AlertQuery
  alias Logflare.User

  @doc """
  Returns the list of alert_queries.

  ## Examples

      iex> list_alert_queries()
      [%AlertQuery{}, ...]

  """
  def list_alert_queries(%User{id: user_id}) do
    from(q in AlertQuery, where: q.user_id == ^user_id)
    |> Repo.all()
  end

  @doc """
  Gets a single alert_query.

  Raises `Ecto.NoResultsError` if the Alert query does not exist.

  ## Examples

      iex> get_alert_query!(123)
      %AlertQuery{}

      iex> get_alert_query!(456)
      ** (Ecto.NoResultsError)

  """
  def get_alert_query!(id), do: Repo.get!(AlertQuery, id)

  @doc """
  Creates a alert_query.

  ## Examples

      iex> create_alert_query(%{field: value})
      {:ok, %AlertQuery{}}

      iex> create_alert_query(%{field: bad_value})
      {:error, %Ecto.Changeset{}}

  """
  def create_alert_query(%User{} = user, attrs \\ %{}) do
    user
    |> Ecto.build_assoc(:alert_queries)
    |> Repo.preload(:user)
    |> AlertQuery.changeset(attrs)
    |> Repo.insert()
  end

  @doc """
  Updates a alert_query.

  ## Examples

      iex> update_alert_query(alert_query, %{field: new_value})
      {:ok, %AlertQuery{}}

      iex> update_alert_query(alert_query, %{field: bad_value})
      {:error, %Ecto.Changeset{}}

  """
  def update_alert_query(%AlertQuery{} = alert_query, attrs) do
    alert_query
    |> Repo.preload(:user)
    |> AlertQuery.changeset(attrs)
    |> Repo.update()
  end

  @doc """
  Deletes a alert_query.

  ## Examples

      iex> delete_alert_query(alert_query)
      {:ok, %AlertQuery{}}

      iex> delete_alert_query(alert_query)
      {:error, %Ecto.Changeset{}}

  """
  def delete_alert_query(%AlertQuery{} = alert_query) do
    Repo.delete(alert_query)
  end

  @doc """
  Returns an `%Ecto.Changeset{}` for tracking alert_query changes.

  ## Examples

      iex> change_alert_query(alert_query)
      %Ecto.Changeset{data: %AlertQuery{}}

  """
  def change_alert_query(%AlertQuery{} = alert_query, attrs \\ %{}) do
    AlertQuery.changeset(alert_query, attrs)
  end
end
