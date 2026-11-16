defmodule Logflare.AlertIssues.Silences do
  @moduledoc """
  Per-tenant silences for alert issues. Silenced alert issues are still created and
  stamped `silenced`, and are hidden from alert issue lists at read time.
  Silences are read from the database on every check; there is no cache yet.
  """

  import Ecto.Query

  alias Logflare.AlertIssues.Silence
  alias Logflare.Repo
  alias Logflare.User

  @spec list(User.t() | pos_integer()) :: [Silence.t()]
  def list(%User{id: user_id}), do: list(user_id)

  def list(user_id) when is_integer(user_id) do
    Silence
    |> where([s], s.user_id == ^user_id)
    |> order_by([s], asc: s.id)
    |> Repo.all()
  end

  @doc """
  Silences whose matchers are all satisfied by `identifiers`.
  """
  @spec list_matching(User.t(), map()) :: [Silence.t()]
  def list_matching(%User{} = user, identifiers) when is_map(identifiers) do
    user
    |> list()
    |> Enum.filter(&Silence.matches?(%{&1 | rule_key: nil}, nil, identifiers))
  end

  @spec create(User.t(), map()) :: {:ok, Silence.t()} | {:error, Ecto.Changeset.t()}
  def create(%User{id: user_id}, attrs) do
    %Silence{user_id: user_id}
    |> Silence.changeset(attrs)
    |> Repo.insert()
  end

  @spec delete(User.t(), integer() | String.t()) :: :ok | {:error, :not_found}
  def delete(%User{} = user, id) when is_binary(id) do
    case Integer.parse(id) do
      {id, ""} -> delete(user, id)
      _ -> {:error, :not_found}
    end
  end

  def delete(%User{id: user_id}, id) when is_integer(id) do
    case Repo.get_by(Silence, id: id, user_id: user_id) do
      nil ->
        {:error, :not_found}

      silence ->
        {:ok, _} = Repo.delete(silence)
        :ok
    end
  end

  @spec silenced?([Silence.t()], String.t() | nil, map() | nil) :: boolean()
  def silenced?(silences, rule_key, identifiers),
    do: Enum.any?(silences, &Silence.matches?(&1, rule_key, identifiers))

  @doc """
  The silence for exactly this rule key and these matchers, if any.
  """
  @spec find_exact([Silence.t()], String.t(), map()) :: Silence.t() | nil
  def find_exact(silences, rule_key, matchers),
    do: Enum.find(silences, &(&1.rule_key == rule_key and &1.matchers == matchers))
end
