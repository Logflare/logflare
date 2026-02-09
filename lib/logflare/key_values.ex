defmodule Logflare.KeyValues do
  @moduledoc false

  import Ecto.Query

  alias Logflare.KeyValues.KeyValue
  alias Logflare.Repo

  @list_limit 500

  @spec list_key_values(keyword()) :: [KeyValue.t()]
  def list_key_values(kw) do
    user_id = Keyword.fetch!(kw, :user_id)
    key_filter = Keyword.get(kw, :key)
    value_filter = Keyword.get(kw, :value)

    KeyValue
    |> where(user_id: ^user_id)
    |> maybe_filter_by(:key, key_filter)
    |> maybe_filter_by(:value, value_filter)
    |> order_by(asc: :key)
    |> limit(@list_limit)
    |> Repo.all()
  end

  defp maybe_filter_by(query, _field, nil), do: query
  defp maybe_filter_by(query, :key, key), do: where(query, key: ^key)
  defp maybe_filter_by(query, :value, value), do: where(query, value: ^value)

  @spec get_key_value(integer()) :: KeyValue.t() | nil
  def get_key_value(id), do: Repo.get(KeyValue, id)

  @spec fetch_key_value_by(keyword()) :: {:ok, KeyValue.t()} | {:error, :not_found}
  def fetch_key_value_by(kw) do
    case Repo.get_by(KeyValue, kw) do
      nil -> {:error, :not_found}
      kv -> {:ok, kv}
    end
  end

  @spec create_key_value(map()) :: {:ok, KeyValue.t()} | {:error, Ecto.Changeset.t()}
  def create_key_value(attrs) do
    %KeyValue{}
    |> KeyValue.changeset(attrs)
    |> Repo.insert()
  end

  @spec update_key_value(KeyValue.t(), map()) ::
          {:ok, KeyValue.t()} | {:error, Ecto.Changeset.t()}
  def update_key_value(%KeyValue{} = kv, attrs) do
    kv
    |> KeyValue.changeset(attrs)
    |> Repo.update()
  end

  @spec delete_key_value(KeyValue.t()) :: {:ok, KeyValue.t()} | {:error, Ecto.Changeset.t()}
  def delete_key_value(%KeyValue{} = kv) do
    Repo.delete(kv)
  end

  @spec count_key_values(integer()) :: non_neg_integer()
  def count_key_values(user_id) do
    KeyValue
    |> where(user_id: ^user_id)
    |> Repo.aggregate(:count)
  end

  @spec lookup(integer(), String.t()) :: String.t() | nil
  def lookup(user_id, key) do
    KeyValue
    |> where(user_id: ^user_id, key: ^key)
    |> select([kv], kv.value)
    |> Repo.one()
  end

  @spec bulk_upsert_key_values(integer(), [map()]) :: {non_neg_integer(), nil | [KeyValue.t()]}
  def bulk_upsert_key_values(user_id, entries) do
    rows =
      Enum.map(entries, fn entry ->
        %{
          user_id: user_id,
          key: entry[:key] || entry["key"],
          value: entry[:value] || entry["value"]
        }
      end)

    Repo.insert_all(KeyValue, rows,
      on_conflict: {:replace, [:value]},
      conflict_target: [:user_id, :key]
    )
  end

  @spec bulk_delete_by_keys(integer(), [String.t()]) :: {non_neg_integer(), nil}
  def bulk_delete_by_keys(user_id, keys) when is_list(keys) do
    KeyValue
    |> where(user_id: ^user_id)
    |> where([kv], kv.key in ^keys)
    |> Repo.delete_all()
  end

  @spec bulk_delete_by_values(integer(), [String.t()]) :: {non_neg_integer(), nil}
  def bulk_delete_by_values(user_id, values) when is_list(values) do
    KeyValue
    |> where(user_id: ^user_id)
    |> where([kv], kv.value in ^values)
    |> Repo.delete_all()
  end
end
