defmodule Logflare.KeyValues do
  @moduledoc false

  import Ecto.Query

  alias Logflare.KeyValues.KeyValue
  alias Logflare.KeyValues.KeyValueUsage
  alias Logflare.Repo

  @list_limit 500
  @usage_retention_days 30

  @type prepared_accessor ::
          {raw_path :: String.t(), prepared :: [String.t()] | Warpath.Expression.t() | :invalid}

  @spec list_key_values_query(keyword()) :: Ecto.Query.t()
  def list_key_values_query(kw) do
    user_id = Keyword.fetch!(kw, :user_id)

    KeyValue
    |> where(user_id: ^user_id)
    |> maybe_filter_by(:key, Keyword.get(kw, :key))
    |> order_by(asc: :key)
  end

  @spec list_key_values(keyword()) :: [KeyValue.t()]
  def list_key_values(kw) do
    user_id = Keyword.fetch!(kw, :user_id)
    key_filter = Keyword.get(kw, :key)

    KeyValue
    |> where(user_id: ^user_id)
    |> maybe_filter_by(:key, key_filter)
    |> order_by(asc: :key)
    |> limit(@list_limit)
    |> Repo.all()
  end

  defp maybe_filter_by(query, _field, nil), do: query
  defp maybe_filter_by(query, :key, key), do: where(query, key: ^key)

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

  @spec lookup(integer(), String.t()) :: map() | nil
  def lookup(user_id, key) do
    KeyValue
    |> where(user_id: ^user_id, key: ^key)
    |> select([kv], kv.value)
    |> Repo.one()
  end

  @spec lookup(integer(), String.t(), prepared_accessor() | nil) :: term() | nil
  def lookup(user_id, key, nil), do: lookup(user_id, key)

  def lookup(user_id, key, {path, _prepared} = accessor_path) when is_binary(path) do
    case lookup(user_id, key) do
      nil -> nil
      value -> extract_value(value, accessor_path)
    end
  end

  def lookup(_user_id, _key, accessor_path) do
    raise ArgumentError,
          "expected accessor path to be nil or a prepared accessor, got: #{inspect(accessor_path)}"
  end

  @spec prepare_accessor(String.t() | nil) :: prepared_accessor() | nil
  def prepare_accessor(nil), do: nil

  def prepare_accessor("$" <> _ = jsonpath) do
    case Warpath.Expression.compile(jsonpath) do
      {:ok, expression} -> {jsonpath, expression}
      {:error, _} -> {jsonpath, :invalid}
    end
  end

  def prepare_accessor(dot_path), do: {dot_path, String.split(dot_path, ".")}

  @spec extract_value(term(), prepared_accessor() | nil) :: term() | nil
  def extract_value(value, nil), do: value

  def extract_value(value, {_path, %Warpath.Expression{} = jsonpath}) when is_map(value) do
    case Warpath.query(value, jsonpath) do
      {:ok, result} -> result
      {:error, _} -> nil
    end
  end

  def extract_value(value, {_path, keys}) when is_map(value) and is_list(keys) do
    get_in(value, keys)
  end

  def extract_value(_value, {_path, :invalid}), do: nil

  def extract_value(value, {path, _prepared}) when not is_map(value) and is_binary(path),
    do: nil

  def extract_value(_value, accessor_path) do
    raise ArgumentError, "expected a prepared accessor, got: #{inspect(accessor_path)}"
  end

  @spec bulk_upsert_key_values(integer(), [map()]) :: {non_neg_integer(), nil | [KeyValue.t()]}
  def bulk_upsert_key_values(user_id, entries) do
    now = DateTime.utc_now()

    rows =
      Enum.map(entries, fn entry ->
        %{
          user_id: user_id,
          key: entry[:key] || entry["key"],
          value: entry[:value] || entry["value"],
          updated_at: now,
          inserted_at: now
        }
      end)

    Repo.insert_all(KeyValue, rows,
      on_conflict: {:replace, [:value, :updated_at]},
      conflict_target: [:user_id, :key]
    )
  end

  @spec bump_usages([{integer(), String.t()}], DateTime.t()) :: :ok
  def bump_usages(pairs, now) when is_list(pairs) do
    pairs
    |> Enum.group_by(fn {uid, _} -> uid end, fn {_, k} -> k end)
    |> Enum.each(fn {user_id, keys} ->
      src =
        KeyValue
        |> where(user_id: ^user_id)
        |> where([kv], kv.key in ^keys)
        |> select([kv], %{key_value_id: kv.id, last_used_at: type(^now, :utc_datetime_usec)})

      Repo.insert_all(KeyValueUsage, src,
        on_conflict: {:replace, [:last_used_at]},
        conflict_target: [:key_value_id]
      )
    end)

    :ok
  end

  @spec prune_usages(DateTime.t()) :: {non_neg_integer(), nil}
  def prune_usages(now \\ DateTime.utc_now()) do
    cutoff = DateTime.add(now, -@usage_retention_days, :day)
    KeyValueUsage |> where([u], u.last_used_at < ^cutoff) |> Repo.delete_all()
  end

  @spec bulk_delete_by_keys(integer(), [String.t()]) :: {non_neg_integer(), nil}
  def bulk_delete_by_keys(user_id, keys) when is_list(keys) do
    KeyValue
    |> where(user_id: ^user_id)
    |> where([kv], kv.key in ^keys)
    |> Repo.delete_all()
  end

  @spec bulk_delete_by_values(integer(), String.t(), [String.t()]) :: {non_neg_integer(), nil}
  def bulk_delete_by_values(user_id, accessor_path, values) when is_list(values) do
    jsonb_keys = build_jsonb_accessor_keys(accessor_path)

    KeyValue
    |> where(user_id: ^user_id)
    |> where([kv], fragment("?#>>? = ANY(?)", kv.value, ^jsonb_keys, ^values))
    |> Repo.delete_all()
  end

  defp build_jsonb_accessor_keys(accessor_path) do
    if String.starts_with?(accessor_path, "$") do
      accessor_path
      |> String.trim_leading("$.")
      |> String.split(".")
      |> Enum.map(&String.trim/1)
    else
      String.split(accessor_path, ".")
    end
  end
end
