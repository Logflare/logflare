defmodule Logflare.KeyValuesTest do
  @moduledoc false
  use Logflare.DataCase

  alias Logflare.KeyValues

  setup do
    user = insert(:user)
    [user: user]
  end

  test "CRUD operations", %{user: user} do
    # create
    assert {:ok, kv} = KeyValues.create_key_value(%{user_id: user.id, key: "k1", value: "v1"})
    assert kv.key == "k1"
    assert kv.value == "v1"

    # read
    assert {:ok, ^kv} = KeyValues.fetch_key_value_by(id: kv.id, user_id: user.id)
    assert {:error, :not_found} = KeyValues.fetch_key_value_by(id: 0, user_id: user.id)

    # update
    assert {:ok, updated} = KeyValues.update_key_value(kv, %{value: "v2"})
    assert updated.value == "v2"

    # list
    assert [_] = KeyValues.list_key_values(user_id: user.id)

    # delete
    assert {:ok, _} = KeyValues.delete_key_value(kv)
    assert {:error, :not_found} = KeyValues.fetch_key_value_by(id: kv.id, user_id: user.id)
  end

  test "unique constraint on user_id + key", %{user: user} do
    assert {:ok, _} = KeyValues.create_key_value(%{user_id: user.id, key: "dup", value: "v1"})

    assert {:error, %Ecto.Changeset{}} =
             KeyValues.create_key_value(%{user_id: user.id, key: "dup", value: "v2"})
  end

  test "lookup/2 returns value or nil", %{user: user} do
    insert(:key_value, user: user, key: "proj1", value: "org_abc")

    assert "org_abc" = KeyValues.lookup(user.id, "proj1")
    assert nil == KeyValues.lookup(user.id, "nonexistent")
  end

  test "bulk_upsert_key_values/2 inserts and upserts", %{user: user} do
    entries = [%{key: "a", value: "1"}, %{key: "b", value: "2"}]
    assert {2, _} = KeyValues.bulk_upsert_key_values(user.id, entries)

    # upsert overwrites
    assert {1, _} = KeyValues.bulk_upsert_key_values(user.id, [%{key: "a", value: "updated"}])
    assert "updated" = KeyValues.lookup(user.id, "a")
  end

  test "count_key_values/1", %{user: user} do
    assert 0 = KeyValues.count_key_values(user.id)
    insert(:key_value, user: user, key: "k1")
    assert 1 = KeyValues.count_key_values(user.id)
  end

  test "list_key_values/1 filters by key", %{user: user} do
    insert(:key_value, user: user, key: "k1", value: "v1")
    insert(:key_value, user: user, key: "k2", value: "v2")

    assert [%{key: "k1"}] = KeyValues.list_key_values(user_id: user.id, key: "k1")
  end

  test "list_key_values/1 filters by value", %{user: user} do
    insert(:key_value, user: user, key: "k1", value: "shared")
    insert(:key_value, user: user, key: "k2", value: "shared")
    insert(:key_value, user: user, key: "k3", value: "other")

    result = KeyValues.list_key_values(user_id: user.id, value: "shared")
    assert length(result) == 2
  end

  test "bulk_delete_by_keys/2", %{user: user} do
    insert(:key_value, user: user, key: "k1", value: "v1")
    insert(:key_value, user: user, key: "k2", value: "v2")
    insert(:key_value, user: user, key: "k3", value: "v3")

    assert {2, _} = KeyValues.bulk_delete_by_keys(user.id, ["k1", "k2"])
    assert [%{key: "k3"}] = KeyValues.list_key_values(user_id: user.id)
  end

  test "bulk_delete_by_values/2", %{user: user} do
    insert(:key_value, user: user, key: "k1", value: "shared")
    insert(:key_value, user: user, key: "k2", value: "shared")
    insert(:key_value, user: user, key: "k3", value: "other")

    assert {2, _} = KeyValues.bulk_delete_by_values(user.id, ["shared"])
    assert [%{key: "k3"}] = KeyValues.list_key_values(user_id: user.id)
  end
end
