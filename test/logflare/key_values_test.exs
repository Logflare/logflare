defmodule Logflare.KeyValuesTest do
  @moduledoc false
  use Logflare.DataCase

  alias Logflare.KeyValues

  setup do
    user = insert(:user)
    [user: user]
  end

  test "CRUD operations", %{user: user} do
    value = %{"org_id" => "abc", "name" => "Acme"}

    # create
    assert {:ok, kv} = KeyValues.create_key_value(%{user_id: user.id, key: "k1", value: value})
    assert kv.key == "k1"
    assert kv.value == value

    # read
    assert {:ok, ^kv} = KeyValues.fetch_key_value_by(id: kv.id, user_id: user.id)
    assert {:error, :not_found} = KeyValues.fetch_key_value_by(id: 0, user_id: user.id)

    # update
    new_value = %{"org_id" => "xyz", "name" => "Updated"}
    assert {:ok, updated} = KeyValues.update_key_value(kv, %{value: new_value})
    assert updated.value == new_value

    # list
    assert [_] = KeyValues.list_key_values(user_id: user.id)

    # delete
    assert {:ok, _} = KeyValues.delete_key_value(kv)
    assert {:error, :not_found} = KeyValues.fetch_key_value_by(id: kv.id, user_id: user.id)
  end

  test "rejects empty map value", %{user: user} do
    assert {:error, changeset} =
             KeyValues.create_key_value(%{user_id: user.id, key: "k", value: %{}})

    assert %{value: ["must not be empty"]} = errors_on(changeset)
  end

  test "unique constraint on user_id + key", %{user: user} do
    assert {:ok, _} =
             KeyValues.create_key_value(%{user_id: user.id, key: "dup", value: %{"v" => "1"}})

    assert {:error, %Ecto.Changeset{}} =
             KeyValues.create_key_value(%{user_id: user.id, key: "dup", value: %{"v" => "2"}})
  end

  test "lookup/2 returns map or nil", %{user: user} do
    value = %{"org_id" => "org_abc", "name" => "Acme"}
    insert(:key_value, user: user, key: "proj1", value: value)

    assert ^value = KeyValues.lookup(user.id, "proj1")
    assert nil == KeyValues.lookup(user.id, "nonexistent")
  end

  test "bulk_upsert_key_values/2 inserts and upserts", %{user: user} do
    entries = [%{key: "a", value: %{"n" => "1"}}, %{key: "b", value: %{"n" => "2"}}]
    assert {2, _} = KeyValues.bulk_upsert_key_values(user.id, entries)

    # upsert overwrites
    updated = %{"n" => "updated"}
    assert {1, _} = KeyValues.bulk_upsert_key_values(user.id, [%{key: "a", value: updated}])
    assert ^updated = KeyValues.lookup(user.id, "a")
  end

  test "count_key_values/1", %{user: user} do
    assert 0 = KeyValues.count_key_values(user.id)
    insert(:key_value, user: user, key: "k1")
    assert 1 = KeyValues.count_key_values(user.id)
  end

  test "list_key_values/1 filters by key", %{user: user} do
    insert(:key_value, user: user, key: "k1", value: %{"v" => "1"})
    insert(:key_value, user: user, key: "k2", value: %{"v" => "2"})

    assert [%{key: "k1"}] = KeyValues.list_key_values(user_id: user.id, key: "k1")
  end

  test "bulk_delete_by_keys/2", %{user: user} do
    insert(:key_value, user: user, key: "k1", value: %{"v" => "1"})
    insert(:key_value, user: user, key: "k2", value: %{"v" => "2"})
    insert(:key_value, user: user, key: "k3", value: %{"v" => "3"})

    assert {2, _} = KeyValues.bulk_delete_by_keys(user.id, ["k1", "k2"])
    assert [%{key: "k3"}] = KeyValues.list_key_values(user_id: user.id)
  end

  describe "list_key_values_query/1" do
    test "returns a query scoped to user", %{user: user} do
      for i <- 1..3, do: insert(:key_value, user: user, key: "k#{i}")

      results =
        KeyValues.list_key_values_query(user_id: user.id)
        |> Logflare.Repo.all()

      assert length(results) == 3
    end

    test "filters by key", %{user: user} do
      insert(:key_value, user: user, key: "match")
      insert(:key_value, user: user, key: "other")

      results =
        KeyValues.list_key_values_query(user_id: user.id, key: "match")
        |> Logflare.Repo.all()

      assert length(results) == 1
      assert hd(results).key == "match"
    end
  end

  describe "bulk_delete_by_values/3" do
    test "deletes by accessor path (dot syntax)", %{user: user} do
      insert(:key_value, user: user, key: "k1", value: %{"org" => "shared"})
      insert(:key_value, user: user, key: "k2", value: %{"org" => "shared"})
      insert(:key_value, user: user, key: "k3", value: %{"org" => "other"})

      assert {2, _} = KeyValues.bulk_delete_by_values(user.id, "org", ["shared"])
      assert [%{key: "k3"}] = KeyValues.list_key_values(user_id: user.id)
    end

    test "deletes by accessor path (nested dot syntax)", %{user: user} do
      insert(:key_value, user: user, key: "k1", value: %{"org" => %{"id" => "abc"}})
      insert(:key_value, user: user, key: "k2", value: %{"org" => %{"id" => "abc"}})
      insert(:key_value, user: user, key: "k3", value: %{"org" => %{"id" => "xyz"}})

      assert {2, _} = KeyValues.bulk_delete_by_values(user.id, "org.id", ["abc"])
      assert [%{key: "k3"}] = KeyValues.list_key_values(user_id: user.id)
    end

    test "deletes by accessor path (jsonpath)", %{user: user} do
      insert(:key_value, user: user, key: "k1", value: %{"org" => %{"id" => "abc"}})
      insert(:key_value, user: user, key: "k2", value: %{"org" => %{"id" => "xyz"}})

      assert {1, _} = KeyValues.bulk_delete_by_values(user.id, "$.org.id", ["abc"])
      assert [%{key: "k2"}] = KeyValues.list_key_values(user_id: user.id)
    end
  end
end
