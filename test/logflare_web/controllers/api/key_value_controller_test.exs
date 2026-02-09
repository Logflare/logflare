defmodule LogflareWeb.Api.KeyValueControllerTest do
  @moduledoc false
  use LogflareWeb.ConnCase

  setup do
    insert(:plan, name: "Free")
    user = insert(:user)
    [user: user]
  end

  describe "index" do
    test "lists user's KV pairs with 500 limit", %{conn: conn, user: user} do
      insert(:key_value, user: user, key: "k1", value: "v1")
      insert(:key_value)

      assert [%{"key" => "k1"}] =
               conn
               |> add_access_token(user, "private")
               |> get(~p"/api/key-values")
               |> json_response(200)
    end

    test "filters by key", %{conn: conn, user: user} do
      insert(:key_value, user: user, key: "k1", value: "v1")
      insert(:key_value, user: user, key: "k2", value: "v2")

      assert [%{"key" => "k1"}] =
               conn
               |> add_access_token(user, "private")
               |> get(~p"/api/key-values?key=k1")
               |> json_response(200)
    end

    test "filters by value", %{conn: conn, user: user} do
      insert(:key_value, user: user, key: "k1", value: "shared")
      insert(:key_value, user: user, key: "k2", value: "shared")
      insert(:key_value, user: user, key: "k3", value: "other")

      result =
        conn
        |> add_access_token(user, "private")
        |> get(~p"/api/key-values?value=shared")
        |> json_response(200)

      assert length(result) == 2
    end
  end

  describe "create" do
    test "upserts multiple KV pairs", %{conn: conn, user: user} do
      plan = Logflare.Repo.get_by!(Logflare.Billing.Plan, name: "Free")
      Ecto.Changeset.change(plan, limit_key_values: 10_000_000) |> Logflare.Repo.update!()

      assert %{"inserted_count" => 2} =
               conn
               |> add_access_token(user, "private")
               |> put_req_header("content-type", "application/json")
               |> post(
                 ~p"/api/key-values",
                 Jason.encode!([%{key: "a", value: "1"}, %{key: "b", value: "2"}])
               )
               |> json_response(201)
    end

    test "upsert overwrites existing values", %{conn: conn, user: user} do
      plan = Logflare.Repo.get_by!(Logflare.Billing.Plan, name: "Free")
      Ecto.Changeset.change(plan, limit_key_values: 10_000_000) |> Logflare.Repo.update!()

      conn
      |> add_access_token(user, "private")
      |> put_req_header("content-type", "application/json")
      |> post(~p"/api/key-values", Jason.encode!([%{key: "a", value: "original"}]))
      |> json_response(201)

      conn
      |> add_access_token(user, "private")
      |> put_req_header("content-type", "application/json")
      |> post(~p"/api/key-values", Jason.encode!([%{key: "a", value: "updated"}]))
      |> json_response(201)

      assert [%{"key" => "a", "value" => "updated"}] =
               conn
               |> add_access_token(user, "private")
               |> get(~p"/api/key-values")
               |> json_response(200)
    end

    test "enforces billing limit", %{conn: conn, user: user} do
      insert(:key_value, user: user)

      conn
      |> add_access_token(user, "private")
      |> put_req_header("content-type", "application/json")
      |> post(~p"/api/key-values", Jason.encode!([%{key: "over", value: "limit"}]))
      |> response(400)
    end
  end

  describe "delete" do
    test "deletes by keys", %{conn: conn, user: user} do
      insert(:key_value, user: user, key: "k1", value: "v1")
      insert(:key_value, user: user, key: "k2", value: "v2")
      insert(:key_value, user: user, key: "k3", value: "v3")

      assert %{"deleted_count" => 2} =
               conn
               |> add_access_token(user, "private")
               |> delete(~p"/api/key-values", %{keys: ["k1", "k2"]})
               |> json_response(200)

      assert [%{"key" => "k3"}] =
               conn
               |> add_access_token(user, "private")
               |> get(~p"/api/key-values")
               |> json_response(200)
    end

    test "deletes by values", %{conn: conn, user: user} do
      insert(:key_value, user: user, key: "k1", value: "shared")
      insert(:key_value, user: user, key: "k2", value: "shared")
      insert(:key_value, user: user, key: "k3", value: "other")

      assert %{"deleted_count" => 2} =
               conn
               |> add_access_token(user, "private")
               |> delete(~p"/api/key-values", %{values: ["shared"]})
               |> json_response(200)

      assert [%{"key" => "k3"}] =
               conn
               |> add_access_token(user, "private")
               |> get(~p"/api/key-values")
               |> json_response(200)
    end
  end
end
