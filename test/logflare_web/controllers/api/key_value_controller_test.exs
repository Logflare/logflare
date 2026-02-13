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
      insert(:key_value, user: user, key: "k1", value: %{"org" => "abc"})
      insert(:key_value)

      assert [%{"key" => "k1", "value" => %{"org" => "abc"}}] =
               conn
               |> add_access_token(user, "private")
               |> get(~p"/api/key-values")
               |> json_response(200)
    end

    test "filters by key", %{conn: conn, user: user} do
      insert(:key_value, user: user, key: "k1", value: %{"v" => "1"})
      insert(:key_value, user: user, key: "k2", value: %{"v" => "2"})

      assert [%{"key" => "k1"}] =
               conn
               |> add_access_token(user, "private")
               |> get(~p"/api/key-values?key=k1")
               |> json_response(200)
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
                 Jason.encode!([
                   %{key: "a", value: %{org: "1"}},
                   %{key: "b", value: %{org: "2"}}
                 ])
               )
               |> json_response(201)
    end

    test "upsert overwrites existing values", %{conn: conn, user: user} do
      plan = Logflare.Repo.get_by!(Logflare.Billing.Plan, name: "Free")
      Ecto.Changeset.change(plan, limit_key_values: 10_000_000) |> Logflare.Repo.update!()

      conn
      |> add_access_token(user, "private")
      |> put_req_header("content-type", "application/json")
      |> post(~p"/api/key-values", Jason.encode!([%{key: "a", value: %{n: "original"}}]))
      |> json_response(201)

      conn
      |> add_access_token(user, "private")
      |> put_req_header("content-type", "application/json")
      |> post(~p"/api/key-values", Jason.encode!([%{key: "a", value: %{n: "updated"}}]))
      |> json_response(201)

      assert [%{"key" => "a", "value" => %{"n" => "updated"}}] =
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
      |> post(~p"/api/key-values", Jason.encode!([%{key: "over", value: %{v: "limit"}}]))
      |> response(400)
    end
  end

  describe "delete" do
    test "deletes by keys", %{conn: conn, user: user} do
      insert(:key_value, user: user, key: "k1", value: %{"v" => "1"})
      insert(:key_value, user: user, key: "k2", value: %{"v" => "2"})
      insert(:key_value, user: user, key: "k3", value: %{"v" => "3"})

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

    test "deletes by accessor path into values", %{conn: conn, user: user} do
      insert(:key_value, user: user, key: "k1", value: %{"org" => "shared"})
      insert(:key_value, user: user, key: "k2", value: %{"org" => "shared"})
      insert(:key_value, user: user, key: "k3", value: %{"org" => "other"})

      assert %{"deleted_count" => 2} =
               conn
               |> add_access_token(user, "private")
               |> delete(~p"/api/key-values", %{
                 values: ["shared"],
                 accessor: "org"
               })
               |> json_response(200)

      assert [%{"key" => "k3"}] =
               conn
               |> add_access_token(user, "private")
               |> get(~p"/api/key-values")
               |> json_response(200)
    end
  end
end
