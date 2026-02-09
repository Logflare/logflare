defmodule LogflareWeb.KeyValuesLiveTest do
  @moduledoc false
  use LogflareWeb.ConnCase

  alias Logflare.KeyValues

  setup do
    insert(:plan, limit_key_values: 100)
    :ok
  end

  test "redirects unauthenticated users to login", %{conn: conn} do
    assert {:error, {:redirect, %{to: "/auth/login"}}} = live(conn, ~p"/key-values")
  end

  describe "authenticated user" do
    setup %{conn: conn} do
      user = insert(:user)
      conn = login_user(conn, user)
      {:ok, user: user, conn: conn}
    end

    test "renders initial page with count and search prompt", %{conn: conn, user: user} do
      insert(:key_value, user: user, key: "k1", value: "v1")

      {:ok, _view, html} = live(conn, ~p"/key-values")

      assert html =~ "key values"
      assert html =~ "Total: 1"
      assert html =~ "Enter filter values"
    end

    test "search requires at least one filter", %{conn: conn} do
      {:ok, view, _html} = live(conn, ~p"/key-values")

      html =
        view
        |> form("#search-form", %{"key" => "", "value" => ""})
        |> render_submit()

      assert html =~ "At least one filter"
    end

    test "search by key returns exact matches", %{conn: conn, user: user} do
      insert(:key_value, user: user, key: "target", value: "v1")
      insert(:key_value, user: user, key: "other", value: "v2")

      {:ok, view, _html} = live(conn, ~p"/key-values")

      view
      |> form("#search-form", %{"key" => "target", "value" => ""})
      |> render_submit()

      html = render(view)
      assert html =~ "target"
      assert html =~ "v1"
      refute html =~ ">other<"
    end

    test "search by value returns exact matches", %{conn: conn, user: user} do
      insert(:key_value, user: user, key: "k1", value: "target_val")
      insert(:key_value, user: user, key: "k2", value: "other_val")

      {:ok, view, _html} = live(conn, ~p"/key-values")

      view
      |> form("#search-form", %{"key" => "", "value" => "target_val"})
      |> render_submit()

      html = render(view)
      assert html =~ "target_val"
      refute html =~ "other_val"
    end

    test "can create a key-value pair", %{conn: conn} do
      {:ok, view, _html} = live(conn, ~p"/key-values")

      view |> element("button", "Create key-value pair") |> render_click()

      html =
        view
        |> form("#create-form", %{"key" => "new_key", "value" => "new_val"})
        |> render_submit()

      assert html =~ "created"
      assert html =~ "Total: 1"
    end

    test "create shows error on duplicate key", %{conn: conn, user: user} do
      insert(:key_value, user: user, key: "dup_key", value: "v1")

      {:ok, view, _html} = live(conn, ~p"/key-values")

      view |> element("button", "Create key-value pair") |> render_click()

      html =
        view
        |> form("#create-form", %{"key" => "dup_key", "value" => "v2"})
        |> render_submit()

      assert html =~ "has already been taken"
    end

    test "create respects plan limits", %{conn: conn} do
      # Update plan to have limit of 0
      plan = Logflare.Repo.one(Logflare.Billing.Plan)

      Logflare.Billing.Plan.changeset(plan, %{limit_key_values: 0})
      |> Logflare.Repo.update!()

      {:ok, view, _html} = live(conn, ~p"/key-values")

      view |> element("button", "Create key-value pair") |> render_click()

      html =
        view
        |> form("#create-form", %{"key" => "new_key", "value" => "new_val"})
        |> render_submit()

      assert html =~ "limit"
    end

    test "can delete a key-value pair", %{conn: conn, user: user} do
      insert(:key_value, user: user, key: "del_key", value: "del_val")
      insert(:key_value, user: user, key: "keep_key", value: "del_val")

      {:ok, view, _html} = live(conn, ~p"/key-values")

      # Search by value to see both results
      view
      |> form("#search-form", %{"key" => "", "value" => "del_val"})
      |> render_submit()

      html = render(view)
      assert html =~ "del_key"
      assert html =~ "keep_key"

      # Find and delete del_key
      kv = KeyValues.list_key_values(user_id: user.id, key: "del_key") |> hd()

      view
      |> element("button[phx-click='delete'][phx-value-id='#{kv.id}']")
      |> render_click()

      html = render(view)
      refute html =~ "del_key"
      assert html =~ "keep_key"
    end

    test "delete refreshes search results", %{conn: conn, user: user} do
      kv = insert(:key_value, user: user, key: "to_delete", value: "shared_val")
      insert(:key_value, user: user, key: "keep", value: "shared_val")

      {:ok, view, _html} = live(conn, ~p"/key-values")

      view
      |> form("#search-form", %{"key" => "", "value" => "shared_val"})
      |> render_submit()

      html = render(view)
      assert html =~ "to_delete"
      assert html =~ "keep"

      view
      |> element("button[phx-click='delete'][phx-value-id='#{kv.id}']")
      |> render_click()

      html = render(view)
      refute html =~ "to_delete"
      assert html =~ "keep"
    end

    test "clear search resets to initial state", %{conn: conn, user: user} do
      insert(:key_value, user: user, key: "k1", value: "v1")

      {:ok, view, _html} = live(conn, ~p"/key-values")

      view
      |> form("#search-form", %{"key" => "k1", "value" => ""})
      |> render_submit()

      html = render(view)
      assert html =~ ">k1<"

      view |> element("button", "Clear") |> render_click()

      assert render(view) =~ "Enter filter values"
    end

    test "count updates after create and delete", %{conn: conn, user: user} do
      {:ok, view, html} = live(conn, ~p"/key-values")
      assert html =~ "Total: 0"

      # Create
      view |> element("button", "Create key-value pair") |> render_click()

      view
      |> form("#create-form", %{"key" => "new_key", "value" => "new_val"})
      |> render_submit()

      assert render(view) =~ "Total: 1"

      # Search to show it, then delete
      view
      |> form("#search-form", %{"key" => "new_key", "value" => ""})
      |> render_submit()

      kv = KeyValues.list_key_values(user_id: user.id) |> hd()

      view
      |> element("button[phx-click='delete'][phx-value-id='#{kv.id}']")
      |> render_click()

      assert render(view) =~ "Total: 0"
    end
  end
end
