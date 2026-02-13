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
      insert(:key_value, user: user, key: "k1", value: %{"org" => "abc"})

      {:ok, _view, html} = live(conn, ~p"/key-values")

      assert html =~ "key values"
      assert html =~ "Total: 1"
      assert html =~ "Enter a key filter"
    end

    test "search requires key filter", %{conn: conn} do
      {:ok, view, _html} = live(conn, ~p"/key-values")

      html =
        view
        |> form("#search-form", %{"key" => ""})
        |> render_submit()

      assert html =~ "Key filter is required"
    end

    test "search by key returns exact matches", %{conn: conn, user: user} do
      insert(:key_value, user: user, key: "target", value: %{"v" => "1"})
      insert(:key_value, user: user, key: "other", value: %{"v" => "2"})

      {:ok, view, _html} = live(conn, ~p"/key-values")

      view
      |> form("#search-form", %{"key" => "target"})
      |> render_submit()

      html = render(view)
      assert html =~ "target"
      refute html =~ ">other<"
    end

    test "can create a key-value pair with JSON value", %{conn: conn} do
      {:ok, view, html} = live(conn, ~p"/key-values")
      assert html =~ "Total: 0"

      view |> element("button", "Create key-value pair") |> render_click()

      html =
        view
        |> form("#create-form", %{"key" => "new_key", "value" => ~s({"org": "abc"})})
        |> render_submit()

      assert html =~ "created"
      assert html =~ "Total: 1"
    end

    test "create shows error on duplicate key", %{conn: conn, user: user} do
      insert(:key_value, user: user, key: "dup_key", value: %{"v" => "1"})

      {:ok, view, _html} = live(conn, ~p"/key-values")

      view |> element("button", "Create key-value pair") |> render_click()

      html =
        view
        |> form("#create-form", %{"key" => "dup_key", "value" => ~s({"v": "2"})})
        |> render_submit()

      assert html =~ "has already been taken"
    end

    test "create respects plan limits", %{conn: conn} do
      plan = Logflare.Repo.one(Logflare.Billing.Plan)

      Logflare.Billing.Plan.changeset(plan, %{limit_key_values: 0})
      |> Logflare.Repo.update!()

      {:ok, view, _html} = live(conn, ~p"/key-values")

      view |> element("button", "Create key-value pair") |> render_click()

      html =
        view
        |> form("#create-form", %{"key" => "new_key", "value" => ~s({"v": "1"})})
        |> render_submit()

      assert html =~ "limit"
    end

    test "can delete a key-value pair", %{conn: conn, user: user} do
      insert(:key_value, user: user, key: "del_key", value: %{"v" => "1"})
      insert(:key_value, user: user, key: "keep_key", value: %{"v" => "2"})

      {:ok, view, html} = live(conn, ~p"/key-values")
      assert html =~ "Total: 2"

      # Search by key to see del_key
      view
      |> form("#search-form", %{"key" => "del_key"})
      |> render_submit()

      html = render(view)
      assert html =~ ">del_key<"

      kv = KeyValues.list_key_values(user_id: user.id, key: "del_key") |> hd()

      view
      |> element("button[phx-click='delete'][phx-value-id='#{kv.id}']")
      |> render_click()

      html = render(view)
      assert html =~ "No results found"
      assert html =~ "Total: 1"
    end

    test "clear search resets to initial state", %{conn: conn, user: user} do
      insert(:key_value, user: user, key: "k1", value: %{"v" => "1"})

      {:ok, view, _html} = live(conn, ~p"/key-values")

      view
      |> form("#search-form", %{"key" => "k1"})
      |> render_submit()

      html = render(view)
      assert html =~ ">k1<"

      view |> element("button", "Clear") |> render_click()

      assert render(view) =~ "Enter a key filter"
    end
  end
end
