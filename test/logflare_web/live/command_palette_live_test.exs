defmodule LogflareWeb.CommandPaletteLiveTest do
  use LogflareWeb.ConnCase, async: false

  import Phoenix.LiveViewTest

  alias LogflareWeb.CommandPaletteLive

  describe "fetch_sources event" do
    setup do
      insert(:plan)
      user = insert(:user)
      conn = Phoenix.ConnTest.build_conn()
      {:ok, user: user, conn: conn}
    end

    test "returns the user's non-system sources", %{conn: conn, user: user} do
      a = insert(:source, user: user, name: "alpha")
      b = insert(:source, user: user, name: "bravo")
      _system = insert(:source, user: user, name: "sys", system_source: true)
      _other = insert(:source, user: insert(:user), name: "other")

      {:ok, view, _html} =
        live_isolated(conn, CommandPaletteLive, session: %{"current_email" => user.email})

      render_hook(view, "fetch_sources", %{})
      assert_reply(view, %{sources: sources})

      names = sources |> Enum.map(& &1.name) |> Enum.sort()
      assert names == Enum.sort([a.name, b.name])

      refute Enum.any?(sources, &(&1.name == "sys"))
      refute Enum.any?(sources, &(&1.name == "other"))

      [first | _] = sources
      assert is_integer(first.id)
      assert is_boolean(first.favorite)
      assert Map.has_key?(first, :service_name)
    end

    test "returns an empty list when the user has no sources", %{conn: conn, user: user} do
      {:ok, view, _html} =
        live_isolated(conn, CommandPaletteLive, session: %{"current_email" => user.email})

      render_hook(view, "fetch_sources", %{})
      assert_reply(view, %{sources: []})
    end
  end

  test "returns an empty list when there is no current_email in session" do
    conn = Phoenix.ConnTest.build_conn()

    {:ok, view, _html} = live_isolated(conn, CommandPaletteLive)

    render_hook(view, "fetch_sources", %{})
    assert_reply(view, %{sources: []})
  end
end
