defmodule LogflareWeb.AccessTokensLiveTest do
  @moduledoc false
  use LogflareWeb.ConnCase

  setup %{conn: conn} do
    insert(:plan)
    user = insert(:user)
    conn = conn |> login_user(user)

    {:ok, user: user, conn: conn}
  end

  test "subheader", %{conn: conn} do
    {:ok, view, _html} = live(conn, ~p"/access-tokens")

    assert view
           |> element("a", "docs")
           |> has_element?()
  end

  test "new action controls create form visibility", %{conn: conn} do
    {:ok, view, _html} = live(conn, ~p"/access-tokens")

    refute has_element?(view, "form")

    view |> element("button", "Create access token") |> render_click()
    assert_patch(view, ~p"/access-tokens/new")
    assert has_element?(view, "form")

    view |> element("button", "Cancel") |> render_click()
    assert_patch(view, ~p"/access-tokens")
    refute has_element?(view, "form")
  end

  test "legacy api key - show only when no access tokens", %{conn: conn} do
    {:ok, view, _html} = live(conn, ~p"/access-tokens")
    html = render(view)
    # able to copy, visible
    assert view
           |> element("button", "Copy")
           |> has_element?()

    # able to see legacy user token
    assert html =~ "Deprecated"
    assert html =~ "Copy"
  end

  test "deprecated: public token", %{conn: conn, user: user} do
    token = insert(:access_token, scopes: "public", resource_owner: user)
    {:ok, view, _html} = live(conn, ~p"/access-tokens")
    html = render(view)
    # able to copy, visible
    assert view
           |> element("button", "Copy")
           |> has_element?()

    assert html =~ token.token
    assert html =~ "public"
    refute html =~ "Deprecated"
    assert html =~ "No description"
  end

  test "create token - ingest into all", %{conn: conn} do
    {:ok, view, _html} = live(conn, ~p"/access-tokens")
    do_ui_create_token(view, "ingest")
    html = view |> element("table") |> render()
    # able to copy, visible
    assert view
           |> element("button", "Copy")
           |> has_element?()

    assert html =~ "ingest"
  end

  test "create token - ingest into one source", %{conn: conn, user: user} do
    source = insert(:source, user: user)
    {:ok, view, _html} = live(conn, ~p"/access-tokens")
    html = do_ui_create_token(view, "ingest:source:#{source.id}")
    # able to copy, visible
    assert view
           |> element("button", "Copy")
           |> has_element?()

    assert html =~ "ingest (#{source.name})"
    refute html =~ "ingest (all)"
  end

  test "selected sources combobox is a single, alphabetically sorted multiple select", %{
    conn: conn,
    user: user
  } do
    for name <- ["Zulu", "alpha", "Bravo"], do: insert(:source, user: user, name: name)

    {:ok, view, _html} = live(conn, ~p"/access-tokens")
    view |> element("button", "Create access token") |> render_click()

    assert has_element?(view, "#scopes-ingest-all[checked]")
    refute has_element?(view, "#scopes-ingest")

    view
    |> element("form")
    |> render_change(%{
      scopes_main: ["ingest"],
      scopes_ingest_mode: "selected",
      scopes_ingest: [],
      scopes_query: []
    })

    select_html =
      view
      |> element("#scopes-ingest")
      |> render()
      |> Floki.parse_fragment!()

    assert [_select] = Floki.find(select_html, "select[multiple]")

    labels =
      select_html
      |> Floki.find("option[value^='ingest:source:']")
      |> Enum.map(&Floki.text/1)
      |> Enum.map(&String.trim/1)

    assert labels == ["alpha", "Bravo", "Zulu"]
  end

  test "create token - ingest into multiple selected sources", %{conn: conn, user: user} do
    [first, second] = insert_pair(:source, user: user)
    {:ok, view, _html} = live(conn, ~p"/access-tokens")

    view |> element("button", "Create access token") |> render_click()

    view
    |> element("form")
    |> render_submit(%{
      description: "multiple sources",
      scopes_main: ["ingest"],
      scopes_ingest_mode: "selected",
      scopes_ingest: ["ingest:source:#{first.id}", "ingest:source:#{second.id}"],
      scopes_query: []
    })

    [token] = Logflare.Auth.list_valid_access_tokens(user)

    assert MapSet.new(String.split(token.scopes)) ==
             MapSet.new(["ingest:source:#{first.id}", "ingest:source:#{second.id}"])
  end

  test "create token - query for one endpoint", %{conn: conn, user: user} do
    endpoint = insert(:endpoint, user: user)
    {:ok, view, _html} = live(conn, ~p"/access-tokens")
    do_ui_create_token(view, "query:endpoint:#{endpoint.id}")

    assert view
           |> element("button", "Copy")
           |> has_element?()

    html = view |> element("table") |> render()
    assert html =~ "query (#{endpoint.name})"
    refute html =~ "query (all)"
  end

  test "create token - query all endpoints", %{conn: conn} do
    {:ok, view, _html} = live(conn, ~p"/access-tokens")
    do_ui_create_token(view, "query")

    html = view |> element("table") |> render()
    assert html =~ "query (all)"
  end

  test "selected endpoints combobox is a single, alphabetically sorted multiple select", %{
    conn: conn,
    user: user
  } do
    for name <- ["Zulu", "alpha", "Bravo"], do: insert(:endpoint, user: user, name: name)

    {:ok, view, _html} = live(conn, ~p"/access-tokens")
    view |> element("button", "Create access token") |> render_click()

    view
    |> element("form")
    |> render_change(%{
      scopes_main: ["ingest", "query"],
      scopes_ingest_mode: "all",
      scopes_query_mode: "selected",
      scopes_query: []
    })

    select_html =
      view
      |> element("#scopes-query")
      |> render()
      |> Floki.parse_fragment!()

    assert [_select] = Floki.find(select_html, "select[multiple]")

    labels =
      select_html
      |> Floki.find("option[value^='query:endpoint:']")
      |> Enum.map(&Floki.text/1)
      |> Enum.map(&String.trim/1)

    assert labels == ["alpha", "Bravo", "Zulu"]
  end

  test "create token - query multiple selected endpoints", %{conn: conn, user: user} do
    [first, second] = insert_pair(:endpoint, user: user)
    {:ok, view, _html} = live(conn, ~p"/access-tokens")

    view |> element("button", "Create access token") |> render_click()

    view
    |> element("form")
    |> render_submit(%{
      description: "multiple endpoints",
      scopes_main: ["query"],
      scopes_query_mode: "selected",
      scopes_query: ["query:endpoint:#{first.id}", "query:endpoint:#{second.id}"],
      scopes_ingest: []
    })

    [token] = Logflare.Auth.list_valid_access_tokens(user)

    assert MapSet.new(String.split(token.scopes)) ==
             MapSet.new(["query:endpoint:#{first.id}", "query:endpoint:#{second.id}"])
  end

  test "selected resources require at least one selection", %{conn: conn, user: user} do
    {:ok, view, _html} = live(conn, ~p"/access-tokens")
    view |> element("button", "Create access token") |> render_click()

    html =
      view
      |> element("form")
      |> render_submit(%{
        description: "empty sources",
        scopes_main: ["ingest"],
        scopes_ingest_mode: "selected",
        scopes_ingest: [""],
        scopes_query: []
      })

    assert html =~ "Select at least one source"

    html =
      view
      |> element("form")
      |> render_submit(%{
        description: "empty endpoints",
        scopes_main: ["query"],
        scopes_query_mode: "selected",
        scopes_ingest: [],
        scopes_query: [""]
      })

    assert html =~ "Select at least one endpoint"
    assert Logflare.Auth.list_valid_access_tokens(user) == []
  end

  test "create ingest token", %{conn: conn} do
    {:ok, view, _html} = live(conn, ~p"/access-tokens")

    do_ui_create_token(view, "ingest")

    assert view
           |> element("button", "Copy")
           |> has_element?()

    html = view |> element("table") |> render()
    assert html =~ "some description"
    assert html =~ "ingest (all)"
  end

  test "create private token", %{conn: conn} do
    {:ok, view, _html} = live(conn, ~p"/access-tokens")

    do_ui_create_token(view, "private")

    html = view |> element("table") |> render()
    assert html =~ "some description"
    assert html =~ "private"
  end

  test "private scope selects and disables unrestricted ingest and query", %{conn: conn} do
    {:ok, view, _html} = live(conn, ~p"/access-tokens")
    view |> element("button", "Create access token") |> render_click()

    view
    |> element("form")
    |> render_change(%{
      private: "true",
      scopes_main: ["ingest"],
      scopes_ingest_mode: "selected",
      scopes_ingest: [],
      scopes_query: []
    })

    assert has_element?(view, "input[value='ingest'][checked][disabled]")
    assert has_element?(view, "input[value='query'][checked][disabled]")
    assert has_element?(view, "input[name='private'][value='true'][checked]:not([disabled])")
    assert has_element?(view, "#scopes-ingest-all[checked][disabled]")
    assert has_element?(view, "#scopes-ingest-selected:not([checked])[disabled]")
    assert has_element?(view, "#scopes-query-all[checked][disabled]")
    assert has_element?(view, "#scopes-query-selected:not([checked])[disabled]")
    refute has_element?(view, "#scopes-ingest")
    refute has_element?(view, "#scopes-query")
  end

  test "private scope can be unchecked without losing selected resources", %{
    conn: conn,
    user: user
  } do
    source = insert(:source, user: user)
    endpoint = insert(:endpoint, user: user)
    source_scope = "ingest:source:#{source.id}"
    endpoint_scope = "query:endpoint:#{endpoint.id}"
    {:ok, view, _html} = live(conn, ~p"/access-tokens")
    view |> element("button", "Create access token") |> render_click()

    view
    |> element("form")
    |> render_change(%{
      private: "true",
      scopes_main: ["ingest", "query"],
      scopes_ingest_mode: "selected",
      scopes_ingest: [source_scope],
      scopes_query_mode: "selected",
      scopes_query: [endpoint_scope]
    })

    assert has_element?(
             view,
             "input[type='hidden'][name='scopes_ingest[]'][value='#{source_scope}']"
           )

    assert has_element?(
             view,
             "input[type='hidden'][name='scopes_query[]'][value='#{endpoint_scope}']"
           )

    view
    |> element("form")
    |> render_change(%{
      private: "false",
      scopes_main: ["ingest", "query"],
      scopes_ingest_mode: "selected",
      scopes_ingest: [source_scope],
      scopes_query_mode: "selected",
      scopes_query: [endpoint_scope]
    })

    refute has_element?(view, "input[name='private'][value='true'][checked]")
    assert has_element?(view, "input[value='ingest'][checked]")
    assert has_element?(view, "input[value='query'][checked]")
    assert has_element?(view, "#scopes-ingest-selected[checked]")
    assert has_element?(view, "#scopes-query-selected[checked]")
    assert has_element?(view, "#scopes-ingest option[value='#{source_scope}'][selected]")
    assert has_element?(view, "#scopes-query option[value='#{endpoint_scope}'][selected]")
  end

  test "create token - rejects partner scope from crafted form payload", %{conn: conn, user: user} do
    {:ok, view, _html} = live(conn, ~p"/access-tokens")

    assert view |> element("button", "Create access token") |> render_click()

    html =
      view
      |> element("form")
      |> render_submit(%{
        description: "crafted",
        scopes_main: ["ingest", "partner"],
        scopes_ingest: [],
        scopes_query: []
      })

    assert html =~ "Could not create access token"
    assert Logflare.Auth.list_valid_access_tokens(user) == []
  end

  test "create token - tolerates non-list scope params from crafted form payload", %{
    conn: conn,
    user: user
  } do
    {:ok, view, _html} = live(conn, ~p"/access-tokens")

    assert view |> element("button", "Create access token") |> render_click()

    view
    |> element("form")
    |> render_submit(%{
      description: "crafted",
      scopes_main: "partner",
      scopes_ingest: "ingest:source:1",
      scopes_query: %{"0" => "query:endpoint:1"}
    })

    assert Logflare.Auth.list_valid_access_tokens(user) == []
  end

  test "update-token-form - tolerates non-list scope params from crafted change payload", %{
    conn: conn,
    user: user
  } do
    {:ok, view, _html} = live(conn, ~p"/access-tokens")

    assert view |> element("button", "Create access token") |> render_click()

    view
    |> element("form")
    |> render_change(%{
      description: "crafted",
      scopes_main: "partner",
      scopes_ingest: "ingest:source:1",
      scopes_query: %{"0" => "query:endpoint:1"}
    })

    assert Logflare.Auth.list_valid_access_tokens(user) == []
  end

  test "create token - rejects unknown scopes from crafted form payload", %{
    conn: conn,
    user: user
  } do
    {:ok, view, _html} = live(conn, ~p"/access-tokens")

    assert view |> element("button", "Create access token") |> render_click()

    html =
      view
      |> element("form")
      |> render_submit(%{
        description: "crafted",
        scopes_main: ["ingest", "admin", "root"],
        scopes_ingest: ["ingest:source:not-a-number"],
        scopes_query: ["query:endpoint:abc"]
      })

    assert html =~ "Could not create access token"
    assert Logflare.Auth.list_valid_access_tokens(user) == []
  end

  test "show private token", %{conn: conn, user: user} do
    token = insert(:access_token, scopes: "private", resource_owner: user)
    {:ok, view, _html} = live(conn, ~p"/access-tokens")

    # not able to copy, not visible
    refute view
           |> element("button", "Copy")
           |> has_element?()

    html = render(view)
    refute html =~ token.token
    assert html =~ "private"
  end

  # returns the rendered table html
  defp do_ui_create_token(view, scopes) do
    assert view
           |> element("button", "Create access token")
           |> render_click()

    assert view |> element("button", "Create") |> has_element?()
    assert view |> element("label", "Scope") |> has_element?()

    assert view
           |> element("form")
           |> render_submit(%{
             description: "some description",
             private: to_string(scopes == "private"),
             scopes_main: if(scopes == "private" or scopes =~ ":", do: [], else: [scopes]),
             scopes_ingest_mode: if(scopes =~ "ingest:", do: "selected", else: "all"),
             scopes_query_mode: if(scopes =~ "query:", do: "selected", else: "all"),
             scopes_ingest: if(scopes =~ "ingest:", do: [scopes], else: []),
             scopes_query: if(scopes =~ "query:", do: [scopes], else: [])
           }) =~ "created successfully"

    view |> element("table") |> render()
  end
end
