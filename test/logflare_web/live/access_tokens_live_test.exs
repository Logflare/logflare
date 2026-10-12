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

  test "dismisses the created token notice", %{conn: conn} do
    {:ok, view, _html} = live(conn, ~p"/access-tokens")
    do_ui_create_token(view, "ingest")

    assert has_element?(view, "button[phx-click='dismiss-created-token'][type='button']")
    view |> element("button", "Dismiss") |> render_click()
    refute has_element?(view, "button", "Dismiss")
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

    assert has_element?(view, "#scopes-ingest-selected[checked]")
    refute has_element?(view, "#scopes-ingest-all[checked]")
    assert has_element?(view, "input#access_token_private[type='checkbox']")
    assert has_element?(view, "label[for='access_token_private']", "Private")
    assert has_element?(view, "input#access_token_description[type='text'][autofocus]")

    select_html =
      view
      |> element("#scopes-ingest")
      |> render()
      |> Floki.parse_fragment!()

    assert [_select] = Floki.find(select_html, "select[multiple]")

    labels =
      select_html
      |> Floki.find("option:not([value=''])")
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
    |> render_submit(
      token_params(
        description: "multiple sources",
        ingest: %{enabled: true, mode: :selected, selected_ids: ["", first.id, second.id]}
      )
    )

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
    |> render_change(
      token_params(
        ingest: %{enabled: true, mode: :all},
        query: %{enabled: true, mode: :selected}
      )
    )

    select_html =
      view
      |> element("#scopes-query")
      |> render()
      |> Floki.parse_fragment!()

    assert [_select] = Floki.find(select_html, "select[multiple]")

    labels =
      select_html
      |> Floki.find("option:not([value=''])")
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
    |> render_submit(
      token_params(
        description: "multiple endpoints",
        ingest: %{enabled: false},
        query: %{enabled: true, mode: :selected, selected_ids: ["", first.id, second.id]}
      )
    )

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
      |> render_submit(
        token_params(
          description: "empty sources",
          ingest: %{enabled: true, mode: :selected, selected_ids: [""]}
        )
      )

    assert html =~ "select at least one source"

    html =
      view
      |> element("form")
      |> render_submit(
        token_params(
          description: "empty endpoints",
          ingest: %{enabled: false},
          query: %{enabled: true, mode: :selected, selected_ids: [""]}
        )
      )

    assert html =~ "select at least one endpoint"
    assert Logflare.Auth.list_valid_access_tokens(user) == []
  end

  test "selected resources reject unrestricted and unowned scope values", %{
    conn: conn,
    user: user
  } do
    other_source = insert(:source, user: insert(:user))
    {:ok, view, _html} = live(conn, ~p"/access-tokens")
    view |> element("button", "Create access token") |> render_click()

    invalid_payloads = [
      {token_params(
         description: "unrestricted ingest",
         ingest: %{enabled: true, mode: :selected, selected_ids: ["ingest"]}
       ), "is invalid"},
      {token_params(
         description: "private through query",
         ingest: %{enabled: false},
         query: %{enabled: true, mode: :selected, selected_ids: ["private"]}
       ), "is invalid"},
      {token_params(
         description: "unowned source",
         ingest: %{enabled: true, mode: :selected, selected_ids: [other_source.id]}
       ), "contains an invalid selected source"}
    ]

    for {payload, error} <- invalid_payloads do
      html = view |> element("form") |> render_submit(payload)
      assert html =~ error
    end

    assert Logflare.Auth.list_valid_access_tokens(user) == []
  end

  test "missing modes grant only selected resources for cast enabled values", %{
    conn: conn,
    user: user
  } do
    source = insert(:source, user: user)
    endpoint = insert(:endpoint, user: user)

    for enabled <- ["true", "1"] do
      {:ok, view, _html} = live(conn, ~p"/access-tokens/new")

      payload =
        token_params(
          description: "missing modes #{enabled}",
          ingest: %{enabled: enabled, selected_ids: [source.id]},
          query: %{enabled: enabled, selected_ids: [endpoint.id]}
        )
        |> update_in(["access_token", "ingest"], &Map.delete(&1, "mode"))
        |> update_in(["access_token", "query"], &Map.delete(&1, "mode"))

      assert render_submit(view, "create-token", payload) =~ "created successfully"
    end

    tokens = Logflare.Auth.list_valid_access_tokens(user)
    assert length(tokens) == 2

    for token <- tokens do
      assert MapSet.new(String.split(token.scopes)) ==
               MapSet.new(["ingest:source:#{source.id}", "query:endpoint:#{endpoint.id}"])
    end
  end

  test "missing modes still reject empty and unowned selected resources", %{
    conn: conn,
    user: user
  } do
    other_user = insert(:user)
    other_source = insert(:source, user: other_user)
    other_endpoint = insert(:endpoint, user: other_user)
    {:ok, view, _html} = live(conn, ~p"/access-tokens/new")

    for {permission, selected_ids, error} <- [
          {:ingest, [], "select at least one source"},
          {:query, [], "select at least one endpoint"},
          {:ingest, [other_source.id], "contains an invalid selected source"},
          {:query, [other_endpoint.id], "contains an invalid selected endpoint"}
        ] do
      payload =
        token_params(ingest: %{enabled: false}, query: %{enabled: false})
        |> put_in(["access_token", Atom.to_string(permission)], %{
          "enabled" => "1",
          "selected_ids" => Enum.map(selected_ids, &to_string/1)
        })

      assert render_submit(view, "create-token", payload) =~ error
    end

    assert Logflare.Auth.list_valid_access_tokens(user) == []
  end

  test "permission fields and embeds cannot be null or blank", %{conn: conn, user: user} do
    source = insert(:source, user: user)
    {:ok, view, _html} = live(conn, ~p"/access-tokens/new")

    base =
      token_params(
        description: "invalid permission",
        ingest: %{enabled: true, mode: :selected, selected_ids: [source.id]}
      )

    invalid_payloads = [
      put_in(base, ["access_token", "ingest", "mode"], nil),
      put_in(base, ["access_token", "ingest", "mode"], ""),
      put_in(base, ["access_token", "ingest", "enabled"], nil),
      put_in(base, ["access_token", "ingest"], nil)
    ]

    for payload <- invalid_payloads do
      html = render_submit(view, "create-token", payload)
      assert html =~ "Could not create access token"
    end

    assert Logflare.Auth.list_valid_access_tokens(user) == []
  end

  test "malformed changes remain invalid instead of restoring default permissions", %{
    conn: conn,
    user: user
  } do
    {:ok, view, _html} = live(conn, ~p"/access-tokens/new")
    payload = put_in(token_params([]), ["access_token", "ingest", "mode"], nil)

    view |> element("form") |> render_change(payload)

    refute has_element?(view, "#scopes-ingest-all[checked]")
    refute has_element?(view, "#scopes-ingest-selected[checked]")

    html = view |> element("form") |> render_submit(payload)
    assert html =~ "Could not create access token"
    assert Logflare.Auth.list_valid_access_tokens(user) == []
  end

  test "selected resources are checked against current ownership on submit", %{
    conn: conn,
    user: user
  } do
    source = insert(:source, user: user)
    {:ok, view, _html} = live(conn, ~p"/access-tokens/new")
    assert {:ok, _source} = Logflare.Sources.delete_source(source)

    html =
      view
      |> element("form")
      |> render_submit(
        token_params(
          description: "deleted source",
          ingest: %{enabled: true, mode: :selected, selected_ids: [source.id]}
        )
      )

    assert html =~ "contains an invalid selected source"
    assert Logflare.Auth.list_valid_access_tokens(user) == []
  end

  test "duplicate selected IDs are normalized before creating a token", %{
    conn: conn,
    user: user
  } do
    source = insert(:source, user: user)
    {:ok, view, _html} = live(conn, ~p"/access-tokens")
    view |> element("button", "Create access token") |> render_click()

    view
    |> element("form")
    |> render_submit(
      token_params(
        description: "duplicate selected ID",
        ingest: %{enabled: true, mode: :selected, selected_ids: [source.id, source.id]}
      )
    )

    [token] = Logflare.Auth.list_valid_access_tokens(user)
    assert token.scopes == "ingest:source:#{source.id}"
  end

  test "empty scope sets and invalid modes are rejected", %{conn: conn, user: user} do
    {:ok, view, _html} = live(conn, ~p"/access-tokens")
    view |> element("button", "Create access token") |> render_click()

    html =
      view
      |> element("form")
      |> render_submit(
        token_params(description: "empty", ingest: %{enabled: false}, query: %{enabled: false})
      )

    assert html =~ "Could not create access token: select at least one scope"

    html =
      view
      |> element("form")
      |> render_submit(
        token_params(description: "bad mode", ingest: %{enabled: true, mode: "bogus"})
      )

    assert html =~ "is invalid"
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

  test "private scope overrides preserved resource selections", %{conn: conn, user: user} do
    source = insert(:source, user: user)
    endpoint = insert(:endpoint, user: user)
    {:ok, view, _html} = live(conn, ~p"/access-tokens")
    view |> element("button", "Create access token") |> render_click()

    view
    |> element("form")
    |> render_submit(
      token_params(
        description: "private override",
        private: true,
        ingest: %{enabled: true, mode: :selected, selected_ids: [source.id]},
        query: %{enabled: true, mode: :selected, selected_ids: [endpoint.id]}
      )
    )

    [token] = Logflare.Auth.list_valid_access_tokens(user)
    assert token.scopes == "private"
  end

  test "private scope selects and disables unrestricted ingest and query", %{conn: conn} do
    {:ok, view, _html} = live(conn, ~p"/access-tokens")
    view |> element("button", "Create access token") |> render_click()

    view
    |> element("form")
    |> render_change(token_params(private: true, ingest: %{enabled: true, mode: :selected}))

    assert has_element?(view, "#scopesmainingest[checked][disabled]")
    assert has_element?(view, "#scopesmainquery[checked][disabled]")

    for {permission, enabled} <- [{"ingest", "true"}, {"query", "false"}] do
      hidden_inputs =
        view
        |> render()
        |> Floki.parse_fragment!()
        |> Floki.find(
          "input[type='hidden'][name='access_token[#{permission}][enabled]']:not([disabled])"
        )

      assert Floki.attribute(hidden_inputs, "value") == [enabled]
    end

    assert has_element?(
             view,
             "input[name='access_token[private]'][value='true'][checked]:not([disabled])"
           )

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
    {:ok, view, _html} = live(conn, ~p"/access-tokens")
    view |> element("button", "Create access token") |> render_click()

    view
    |> element("form")
    |> render_change(
      token_params(
        private: true,
        ingest: %{enabled: true, mode: :selected, selected_ids: [source.id]},
        query: %{enabled: true, mode: :selected, selected_ids: [endpoint.id]}
      )
    )

    assert has_element?(
             view,
             "input[type='hidden'][name='access_token[ingest][selected_ids][]'][value='#{source.id}']"
           )

    assert has_element?(
             view,
             "input[type='hidden'][name='access_token[query][selected_ids][]'][value='#{endpoint.id}']"
           )

    view
    |> element("form")
    |> render_change(
      token_params(
        private: false,
        ingest: %{enabled: true, mode: :selected, selected_ids: [source.id]},
        query: %{enabled: true, mode: :selected, selected_ids: [endpoint.id]}
      )
    )

    refute has_element?(view, "input[name='access_token[private]'][value='true'][checked]")
    assert has_element?(view, "#scopesmainingest[checked]")
    assert has_element?(view, "#scopesmainquery[checked]")
    assert has_element?(view, "#scopes-ingest-selected[checked]")
    assert has_element?(view, "#scopes-query-selected[checked]")
    assert has_element?(view, "#scopes-ingest option[value='#{source.id}'][selected]")
    assert has_element?(view, "#scopes-query option[value='#{endpoint.id}'][selected]")
  end

  test "create token - rejects partner value from crafted form payload", %{conn: conn, user: user} do
    {:ok, view, _html} = live(conn, ~p"/access-tokens")

    assert view |> element("button", "Create access token") |> render_click()

    html =
      view
      |> element("form")
      |> render_submit(
        token_params(
          description: "crafted",
          ingest: %{enabled: true, mode: :selected, selected_ids: ["partner"]}
        )
      )

    assert html =~ "is invalid"
    assert Logflare.Auth.list_valid_access_tokens(user) == []
  end

  test "create token - tolerates non-list scope params from crafted form payload", %{
    conn: conn,
    user: user
  } do
    {:ok, view, _html} = live(conn, ~p"/access-tokens")

    assert view |> element("button", "Create access token") |> render_click()

    html =
      view
      |> element("form")
      |> render_submit(%{"access_token" => %{"description" => "crafted", "ingest" => "partner"}})

    assert html =~ "is invalid"
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
    |> render_change(%{"access_token" => %{"description" => "crafted", "ingest" => "partner"}})

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
      |> render_submit(
        token_params(
          description: "crafted",
          ingest: %{enabled: true, mode: :selected, selected_ids: ["not-a-number"]},
          query: %{enabled: true, mode: :selected, selected_ids: ["abc"]}
        )
      )

    assert html =~ "is invalid"
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

    form_options =
      cond do
        scopes == "private" ->
          [description: "some description", private: true]

        String.starts_with?(scopes, "ingest:source:") ->
          [
            description: "some description",
            ingest: %{
              enabled: true,
              mode: :selected,
              selected_ids: [scope_id(scopes)]
            }
          ]

        String.starts_with?(scopes, "query:endpoint:") ->
          [
            description: "some description",
            ingest: %{enabled: false},
            query: %{enabled: true, mode: :selected, selected_ids: [scope_id(scopes)]}
          ]

        scopes == "query" ->
          [
            description: "some description",
            ingest: %{enabled: false},
            query: %{enabled: true, mode: :all}
          ]

        scopes == "ingest" ->
          [description: "some description"]
      end

    assert view
           |> element("form")
           |> render_submit(token_params(form_options)) =~ "created successfully"

    view |> element("table") |> render()
  end

  defp token_params(options) do
    ingest = permission_params(true, Keyword.get(options, :ingest, %{}))
    query = permission_params(false, Keyword.get(options, :query, %{}))

    %{
      "access_token" => %{
        "description" => Keyword.get(options, :description, ""),
        "private" => to_string(Keyword.get(options, :private, false)),
        "ingest" => ingest,
        "query" => query
      }
    }
  end

  defp permission_params(default_enabled, overrides) do
    permission =
      Map.merge(
        %{enabled: default_enabled, mode: :all, selected_ids: []},
        overrides
      )

    %{
      "enabled" => to_string(permission.enabled),
      "mode" => to_string(permission.mode),
      "selected_ids" => Enum.map(permission.selected_ids, &to_string/1)
    }
  end

  defp scope_id(scope) do
    scope |> String.split(":") |> List.last() |> String.to_integer()
  end
end
