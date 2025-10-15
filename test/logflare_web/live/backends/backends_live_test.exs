defmodule LogflareWeb.BackendsLiveTest do
  use LogflareWeb.ConnCase

  alias Logflare.Sources

  setup do
    insert(:plan)

    :ok
  end

  defp log_in_user_with_source(%{conn: conn}) do
    user = insert(:user)
    source = insert(:source, user_id: user.id)

    %{conn: login_user(conn, user), user: user, source: source}
  end

  describe "index" do
    setup :log_in_user_with_source

    test "render backends successfully", %{conn: conn, user: user, source: source} do
      backend = insert(:backend, sources: [source], user: user)
      {:ok, view, _html} = live(conn, ~p"/backends")

      html = render(view)
      assert html =~ "rules: 0"
      assert html =~ backend.name
      assert html =~ Atom.to_string(backend.type)
    end

    test "render backends with metadata", %{conn: conn, user: user, source: source} do
      insert(:backend, sources: [source], user: user, metadata: %{some: "custom-metadata"})
      {:ok, view, _html} = live(conn, ~p"/backends")

      html = render(view)
      assert html =~ "some: custom-metadata"
    end

    test "able to view number of source rules attached", %{
      conn: conn,
      user: user,
      source: source
    } do
      rule = insert(:rule, source: source)
      backend = insert(:backend, rules: [rule], user: user)
      {:ok, view, _html} = live(conn, ~p"/backends")

      assert render(view) =~ "rules: 1"

      html =
        view
        |> element("a", backend.name)
        |> render_click()

      assert html =~ rule.lql_string
      assert html =~ rule.source.name
    end

    test "bug: string user_id on session for team users", %{conn: conn, user: user} do
      conn = put_session(conn, :user_id, inspect(user.id))
      assert {:ok, _view, _html} = live(conn, ~p"/backends")
    end
  end

  describe "show" do
    setup :log_in_user_with_source

    test "render backend details", %{conn: conn, user: user, source: source} do
      backend = insert(:backend, sources: [source], user: user)
      {:ok, view, _html} = live(conn, ~p"/backends/#{backend.id}")

      html = render(view)
      assert html =~ backend.name
      assert html =~ "#{backend.type}"
    end

    test "redacts certain config attributes from display", %{
      conn: conn,
      user: user,
      source: source
    } do
      backend = insert(:backend, sources: [source], user: user)
      {:ok, view, _html} = live(conn, ~p"/backends/#{backend.id}")
      html = render(view)
      assert html =~ "&quot;dataset_id&quot;: &quot;**********&quot;"
    end

    test "render backend with metadata", %{conn: conn, user: user, source: source} do
      backend =
        insert(:backend, sources: [source], user: user, metadata: %{some: "custom-metadata"})

      {:ok, view, _html} = live(conn, ~p"/backends/#{backend.id}")

      html = render(view)
      assert html =~ "some: custom-metadata"
    end

    test "add/delete a rule", %{
      conn: conn,
      user: user,
      source: source
    } do
      backend = insert(:backend, rules: [], user: user)
      {:ok, view, _html} = live(conn, ~p"/backends/#{backend.id}")

      assert view
             |> element("button", "Add a drain rule")
             |> render_click() =~ "Source"

      html =
        view
        |> element("form#rule")
        |> render_submit(%{
          rule: %{
            lql_string: "my:value",
            source_id: source.id,
            backend_id: backend.id
          }
        })

      assert html =~ "Successfully created rule for #{backend.name}"
      assert html =~ "my:value"
      assert html =~ source.name

      html =
        view
        |> element("ul li button", "Delete rule")
        |> render_click()

      refute html =~ "my:value"
      refute html =~ source.name
    end

    test "add/delete an alert", %{conn: conn, user: user} do
      alert_query = insert(:alert, user: user)
      backend = insert(:backend, user: user, type: :incidentio)
      {:ok, view, _html} = live(conn, ~p"/backends/#{backend.id}")

      view
      |> element("button", "Add an alert")
      |> render_click()

      assert view
             |> element("form#alert")
             |> render_submit(%{
               alert: %{
                 alert_id: alert_query.id
               }
             }) =~
               "Alert successfully added"

      assert view
             |> element("button", "Remove alert")
             |> render_click() =~ "Alert successfully removed from backend"

      refute render(view) =~ alert_query.name
    end
  end

  describe "new" do
    setup %{conn: conn} do
      user = insert(:user)

      %{conn: login_user(conn, user), user: user}
    end

    test "can create a new backend", %{conn: conn} do
      {:ok, view, _html} = live(conn, ~p"/backends")

      assert view
             |> element("a", "New backend")
             |> render_click() =~ ~r/\/new/

      assert view
             |> element("select#type")
             |> render_change(%{backend: %{type: "webhook"}}) =~ "Websocket URL"

      assert view
             |> form("form", %{
               backend: %{
                 name: "my webhook",
                 type: "webhook",
                 config: %{
                   url: "http://localhost:1234"
                 }
               }
             })
             |> render_submit() =~ "localhost"

      refute render(view) =~ "URL"

      assert render(view) =~ "my webhook"
      assert render(view) =~ "Successfully created backend"
    end

    test "bug: can create a new incidentio backend with metadata", %{conn: conn} do
      {:ok, view, _html} = live(conn, ~p"/backends")

      assert view
             |> element("a", "New backend")
             |> render_click() =~ ~r/\/new/

      assert view
             |> element("select#type")
             |> render_change(%{backend: %{type: "incidentio"}}) =~
               "Alert Source Configuration ID"

      assert view
             |> form("form", %{
               backend: %{
                 name: "my incidentio",
                 type: "incidentio",
                 config: %{
                   api_token: "http://localhost:1234",
                   alert_source_config_id: "123",
                   metadata: "team=example"
                 }
               }
             })
             |> render_submit()

      assert render(view) =~ "Successfully created backend"
    end

    test "on backend type switch, will change the inputs", %{conn: conn} do
      {:ok, view, _html} = live(conn, ~p"/backends/new")

      refute render(view) =~ "Postgres URL"

      assert view
             |> element("select#type")
             |> render_change(%{backend: %{type: "postgres"}}) =~ "Postgres URL"

      refute has_element?(view, "#datadog-region option", "US1-FED")

      view
      |> element("select#type")
      |> render_change(%{backend: %{type: "datadog"}})

      for region <- ["US1", "US3", "US5", "EU", "AP1", "US1-FED"] do
        assert has_element?(view, "#datadog-region option", region)
      end
    end

    test "change type will change inputs", %{conn: conn} do
      assert {:ok, view, _html} = live(conn, ~p"/backends/new")

      assert view
             |> element("select#type")
             |> render_change(%{backend: %{type: :postgres}}) =~ "Username"

      refute render(view) =~ "Project ID"

      assert view
             |> element("select#type")
             |> render_change(%{backend: %{type: :bigquery}}) =~ "Project ID"

      refute render(view) =~ "Username"
    end

    test "cancel will nav back to index", %{conn: conn} do
      assert {:ok, view, _html} = live(conn, ~p"/backends/new")

      view
      |> element("a", "Cancel")
      |> render_click()

      assert_redirect(view, ~p"/backends")
    end
  end

  describe "edit" do
    setup :log_in_user_with_source

    test "can edit backend", %{conn: conn, user: user, source: source} do
      backend = insert(:backend, sources: [source], user: user, type: :webhook)
      {:ok, view, _html} = live(conn, ~p"/backends/#{backend.id}/edit")

      assert view |> element("label", "Name") |> has_element?()

      html =
        view
        |> form("form", %{
          backend: %{
            description: "some description",
            name: "some other name",
            config: %{
              url: "https://some-other-url.com"
            }
          }
        })
        |> render_submit()

      assert html =~ "some-other-url.com"
      assert html =~ "some other name"
      assert html =~ "some description"
    end

    test "will show correct config inputs", %{conn: conn} do
      backend = insert(:backend, type: :webhook)
      assert {:ok, view, _html} = live(conn, ~p"/backends/#{backend.id}/edit")

      assert render(view) =~ "URL"

      refute view |> element("select#type") |> has_element?()
    end

    test "cancel will nav back to show", %{conn: conn} do
      backend = insert(:backend, type: :webhook)
      assert {:ok, view, _html} = live(conn, ~p"/backends/#{backend.id}/edit")

      view
      |> element("a", "Cancel")
      |> render_click()

      assert_redirect(view, ~p"/backends")
    end

    test "successful delete backend", %{conn: conn, user: user} do
      backend =
        insert(:backend,
          sources: [],
          user: user,
          name: "my webhook",
          type: "webhook",
          config: %{
            url: "http://localhost:1234"
          }
        )

      {:ok, view, _html} = live(conn, ~p"/backends/#{backend.id}/edit")

      refute view
             |> element("button", "Delete")
             |> render_click() =~ "localhost"

      assert_patched(view, ~p"/backends")
      refute render(view) =~ "my webhook"
      assert render(view) =~ "Successfully deleted backend"
    end

    test "error on deleting a backend with attached sources", %{
      conn: conn,
      user: user,
      source: source
    } do
      backend = insert(:backend, sources: [source], user: user)
      {:ok, view, _html} = live(conn, ~p"/backends/#{backend.id}/edit")

      assert view
             |> element("button", "Delete")
             |> render_click() =~ backend.name

      assert view
             |> render() =~ "There are still sources connected to this backend"
    end
  end

  describe "default ingest" do
    setup :log_in_user_with_source

    test "shows default ingest section for supported backends", %{
      conn: conn,
      user: user,
      source: _source
    } do
      backend =
        insert(:backend,
          user: user,
          type: :clickhouse,
          config: %{
            url: "http://localhost",
            database: "test",
            port: 8123
          }
        )

      {:ok, view, _html} = live(conn, ~p"/backends/#{backend.id}")

      html = render(view)
      assert html =~ "Ingest Configuration"
      assert html =~ "Add a Source"
    end

    test "does not show default ingest section for unsupported backends", %{
      conn: conn,
      user: user
    } do
      backend = insert(:backend, user: user, type: :webhook, config: %{url: "http://test.com"})
      {:ok, view, _html} = live(conn, ~p"/backends/#{backend.id}")

      html = render(view)
      refute html =~ "Ingest Configuration"
      refute html =~ "Add a Source"
    end

    test "toggle form shows source selection", %{conn: conn, user: user, source: source} do
      backend =
        insert(:backend,
          user: user,
          type: :clickhouse,
          config: %{
            url: "http://localhost",
            database: "test",
            port: 8123
          }
        )

      {:ok, view, _html} = live(conn, ~p"/backends/#{backend.id}")

      assert view
             |> element("button", "Add a Source")
             |> render_click() =~ "Source"

      html = render(view)
      assert html =~ source.name
      assert html =~ "Choose a source"
    end

    test "add source as default ingest", %{conn: conn, user: user, source: source} do
      backend =
        insert(:backend,
          user: user,
          type: :clickhouse,
          config: %{
            url: "http://localhost",
            database: "test",
            port: 8123
          }
        )

      {:ok, view, _html} = live(conn, ~p"/backends/#{backend.id}")

      view
      |> element("button", "Add a Source")
      |> render_click()

      html =
        view
        |> element("form#default_ingest")
        |> render_submit(%{
          default_ingest: %{
            source_id: source.id,
            backend_id: backend.id
          }
        })

      assert html =~ "Successfully added source to backend"
      assert html =~ source.name
      assert html =~ "sends logs to this backend"
    end

    test "remove source from default ingest", %{conn: conn, user: user, source: source} do
      backend =
        insert(:backend,
          user: user,
          type: :clickhouse,
          config: %{url: "http://localhost", database: "test", port: 8123},
          default_ingest?: false
        )

      {:ok, _} = Logflare.Backends.update_source_backends(source, [backend])
      {:ok, view, _html} = live(conn, ~p"/backends/#{backend.id}")

      html = render(view)
      assert html =~ source.name
      assert html =~ "sends logs to this backend"

      html =
        view
        |> element("button", "Remove")
        |> render_click()

      assert html =~ "Removed source from backend"
      refute html =~ "sends logs to this backend"
    end

    test "shows all user sources in dropdown", %{
      conn: conn,
      user: user,
      source: source
    } do
      {:ok, _source1} =
        Sources.update_source(source, %{
          name: "Source 1"
        })

      _source2 =
        insert(:source,
          user: user,
          name: "Source 2"
        )

      backend =
        insert(:backend,
          user: user,
          type: :clickhouse,
          config: %{
            url: "http://localhost",
            database: "test",
            port: 8123
          }
        )

      {:ok, view, _html} = live(conn, ~p"/backends/#{backend.id}")

      view
      |> element("button", "Add a Source")
      |> render_click()

      html = render(view)

      assert html =~ "Source 1"
      assert html =~ "Source 2"
    end

    test "hides 'Add a Source' button when default_ingest is enabled", %{
      conn: conn,
      user: user,
      source: _source
    } do
      backend =
        insert(:backend,
          user: user,
          type: :clickhouse,
          config: %{url: "http://localhost", database: "test", port: 8123},
          default_ingest?: true
        )

      {:ok, view, _html} = live(conn, ~p"/backends/#{backend.id}")

      html = render(view)

      refute html =~ "Add a Source"
      assert html =~ "Ingest from all sources"

      assert html =~
               "This backend is configured to receive logs from ALL your sources automatically"
    end

    test "can add multiple sources as default ingest", %{conn: conn, user: user, source: source} do
      source1 = source
      source2 = insert(:source, user: user)

      backend =
        insert(:backend,
          user: user,
          type: :clickhouse,
          config: %{url: "http://localhost", database: "test", port: 8123}
        )

      {:ok, view, _html} = live(conn, ~p"/backends/#{backend.id}")

      view |> element("button", "Add a Source") |> render_click()

      view
      |> element("form#default_ingest")
      |> render_submit(%{
        default_ingest: %{source_id: source1.id, backend_id: backend.id}
      })

      html = render(view)
      assert html =~ source1.name
      assert html =~ "sends logs to this backend"

      assert html =~ "Add a Source"

      view |> element("button", "Add a Source") |> render_click()

      html =
        view
        |> element("form#default_ingest")
        |> render_submit(%{
          default_ingest: %{source_id: source2.id, backend_id: backend.id}
        })

      assert html =~ source1.name
      assert html =~ source2.name
      assert html =~ "sends logs to this backend"
      assert length(String.split(html, "sends logs to this backend")) == 3

      # Now all sources are used, button should be hidden
      refute html =~ "Add a Source"
    end

    test "button disappears when all available sources are added", %{
      conn: conn,
      user: user,
      source: source
    } do
      backend =
        insert(:backend,
          user: user,
          type: :clickhouse,
          config: %{url: "http://localhost", database: "test", port: 8123}
        )

      {:ok, view, _html} = live(conn, ~p"/backends/#{backend.id}")

      html = render(view)
      assert html =~ "Add a Source"

      view |> element("button", "Add a Source") |> render_click()

      html =
        view
        |> element("form#default_ingest")
        |> render_submit(%{
          default_ingest: %{source_id: source.id, backend_id: backend.id}
        })

      refute html =~ "Add a Source"
      assert html =~ source.name
      assert html =~ "sends logs to this backend"
    end
  end

  test "redirects to login page when not logged in", %{conn: conn} do
    assert conn
           |> get(~p"/backends")
           |> redirected_to(302) == ~p"/auth/login"
  end
end
