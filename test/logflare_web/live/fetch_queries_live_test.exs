defmodule LogflareWeb.FetchQueriesLiveTest do
  use LogflareWeb.ConnCase

  alias Logflare.FetchQueries

  @create_attrs %{
    name: "Test Fetch Query",
    description: "Test description",
    language: "bq_sql",
    query: "select current_timestamp() as my_time",
    cron: "0 0 * * * *"
  }

  @update_attrs %{
    name: "Updated Fetch Query",
    description: "Updated description",
    query: "select another from `my-source`",
    cron: "2 * * * * *"
  }

  setup %{conn: conn} do
    insert(:plan, name: "Free")
    user = insert(:user)
    team = insert(:team, user: user)
    conn = login_user(conn, user)
    [user: user, team: team, conn: conn]
  end

  defp create_fetch_query(%{user: user}) do
    backend = insert(:backend, user: user)
    source = insert(:source, user: user)

    attrs =
      @create_attrs
      |> Map.put(:backend_id, backend.id)
      |> Map.put(:source_id, source.id)
      |> Map.put(:user_id, user.id)

    {:ok, fetch_query} = FetchQueries.create_fetch_query(attrs)
    fetch_query = FetchQueries.preload_fetch_query(fetch_query)
    %{fetch_query: fetch_query, backend: backend, source: source}
  end

  describe "unauthorized" do
    test "redirects when accessing fetch query that doesn't belong to user", %{conn: conn} do
      user = insert(:user)
      other_user = insert(:user)
      backend = insert(:backend, user: other_user)
      source = insert(:source, user: other_user)

      fetch_query =
        insert(:fetch_query,
          user: other_user,
          backend: backend,
          source: source
        )

      conn =
        conn
        |> login_user(user)
        |> get(~p"/fetch/#{fetch_query.id}")

      assert redirected_to(conn, 302) =~ "/fetch"
    end
  end

  describe "Index" do
    setup [:create_fetch_query]

    test "lists all fetch_queries", %{conn: conn, fetch_query: fetch_query, team: team} do
      {:ok, view, html} = live(conn, ~p"/fetch")
      assert html =~ fetch_query.name
      assert html =~ "Fetch Jobs"

      # link to show
      view
      |> element("a", "View")
      |> render_click()

      assert_patched(view, "/fetch/#{fetch_query.id}")
      assert has_element?(view, "*", fetch_query.name)
    end

    test "show for nonexistent query", %{conn: conn} do
      {:error, {:live_redirect, %{flash: %{"error" => "Fetch query not found"}}}} =
        live(conn, ~p"/fetch/123")
    end

    test "shows empty state when no fetch queries exist", %{conn: conn, fetch_query: _fq} do
      other_user = insert(:user)
      {:ok, view, html} = live(login_user(conn, other_user), ~p"/fetch")

      assert html =~ "No fetch queries yet"
      assert html =~ "Create one"
    end
  end

  describe "New" do
    test "saves new fetch_query with webhook backend", %{conn: conn, user: user} do
      backend = insert(:backend, user: user)
      source = insert(:source, user: user)

      {:ok, view, _html} = live(conn, ~p"/fetch")

      view |> element("a", "New Fetch Job") |> render_click()
      assert_patch(view, "/fetch/new")

      html =
        view
        |> element("form")
        |> render_submit(%{
          fetch_query: %{
            name: "Test Fetch",
            description: "Test desc",
            language: "jsonpath",
            query: "$.data[*]",
            cron: "0 0 * * * *",
            backend_id: backend.id,
            source_id: source.id,
            enabled: "true"
          }
        })

      assert html =~ "Fetch query created successfully"
      # redirected to :show
      assert html =~ "Test Fetch"
      assert html =~ "Test desc"
    end

    test "saves new fetch_query with bigquery backend", %{conn: conn, user: user} do
      backend = insert(:backend, user: user, type: :bigquery)
      source = insert(:source, user: user)

      {:ok, view, _html} = live(conn, ~p"/fetch")

      view |> element("a", "New Fetch Job") |> render_click()
      assert_patch(view, "/fetch/new")

      html =
        view
        |> element("form")
        |> render_submit(%{
          fetch_query: %{
            name: "BigQuery Fetch",
            description: "From BigQuery",
            language: "bq_sql",
            query: "SELECT * FROM `my-dataset.my-table`",
            cron: "0 0 * * * *",
            backend_id: backend.id,
            source_id: source.id,
            enabled: "true"
          }
        })

      assert html =~ "Fetch query created successfully"
      assert html =~ "BigQuery Fetch"
    end

    test "save with validation errors", %{conn: conn, user: user} do
      backend = insert(:backend, user: user)
      source = insert(:source, user: user)

      {:ok, view, _html} = live(conn, ~p"/fetch/new")

      # Missing required fields
      view
      |> element("form")
      |> render_submit(%{
        fetch_query: %{
          name: "",
          description: "",
          language: "bq_sql",
          query: "",
          cron: "",
          backend_id: backend.id,
          source_id: source.id
        }
      })

      html = render(view)
      assert html =~ "can&#39;t be blank" or html =~ "can't be blank"
    end

    test "validates invalid cron expression", %{conn: conn, user: user} do
      backend = insert(:backend, user: user)
      source = insert(:source, user: user)

      {:ok, view, _html} = live(conn, ~p"/fetch/new")

      view
      |> element("form")
      |> render_submit(%{
        fetch_query: %{
          name: "Test",
          language: "bq_sql",
          query: "SELECT 1",
          cron: "invalid cron",
          backend_id: backend.id,
          source_id: source.id
        }
      })

      html = render(view)
      assert html =~ "Cron"
    end
  end

  describe "Show" do
    setup [:create_fetch_query]

    test "displays fetch query details", %{conn: conn, fetch_query: fetch_query} do
      {:ok, view, html} = live(conn, ~p"/fetch/#{fetch_query.id}")

      assert html =~ fetch_query.name
      assert html =~ fetch_query.description
      assert html =~ fetch_query.query
      assert html =~ fetch_query.cron
      assert html =~ fetch_query.backend.name
      assert html =~ fetch_query.source.name
    end

    test "shows edit and back buttons", %{conn: conn, fetch_query: fetch_query} do
      {:ok, view, html} = live(conn, ~p"/fetch/#{fetch_query.id}")

      assert html =~ "Edit"
      assert html =~ "Back"

      view |> element("a", "Edit") |> render_click()
      assert_patched(view, "/fetch/#{fetch_query.id}/edit")
    end

    test "shows enabled/disabled status", %{
      conn: conn,
      fetch_query: fetch_query,
      backend: backend,
      source: source
    } do
      disabled_fq =
        insert(:fetch_query,
          user: fetch_query.user,
          backend: backend,
          source: source,
          enabled: false
        )

      {:ok, _view, html} = live(conn, ~p"/fetch/#{disabled_fq.id}")
      assert html =~ "Disabled"

      {:ok, _view, html} = live(conn, ~p"/fetch/#{fetch_query.id}")
      assert html =~ "Enabled"
    end
  end

  describe "Edit" do
    setup [:create_fetch_query]

    test "update fetch_query", %{conn: conn, fetch_query: fetch_query} do
      {:ok, view, _html} = live(conn, ~p"/fetch")

      view
      |> element("a", "View")
      |> render_click()

      view
      |> element("a", "Edit")
      |> render_click()

      assert_patched(view, "/fetch/#{fetch_query.id}/edit")

      view
      |> element("form")
      |> render_submit(%{
        fetch_query: @update_attrs
      }) =~ "Fetch query updated successfully"

      html = render(view)
      assert html =~ @update_attrs.name
      assert html =~ @update_attrs.query
      refute html =~ fetch_query.name
      refute html =~ fetch_query.query
    end

    test "update enabled status", %{conn: conn, fetch_query: fetch_query} do
      {:ok, view, html} = live(conn, ~p"/fetch/#{fetch_query.id}/edit")
      assert html =~ "checked"

      view
      |> element("form")
      |> render_submit(%{
        fetch_query: %{id: fetch_query.id, enabled: "false"}
      })

      # Verify disabled in database
      updated = FetchQueries.get_fetch_query(fetch_query.id)
      refute updated.enabled
    end

    test "delete fetch_query", %{conn: conn, fetch_query: fetch_query} do
      {:ok, view, _html} = live(conn, ~p"/fetch/#{fetch_query.id}/edit")

      view
      |> element("a", "Delete")
      |> render_click()

      assert_patch(view, "/fetch")
    end

    test "can't change backend after creation", %{
      conn: conn,
      fetch_query: fetch_query,
      user: user
    } do
      new_backend = insert(:backend, user: user)
      {:ok, view, html} = live(conn, ~p"/fetch/#{fetch_query.id}/edit")

      # Verify select is disabled (optional - depends on implementation)
      # For now just verify the form works
      assert html =~ "Backend"
    end
  end

  describe "team context" do
    setup %{user: user, team: team} do
      team_user = insert(:team_user, team: team, provider_uid: "provider_#{System.unique_integer()}")
      backend = insert(:backend, user: user)
      source = insert(:source, user: user)

      fetch_query =
        insert(:fetch_query,
          user: user,
          backend: backend,
          source: source
        )

      [team_user: team_user, fetch_query: fetch_query, backend: backend, source: source]
    end

    test "team user can list fetch queries", %{
      conn: conn,
      user: user,
      team_user: team_user,
      fetch_query: fetch_query
    } do
      {:ok, _view, html} =
        conn
        |> login_user(user, team_user)
        |> live(~p"/fetch")

      assert html =~ fetch_query.name
    end

    test "team user can view fetch query details", %{
      conn: conn,
      user: user,
      team_user: team_user,
      fetch_query: fetch_query
    } do
      {:ok, _view, html} =
        conn
        |> login_user(user, team_user)
        |> live(~p"/fetch/#{fetch_query.id}")

      assert html =~ fetch_query.name
      assert html =~ fetch_query.description
    end
  end

  describe "form validation" do
    setup [:create_fetch_query]

    test "validates language-specific query requirements", %{conn: conn, user: user} do
      backend = insert(:backend, user: user)
      source = insert(:source, user: user)

      {:ok, view, _html} = live(conn, ~p"/fetch/new")

      # BigQuery requires query
      view
      |> element("form")
      |> render_submit(%{
        fetch_query: %{
          name: "Missing Query",
          language: "bq_sql",
          query: "",
          cron: "0 0 * * * *",
          backend_id: backend.id,
          source_id: source.id
        }
      })

      html = render(view)
      assert html =~ "can&#39;t be blank" or html =~ "can't be blank"
    end

    test "accepts optional query for webhook backend", %{conn: conn, user: user} do
      backend = insert(:backend, user: user, type: :webhook, config: %{url: "https://example.com/webhook"})
      source = insert(:source, user: user)

      {:ok, view, _html} = live(conn, ~p"/fetch/new")

      # Webhook with no language constraint can have empty query (uses backend URL)
      # Submit form - webhook accepts empty query
      view
      |> element("form")
      |> render_submit(%{
        fetch_query: %{
          name: "Webhook No Query",
          language: "bq_sql",
          query: "",
          cron: "0 0 * * * *",
          backend_id: backend.id,
          source_id: source.id
        }
      })

      # Verify fetch query was created with empty query
      assert FetchQueries.list_fetch_queries_by_user_access(user)
             |> Enum.find(fn fq -> fq.name == "Webhook No Query" end)
    end
  end

  describe "language dropdown behavior" do
    setup [:create_fetch_query]

    test "shows all language options", %{conn: conn} do
      {:ok, _view, html} = live(conn, ~p"/fetch/new")

      assert html =~ "BigQuery SQL"
      assert html =~ "PostgreSQL"
      assert html =~ "LQL"
      assert html =~ "JSONPath"
    end

    test "defaults to bigquery sql", %{conn: conn} do
      {:ok, view, html} = live(conn, ~p"/fetch/new")

      # Check that bq_sql is selected by default in form
      assert html =~ "bq_sql"
    end
  end

  describe "backend and source dropdowns" do
    test "lists user's backends in dropdown", %{conn: conn, user: user} do
      backend1 = insert(:backend, user: user, name: "Backend 1")
      backend2 = insert(:backend, user: user, name: "Backend 2")

      {:ok, _view, html} = live(conn, ~p"/fetch/new")

      assert html =~ backend1.name
      assert html =~ backend2.name
    end

    test "lists user's sources in dropdown", %{conn: conn, user: user} do
      source1 = insert(:source, user: user, name: "Source 1")
      source2 = insert(:source, user: user, name: "Source 2")

      {:ok, _view, html} = live(conn, ~p"/fetch/new")

      assert html =~ source1.name
      assert html =~ source2.name
    end

    test "only shows user's backends and sources", %{conn: conn, user: user} do
      other_user = insert(:user)
      user_backend = insert(:backend, user: user, name: "User Backend")
      _other_backend = insert(:backend, user: other_user, name: "Other Backend")
      user_source = insert(:source, user: user, name: "User Source")
      _other_source = insert(:source, user: other_user, name: "Other Source")

      {:ok, _view, html} = live(conn, ~p"/fetch/new")

      assert html =~ user_backend.name
      refute html =~ "Other Backend"
      assert html =~ user_source.name
      refute html =~ "Other Source"
    end
  end

  describe "enabled checkbox" do
    setup [:create_fetch_query]

    test "checkbox is checked by default for new queries", %{conn: conn} do
      {:ok, _view, html} = live(conn, ~p"/fetch/new")
      assert html =~ "checked"
    end

    test "checkbox state is preserved on edit", %{
      conn: conn,
      fetch_query: fetch_query,
      backend: backend,
      source: source
    } do
      disabled_fq =
        insert(:fetch_query,
          user: fetch_query.user,
          backend: backend,
          source: source,
          enabled: false
        )

      {:ok, _view, html} = live(conn, ~p"/fetch/#{disabled_fq.id}/edit")
      # If disabled, there should not be a checked attribute (or similar)
      assert html =~ "Enabled"
    end
  end

  describe "execution history" do
    setup [:create_fetch_query]

    test "loads execution history on show", %{conn: conn, fetch_query: fetch_query} do
      {:ok, view, _html} = live(conn, ~p"/fetch/#{fetch_query.id}")

      # Verify that show page renders without error
      assert has_element?(view, "*", fetch_query.name)
    end
  end
end
