defmodule LogflareWeb.EndpointsLiveTest do
  @moduledoc false

  use LogflareWeb.ConnCase

  setup %{conn: conn} do
    insert(:plan)
    user = insert(:user)
    team = insert(:team, user: user)
    conn = login_user(conn, user)
    {:ok, user: user, team: team, conn: conn}
  end

  describe "with existing endpoint" do
    setup %{user: user} do
      {:ok, endpoint: insert(:endpoint, user: user)}
    end

    test "list endpoints", %{conn: conn, endpoint: endpoint, team: team} do
      {:ok, view, _html} = live(conn, "/endpoints")

      # intro message and link to docs
      assert has_element?(view, "p", "are GET JSON API endpoints")
      # link to docs
      assert has_element?(view, ".subhead a", "docs")
      # link to access tokens
      assert has_element?(view, ".subhead a", "access tokens")

      # description
      assert has_element?(view, "ul li p", endpoint.description)
      assert has_element?(view, "ul li *[title='Auth enabled']")

      # link to show
      view
      |> element("ul li a", endpoint.name)
      |> render_click()

      assert_patched(view, "/endpoints/#{endpoint.id}?t=#{team.id}")
      assert has_element?(view, "code", endpoint.query)
    end

    test "show endpoint", %{conn: conn, endpoint: endpoint, team: team} do
      {:ok, view, _html} = live(conn, "/endpoints/#{endpoint.id}")
      assert has_element?(view, "h1,h2,h3,h4,h5", endpoint.name)
      assert has_element?(view, "code", endpoint.query)
      assert has_element?(view, "p", endpoint.description)

      # link to edit
      assert element(view, ".subhead a", "edit") |> render_click() =~ "/edit"
      assert_patched(view, "/endpoints/#{endpoint.id}/edit?t=#{team.id}")
    end

    test "show endpoint -> edit endpoint", %{conn: conn, endpoint: endpoint} do
      {:ok, view, html} = live(conn, "/endpoints/#{endpoint.id}/edit")
      assert html =~ "/edit"
      assert has_element?(view, "h1,h2,h3,h4,h5", endpoint.name)

      new_query = "select current_timestamp() as my_time"

      view
      |> element("form#endpoint")
      |> render_submit(%{
        endpoint: %{
          query: new_query
        }
      })

      # show the endpoint
      assert has_element?(view, "code", new_query)
    end

    test "edit endpoint with redact_pii checkbox", %{conn: conn, endpoint: endpoint} do
      {:ok, view, _html} = live(conn, "/endpoints/#{endpoint.id}/edit")

      # Check that the redact_pii checkbox is present
      assert has_element?(view, "input[type=checkbox][name=\"endpoint[redact_pii]\"]")

      # Submit form with redact_pii enabled
      view
      |> element("form#endpoint")
      |> render_submit(%{
        endpoint: %{
          redact_pii: true
        }
      })

      assert render(view) =~ ~r/redact PII:.*enabled/
    end

    test "delete endpoint from edit", %{conn: conn, endpoint: endpoint} do
      {:ok, view, _html} = live(conn, ~p"/endpoints/#{endpoint.id}/edit")
      assert view |> element("button", "Delete") |> render_click() =~ "has been deleted"

      # link back to list, removed from endpoints list
      assert_patched(view, "/endpoints")
      refute has_element?(view, endpoint.name)

      {:ok, view, _html} = live(conn, "/endpoints/#{endpoint.id}")
      assert has_element?(view, "*", "Endpoint Not Found")
    end
  end

  test "index -> new endpoint", %{conn: conn, team: team} do
    {:ok, view, _html} = live(conn, "/endpoints")

    assert view
           |> element("a", "New endpoint")
           |> render_click() =~ ~r/\~\/.+endpoints.+\/new/

    assert_patch(view, "/endpoints/new?t=#{team.id}")
  end

  test "index - show cache count", %{conn: conn, user: user} do
    endpoint = insert(:endpoint, user: user)
    _pid = start_supervised!({Logflare.Endpoints.ResultsCache, {endpoint, %{}, []}})

    {:ok, view, _html} = live(conn, "/endpoints")

    assert render(view) =~ ~r/caches:.+1/
  end

  test "new endpoint", %{conn: conn, team: team} do
    {:ok, view, _html} = live(conn, "/endpoints/new")
    assert view |> has_element?("form#endpoint")

    new_query = "select current_timestamp() as my_time"

    view
    |> element("form#endpoint")
    |> render_submit(%{
      endpoint: %{
        description: "some description",
        name: "some query",
        query: new_query,
        language: "bq_sql"
      }
    }) =~ "created successfully"

    assert has_element?(view, "h1,h2,h3,h4,h5", "some query")
    assert has_element?(view, "code", new_query)
    path = assert_patch(view)
    assert path =~ ~r/\/endpoints\/\S+\?t=#{team.id}/
    assert render(view) =~ "some description"
  end

  test "new endpoint with redact_pii enabled", %{conn: conn} do
    {:ok, view, _html} = live(conn, "/endpoints/new")
    assert view |> has_element?("form#endpoint")

    new_query = "select 'test' as test"

    view
    |> element("form#endpoint")
    |> render_submit(%{
      endpoint: %{
        description: "some description with PII redaction",
        name: "some query with pii",
        query: new_query,
        redact_pii: true,
        language: "bq_sql"
      }
    }) =~ "created successfully"

    assert render(view) =~ ~r/redact PII:.*enabled/
  end

  describe "parse queries on change" do
    setup do
      [valid_query: "select current_timestamp() as my_time", invalid_query: "bad_query"]
    end

    test "new endpoint", %{
      conn: conn,
      valid_query: valid_query,
      invalid_query: invalid_query,
      team: team
    } do
      {:ok, view, _html} = live(conn, "/endpoints/new")

      # triggering event handler directly since Monaco does this via JavaScript
      assert view
             |> with_target("#endpoint_query_editor")
             |> render_hook("parse-query", %{"value" => invalid_query}) =~ "SQL Parse error!"

      refute view
             |> with_target("#endpoint_query_editor")
             |> render_hook("parse-query", %{"value" => valid_query}) =~ "SQL Parse error!"

      # Run form is updated with query and declared params
      assert view
             |> with_target("#endpoint_query_editor")
             |> render_hook("parse-query", %{"value" => valid_query <> " where id = @id"})

      assert view |> element(~s|input#run_query[value="#{valid_query}]"|)

      assert view |> render =~
               ~s|<input id="run_params_0_id" name="run[params][id]" type="text" value=""/>|

      # saves the change
      assert view
             |> element("form", "Save")
             |> render_submit(%{
               endpoint: %{
                 name: "new-endpoint",
                 query: "select @my_param as valid",
                 language: "bq_sql"
               }
             }) =~ "select @my_param as valid"

      path = assert_patch(view)
      assert path =~ ~r/\/endpoints\/\S+\?t=#{team.id}/
    end

    test "edit endpoint", %{conn: conn, user: user, team: team} do
      endpoint = insert(:endpoint, user: user, query: "select @other as initial")
      {:ok, view, _html} = live(conn, "/endpoints/#{endpoint.id}/edit")

      # saves the change
      assert view
             |> element("form#endpoint")
             |> render_submit(%{
               endpoint: %{
                 query: "select @my_param as valid"
               }
             })

      assert has_element?(view, "code", "select @my_param as valid")

      assert_patched(view, "/endpoints/#{endpoint.id}?t=#{team.id}")
      # no longer has the initail query string
      refute render(view) =~ endpoint.query
      refute render(view) =~ "@other"
      refute render(view) =~ "initial"
    end

    test "bug: edit query with validation errors do not affect saved values", %{
      conn: conn,
      user: user
    } do
      endpoint = insert(:endpoint, user: user, query: "select @other as initial")
      {:ok, view, _html} = live(conn, "/endpoints/#{endpoint.id}/edit")

      view
      |> change_editor_query("select @test as changed")

      assert view |> element("#endpoint_query") |> render() =~ "select @test as changed"

      view
      |> element("form#endpoint")
      |> render_change(%{
        endpoint: %{
          name: "changed"
        }
      })

      # should not reset the hidden input
      assert view |> element("#endpoint_query") |> render() =~ "select @test as changed"

      assert view
             |> form("#endpoint")
             |> render_submit(%{
               endpoint: %{
                 name: "changed",
                 query: "select @test as changed_again"
               }
             })

      assert render(view) =~ "select @test as changed_again"
      assert render(view) =~ "changed"
    end
  end

  test "show endpoint, auth disabled", %{conn: conn, user: user} do
    endpoint = insert(:endpoint, user: user, query: "select 'id' as id", enable_auth: false)
    {:ok, view, _html} = live(conn, "/endpoints/#{endpoint.id}")
    assert render(view) =~ "Authentication not enabled"
  end

  test "show endpoint, auth enabled", %{conn: conn, user: user} do
    endpoint = insert(:endpoint, user: user, query: "select 'id' as id", enable_auth: true)
    {:ok, view, _html} = live(conn, "/endpoints/#{endpoint.id}")
    refute render(view) =~ "Authentication not enabled"
  end

  describe "run queries" do
    setup do
      pid = self()

      GoogleApi.BigQuery.V2.Api.Jobs
      |> expect(:bigquery_jobs_query, 1, fn _conn, _proj_id, opts ->
        assert [body: %_{useQueryCache: false}] = opts

        send(
          pid,
          {:opts, %{useQueryCache: opts[:body].useQueryCache, labels: opts[:body].labels}}
        )

        {:ok, TestUtils.gen_bq_response([%{"testing" => "results-123"}])}
      end)

      :ok
    end

    test "new endpoint", %{conn: conn, team: team} do
      {:ok, view, _html} = live(conn, "/endpoints/new")

      refute render(view) =~ "results-123"

      view
      |> element("form", "Test query")
      |> render_submit(%{
        run: %{
          query: "select current_datetime() as new",
          params: %{}
        }
      }) =~ "results-123"

      assert_received {:opts,
                       %{
                         useQueryCache: false,
                         labels: %{"endpoint_id" => "nil", "managed_by" => "logflare"}
                       }}

      assert has_element?(view, "label", "Description")
      assert has_element?(view, "h5", "Caching")
      assert has_element?(view, "label", "Cache TTL")
      assert has_element?(view, "label", "Proactive Re-querying")
      assert has_element?(view, "label", "Enable query sandboxing")
      assert has_element?(view, "label", "Max limit")
      assert has_element?(view, "label", "Enable authentication")

      assert view |> render() =~ ~r/1 .+ processed/

      assert view
             |> element("form#endpoint")
             |> render_submit(%{
               endpoint: %{
                 name: "some name",
                 query: "select current_datetime() as saved"
               }
             }) =~ "saved"

      path = assert_patch(view)
      assert path =~ ~r/\/endpoints\/\S+\?t=#{team.id}/
    end

    test "edit endpoint", %{conn: conn, user: user} do
      endpoint = insert(:endpoint, user: user)
      {:ok, view, _html} = live(conn, "/endpoints/#{endpoint.id}/edit")
      refute render(view) =~ "results-123"

      view
      |> element("form", "Test query")
      |> render_submit(%{}) =~ "results-123"

      assert view
             |> element("form#endpoint")
             |> render_submit(%{
               endpoint: %{
                 description: "different description",
                 query: "select current_datetime() as updated"
               }
             }) =~ "updated"

      assert render(view) =~ "different description"
    end

    test "show endpoint, with params", %{conn: conn, user: user} do
      endpoint =
        insert(:endpoint,
          user: user,
          query: "select 'id' as id, @test_param as param;\n\n",
          labels: "session_id,test=@test_param"
        )

      {:ok, view, _html} = live(conn, "/endpoints/#{endpoint.id}")
      refute render(view) =~ "results-123"
      # sow declared params
      html = render(view)
      # assert html =~ "test_param"
      assert html =~ endpoint.token
      assert html =~ inspect(endpoint.max_limit)
      assert html =~ ~r/caching\:.+#{endpoint.cache_duration_seconds} seconds/
      assert html =~ ~r/cache warming\:.+ #{endpoint.proactive_requerying_seconds} seconds/
      assert html =~ ~r/query sandboxing\:.+ disabled/

      # test the query
      assert view
             |> element("form", "Test query")
             |> render_submit(%{
               run: %{
                 query: endpoint.query,
                 params: %{"test_param" => "my_param_value"}
               }
             }) =~ "results-123"

      assert has_element?(view, "input[value='my_param_value']")

      assert_received {:opts, %{labels: labels}}
      assert labels["endpoint_id"] == endpoint.id |> to_string()
    end
  end

  defp change_editor_query(view, query) do
    result =
      view
      |> with_target("#endpoint_query_editor")
      |> render_hook("parse-query", %{"value" => query})

    result
  end

  describe "backend selection" do
    setup tags do
      original_env = Application.get_env(:logflare, :env)
      original_overrides = Application.get_env(:logflare, :feature_flag_override)

      Application.put_env(:logflare, :env, tags[:env] || :test)

      if tags[:feature_overrides] do
        Application.put_env(:logflare, :feature_flag_override, tags[:feature_overrides])
      end

      on_exit(fn ->
        Application.put_env(:logflare, :env, original_env)

        if original_overrides do
          Application.put_env(:logflare, :feature_flag_override, original_overrides)
        else
          Application.delete_env(:logflare, :feature_flag_override)
        end
      end)

      :ok
    end

    setup %{user: user} do
      backend = insert(:backend, user: user, type: :clickhouse, name: "Test ClickHouse")
      {:ok, backend: backend}
    end

    test "shows backend selection when backends exist (flag enabled in test)", %{
      conn: conn,
      backend: _backend
    } do
      {:ok, view, _html} = live(conn, "/endpoints/new")

      html = render(view)
      assert html =~ "Backend (optional)"
      assert html =~ "(clickhouse)"
      assert html =~ "Default (BigQuery)"
    end

    @tag env: :prod, feature_overrides: %{"endpointBackendSelection" => "false"}
    test "hides backend selection when flag is disabled", %{conn: conn, backend: _backend} do
      {:ok, view, _html} = live(conn, "/endpoints/new")

      html = render(view)
      refute html =~ "Backend (optional)"
      refute html =~ "Test ClickHouse"
    end

    test "hides backend selection when flag is enabled but no backends exist", %{
      conn: conn,
      backend: backend
    } do
      Logflare.Repo.delete(backend)

      {:ok, view, _html} = live(conn, "/endpoints/new")

      html = render(view)
      refute html =~ "Backend (optional)"
    end

    test "language determination works correctly with backend selection", %{
      conn: conn,
      backend: backend
    } do
      {:ok, view, _html} = live(conn, "/endpoints/new")

      view
      |> element("form#endpoint")
      |> render_change(%{
        endpoint: %{
          backend_id: backend.id,
          name: "test endpoint",
          query: "SELECT 1"
        }
      })

      assert has_element?(view, "#query-language", "ClickHouse SQL")
    end
  end

  describe "PII redaction" do
    test "PII redaction only affects query results, not query display", %{conn: conn, user: user} do
      endpoint =
        insert(:endpoint,
          user: user,
          query: "select '192.168.1.1' as ip, 'User from 10.0.0.1' as message",
          redact_pii: true
        )

      {:ok, view, _html} = live(conn, "/endpoints/#{endpoint.id}")

      assert render(view) =~ ~r/redact PII:.*enabled/
      visible_code = view |> element("div.tw-w-full.tw-bg-zinc-800 code") |> render()
      assert visible_code =~ "192.168.1.1"
      assert visible_code =~ "10.0.0.1"

      if view |> has_element?("div.collapse code") do
        expanded_code = view |> element("div.collapse code") |> render()
        assert expanded_code =~ "REDACTED"
      end
    end

    test "PII redaction in query results", %{conn: conn, user: user} do
      endpoint =
        insert(:endpoint,
          user: user,
          query: "select 'test' as message",
          redact_pii: true
        )

      {:ok, view, _html} = live(conn, "/endpoints/#{endpoint.id}/edit")

      GoogleApi.BigQuery.V2.Api.Jobs
      |> expect(:bigquery_jobs_query, 1, fn _conn, _proj_id, _opts ->
        {:ok,
         TestUtils.gen_bq_response([%{"ip" => "192.168.1.1", "message" => "User from 10.0.0.1"}])}
      end)

      view
      |> element("form", "Test query")
      |> render_submit(%{run: %{query: endpoint.query, params: %{}}})

      assert render(view) =~ "REDACTED"
    end
  end

  test "saving endpoint clears test results", %{conn: conn, user: user} do
    endpoint = insert(:endpoint, user: user, query: "select 'test' as message")
    {:ok, view, _html} = live(conn, "/endpoints/#{endpoint.id}/edit")

    # Simulate having test results by setting up mock and running query
    GoogleApi.BigQuery.V2.Api.Jobs
    |> expect(:bigquery_jobs_query, 1, fn _conn, _proj_id, _opts ->
      {:ok, TestUtils.gen_bq_response([%{"testing" => "results-123"}])}
    end)

    view
    |> element("form", "Test query")
    |> render_submit(%{run: %{query: endpoint.query, params: %{}}})

    # Verify test results are shown
    assert render(view) =~ "results-123"

    # Save endpoint
    view |> element("form#endpoint") |> render_submit(%{endpoint: %{description: "updated"}})

    # Verify test results are cleared
    refute render(view) =~ "results-123"
  end

  test "navigating from show to edit clears test results", %{conn: conn, user: user} do
    endpoint = insert(:endpoint, user: user)
    {:ok, view, _html} = live(conn, "/endpoints/#{endpoint.id}")

    GoogleApi.BigQuery.V2.Api.Jobs
    |> expect(:bigquery_jobs_query, 1, fn _conn, _proj_id, _opts ->
      {:ok, TestUtils.gen_bq_response([%{"testing" => "results-123"}])}
    end)

    view
    |> element("form", "Test query")
    |> render_submit(%{run: %{query: endpoint.query, params: %{}}})

    assert render(view) =~ "results-123"

    view |> element(".subhead a", "edit") |> render_click()
    refute render(view) =~ "results-123"
  end

  describe "sandbox query testing UI" do
    setup %{user: user} do
      endpoint =
        insert(:endpoint,
          user: user,
          sandboxable: true,
          query: """
          WITH errors AS (
            SELECT 'test error' as err, 500 as code
          )
          SELECT err, code FROM errors
          """
        )

      {:ok, endpoint: endpoint}
    end

    test "shows sandbox query form when sandboxable is true", %{conn: conn, endpoint: endpoint} do
      {:ok, view, _html} = live(conn, "/endpoints/#{endpoint.id}")

      assert has_element?(view, "h4", "Test Sandbox Query")
      assert has_element?(view, "input[type=radio][value=sql]")
      assert has_element?(view, "input[type=radio][value=lql]")
      assert has_element?(view, "textarea[name='sandbox_form[sandbox_query]']")
      assert has_element?(view, "input[type=checkbox][name='sandbox_form[show_transformed]']")
      assert has_element?(view, "button", "Test Sandbox Query")
    end

    test "hides sandbox query form when sandboxable is false", %{conn: conn, user: user} do
      endpoint = insert(:endpoint, user: user, sandboxable: false)
      {:ok, view, _html} = live(conn, "/endpoints/#{endpoint.id}")

      refute has_element?(view, "h4", "Test Sandbox Query")
      refute has_element?(view, "textarea[name='sandbox_form[sandbox_query]']")
    end

    test "executes SQL sandbox query successfully", %{conn: conn, endpoint: endpoint} do
      {:ok, view, _html} = live(conn, "/endpoints/#{endpoint.id}")

      GoogleApi.BigQuery.V2.Api.Jobs
      |> expect(:bigquery_jobs_query, 1, fn _conn, _proj_id, _opts ->
        {:ok, TestUtils.gen_bq_response([%{"err" => "test error", "code" => "500"}])}
      end)

      view
      |> element("form", "Test Sandbox Query")
      |> render_submit(%{
        sandbox_form: %{
          query_mode: "sql",
          sandbox_query: "SELECT err, code FROM errors",
          params: %{},
          show_transformed: "false"
        }
      })

      assert render(view) =~ "Ran sandbox query successfully"
      assert has_element?(view, "h5", "Sandbox Query Results")
      assert render(view) =~ "test error"
      assert render(view) =~ "500"
    end

    test "executes LQL sandbox query successfully", %{conn: conn, endpoint: endpoint} do
      {:ok, view, _html} = live(conn, "/endpoints/#{endpoint.id}")

      GoogleApi.BigQuery.V2.Api.Jobs
      |> expect(:bigquery_jobs_query, 1, fn _conn, _proj_id, _opts ->
        {:ok, TestUtils.gen_bq_response([%{"err" => "test error"}])}
      end)

      view
      |> element("form", "Test Sandbox Query")
      |> render_submit(%{
        sandbox_form: %{
          query_mode: "lql",
          sandbox_query: "s:err",
          params: %{},
          show_transformed: "false"
        }
      })

      assert render(view) =~ "Ran sandbox query successfully"
      assert has_element?(view, "h5", "Sandbox Query Results")
      assert render(view) =~ "test error"
    end

    test "displays error for invalid table reference in sandbox query", %{
      conn: conn,
      endpoint: endpoint
    } do
      {:ok, view, _html} = live(conn, "/endpoints/#{endpoint.id}")

      view
      |> element("form", "Test Sandbox Query")
      |> render_submit(%{
        sandbox_form: %{
          query_mode: "sql",
          sandbox_query: "SELECT * FROM unauthorized_table",
          params: %{},
          show_transformed: "false"
        }
      })

      html = render(view)

      assert html =~ "Error occurred when running sandbox query"
      assert has_element?(view, ".alert-danger")
    end

    test "shows transformed query when checkbox is enabled", %{conn: conn, endpoint: endpoint} do
      {:ok, view, _html} = live(conn, "/endpoints/#{endpoint.id}")

      GoogleApi.BigQuery.V2.Api.Jobs
      |> expect(:bigquery_jobs_query, 1, fn _conn, _proj_id, _opts ->
        {:ok, TestUtils.gen_bq_response([%{"err" => "test error"}])}
      end)

      view
      |> element("form", "Test Sandbox Query")
      |> render_submit(%{
        sandbox_form: %{
          query_mode: "sql",
          sandbox_query: "SELECT err FROM errors",
          params: %{},
          show_transformed: "true"
        }
      })

      assert render(view) =~ "Ran sandbox query successfully"
      assert has_element?(view, "summary", "Show Transformed Query")
      assert render(view) =~ "WITH errors AS"
    end

    test "sandbox query works with parameters", %{conn: conn, user: user} do
      endpoint =
        insert(:endpoint,
          user: user,
          sandboxable: true,
          query: """
          WITH filtered AS (
            SELECT 'test' as value
          )
          SELECT value FROM filtered
          """
        )

      {:ok, view, _html} = live(conn, "/endpoints/#{endpoint.id}")

      GoogleApi.BigQuery.V2.Api.Jobs
      |> expect(:bigquery_jobs_query, 1, fn _conn, _proj_id, _opts ->
        {:ok, TestUtils.gen_bq_response([%{"value" => "test"}])}
      end)

      view
      |> element("form", "Test Sandbox Query")
      |> render_submit(%{
        sandbox_form: %{
          query_mode: "sql",
          sandbox_query: "SELECT value FROM filtered",
          params: %{},
          show_transformed: "false"
        }
      })

      assert render(view) =~ "Ran sandbox query successfully"
      assert render(view) =~ "test"
    end

    test "sandbox query displays query cost for BigQuery", %{conn: conn, endpoint: endpoint} do
      {:ok, view, _html} = live(conn, "/endpoints/#{endpoint.id}")

      GoogleApi.BigQuery.V2.Api.Jobs
      |> expect(:bigquery_jobs_query, 1, fn _conn, _proj_id, _opts ->
        response = TestUtils.gen_bq_response([%{"err" => "test"}])
        # Override totalBytesProcessed to a larger value for testing cost display
        {:ok, %{response | totalBytesProcessed: "1048576"}}
      end)

      view
      |> element("form", "Test Sandbox Query")
      |> render_submit(%{
        sandbox_form: %{
          query_mode: "sql",
          sandbox_query: "SELECT err FROM errors",
          params: %{},
          show_transformed: "false"
        }
      })

      assert render(view) =~ "Ran sandbox query successfully"
      assert render(view) =~ "processed"
    end

    test "sandbox query handles LQL parsing errors gracefully", %{conn: conn, endpoint: endpoint} do
      {:ok, view, _html} = live(conn, "/endpoints/#{endpoint.id}")

      # Submit invalid LQL that will fail parsing
      assert view
             |> element("form", "Test Sandbox Query")
             |> render_submit(%{
               sandbox_form: %{
                 query_mode: "lql",
                 sandbox_query: "m.invalid:field:with:colons",
                 params: %{},
                 show_transformed: "false"
               }
             })

      html = render(view)

      assert html =~ "Error occurred when running sandbox query" or
               has_element?(view, "h5", "Sandbox Query Error")
    end

    test "sandbox query section preserves query input on error", %{conn: conn, endpoint: endpoint} do
      {:ok, view, _html} = live(conn, "/endpoints/#{endpoint.id}")

      view
      |> element("form", "Test Sandbox Query")
      |> render_submit(%{
        sandbox_form: %{
          query_mode: "sql",
          sandbox_query: "SELECT * FROM invalid_table",
          params: %{},
          show_transformed: "false"
        }
      })

      assert render(view) =~ "Error occurred when running sandbox query"
      assert render(view) =~ "SELECT * FROM invalid_table"
    end

    test "sandbox query mode toggle shows SQL and LQL options", %{conn: conn, endpoint: endpoint} do
      {:ok, view, _html} = live(conn, "/endpoints/#{endpoint.id}")

      html = render(view)
      assert html =~ "Query Mode"
      assert has_element?(view, "input[type=radio][value=sql]")
      assert has_element?(view, "input[type=radio][value=lql]")
      assert has_element?(view, "label", "SQL")
      assert has_element?(view, "label", "LQL")
    end

    test "sandbox query UI shows help text about CTE restrictions", %{
      conn: conn,
      endpoint: endpoint
    } do
      {:ok, view, _html} = live(conn, "/endpoints/#{endpoint.id}")

      html = render(view)
      assert html =~ "Test how consumers can query your endpoint"
      assert html =~ "?sql="
      assert html =~ "?lql="
      assert html =~ "restricted to the CTE tables"
    end

    test "sandbox query errors do not expose Ecto query internals", %{
      conn: conn,
      user: user
    } do
      endpoint =
        insert(:endpoint,
          user: user,
          sandboxable: true,
          query: """
          WITH event_logs AS (
            SELECT timestamp, event_message FROM YourApp.SourceName
          )
          SELECT * FROM event_logs
          """
        )

      {:ok, view, _html} = live(conn, "/endpoints/#{endpoint.id}")

      view
      |> element("form", "Test Sandbox Query")
      |> render_submit(%{
        sandbox_form: %{
          query_mode: "lql",
          sandbox_query: "c:avg(timestamp)",
          params: %{},
          show_transformed: "false"
        }
      })

      # Should show error without exposing Ecto query internals
      assert has_element?(view, ".alert-danger") or
               has_element?(view, "h5", "Sandbox Query Error")

      html = render(view)
      refute html =~ "field(e0"
      refute html =~ "from e0 in"
      refute html =~ "%Ecto.Query{"
    end
  end

  describe "resolving team context" do
    setup %{user: user, team: team} do
      team_user = insert(:team_user, team: team)
      endpoint = insert(:endpoint, user: user)

      [user: user, team: team, team_user: team_user, endpoint: endpoint]
    end

    test "team user can list endpoints", %{
      conn: conn,
      user: user,
      team_user: team_user,
      endpoint: endpoint
    } do
      {:ok, _view, html} =
        conn
        |> login_user(user, team_user)
        |> live(~p"/endpoints?t=#{team_user.team_id}")

      assert html =~ endpoint.name
    end

    test "team user can view endpoint without t= param", %{
      conn: conn,
      user: user,
      team_user: team_user,
      endpoint: endpoint
    } do
      {:ok, _view, html} =
        conn
        |> login_user(user, team_user)
        |> live(~p"/endpoints/#{endpoint.id}")

      assert html =~ endpoint.name
    end

    test "endpoints links preserve team param", %{
      conn: conn,
      user: user,
      team_user: team_user,
      endpoint: endpoint
    } do
      {:ok, _view, html} =
        conn |> login_user(user, team_user) |> live(~p"/endpoints?t=#{team_user.team_id}")

      for path <- ["endpoints/new", "endpoints/#{endpoint.id}"] do
        assert html =~ ~r/#{path}[^"<]*t=#{team_user.team_id}/
      end
    end

    test "endpoint show links preserve team param", %{
      conn: conn,
      user: user,
      team_user: team_user,
      endpoint: endpoint
    } do
      {:ok, _view, html} =
        conn
        |> login_user(user, team_user)
        |> live(~p"/endpoints/#{endpoint}?t=#{team_user.team_id}")

      for path <- ["endpoints/#{endpoint.id}/edit", "access-tokens"] do
        assert html =~ ~r/#{path}[^"<]*t=#{team_user.team_id}/
      end
    end
  end

  describe "bigquery reservations field" do
    test "shows BigQuery reservations field for BigQuery endpoints", %{conn: conn} do
      {:ok, view, _html} = live(conn, "/endpoints/new")

      html = render(view)
      assert html =~ "BigQuery Reservations (optional)"
      assert html =~ "bigquery_reservations"
    end

    test "hides BigQuery reservations field for non-BigQuery backends", %{conn: conn, user: user} do
      backend = insert(:backend, user: user, type: :clickhouse, name: "Test ClickHouse")
      {:ok, view, _html} = live(conn, "/endpoints/new")

      # Select ClickHouse backend
      view
      |> element("form#endpoint")
      |> render_change(%{
        endpoint: %{
          backend_id: backend.id,
          name: "test endpoint",
          query: "SELECT 1"
        }
      })

      html = render(view)
      refute html =~ "BigQuery Reservations"
    end

    test "creates endpoint with BigQuery reservations and displays them on show page", %{
      conn: conn
    } do
      {:ok, view, _html} = live(conn, "/endpoints/new")

      reservations =
        "projects/123/locations/us/reservations/res1\nprojects/123/locations/us/reservations/res2"

      view
      |> element("form#endpoint")
      |> render_submit(%{
        endpoint: %{
          name: "endpoint-with-reservations",
          query: "select current_timestamp() as ts",
          language: "bq_sql",
          bigquery_reservations: reservations
        }
      })

      assert has_element?(view, "h1,h2,h3,h4,h5", "endpoint-with-reservations")

      # Verify the reservation was saved in database
      endpoint = Logflare.Endpoints.get_by(name: "endpoint-with-reservations")
      assert endpoint.bigquery_reservations == reservations

      # Verify reservations are displayed in the show view with whitespace preserved
      html = render(view)
      assert html =~ "BigQuery reservations:"
      assert html =~ "projects/123/locations/us/reservations/res1"
      assert html =~ "projects/123/locations/us/reservations/res2"
    end

    test "updates endpoint with BigQuery reservations and displays them on show page", %{
      conn: conn,
      user: user
    } do
      endpoint = insert(:endpoint, user: user, bigquery_reservations: nil)
      {:ok, view, _html} = live(conn, "/endpoints/#{endpoint.id}/edit")

      reservations =
        "projects/456/locations/eu/reservations/my-res-1\nprojects/456/locations/eu/reservations/my-res-2"

      view
      |> element("form#endpoint")
      |> render_submit(%{
        endpoint: %{
          bigquery_reservations: reservations
        }
      })

      # Verify the reservation was updated in database
      updated_endpoint = Logflare.Endpoints.get_endpoint_query(endpoint.id)
      assert updated_endpoint.bigquery_reservations == reservations

      # Verify reservations are displayed in the show view with whitespace preserved
      html = render(view)
      assert html =~ "BigQuery reservations:"
      assert html =~ "projects/456/locations/eu/reservations/my-res-1"
      assert html =~ "projects/456/locations/eu/reservations/my-res-2"
    end

    test "shows existing reservations when editing endpoint", %{conn: conn, user: user} do
      reservations = "projects/789/locations/us/reservations/existing-res"
      endpoint = insert(:endpoint, user: user, bigquery_reservations: reservations)

      {:ok, view, _html} = live(conn, "/endpoints/#{endpoint.id}/edit")

      html = render(view)
      assert html =~ "existing-res"
    end

    test "displays reservations on endpoint show page with whitespace preserved", %{
      conn: conn,
      user: user
    } do
      reservations =
        "projects/111/locations/us/reservations/first\nprojects/222/locations/eu/reservations/second\nprojects/333/locations/asia/reservations/third"

      endpoint = insert(:endpoint, user: user, bigquery_reservations: reservations)

      {:ok, view, _html} = live(conn, "/endpoints/#{endpoint.id}")

      html = render(view)

      # Verify all reservations are displayed
      assert html =~ "BigQuery reservations:"
      assert html =~ "projects/111/locations/us/reservations/first"
      assert html =~ "projects/222/locations/eu/reservations/second"
      assert html =~ "projects/333/locations/asia/reservations/third"
    end

    test "does not show reservations section when no reservations configured", %{
      conn: conn,
      user: user
    } do
      endpoint = insert(:endpoint, user: user, bigquery_reservations: nil)

      {:ok, view, _html} = live(conn, "/endpoints/#{endpoint.id}")

      html = render(view)
      refute html =~ "BigQuery reservations:"
    end

    test "does not show reservations section for non-BigQuery endpoints", %{
      conn: conn,
      user: user
    } do
      backend = insert(:backend, user: user, type: :clickhouse, name: "CH Backend")

      endpoint =
        insert(:endpoint,
          user: user,
          backend: backend,
          language: :ch_sql,
          bigquery_reservations: "projects/123/reservations/should-not-show"
        )

      {:ok, view, _html} = live(conn, "/endpoints/#{endpoint.id}")

      html = render(view)
      refute html =~ "BigQuery reservations:"
    end
  end
end
