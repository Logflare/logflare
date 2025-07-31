defmodule LogflareWeb.EndpointsLiveTest do
  @moduledoc false
  use LogflareWeb.ConnCase

  setup %{conn: conn} do
    insert(:plan)
    user = insert(:user)
    conn = login_user(conn, user)
    {:ok, user: user, conn: conn}
  end

  describe "with existing endpoint" do
    setup %{user: user} do
      {:ok, endpoint: insert(:endpoint, user: user)}
    end

    test "list endpoints", %{conn: conn, endpoint: endpoint} do
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

      assert_patched(view, "/endpoints/#{endpoint.id}")
      assert has_element?(view, "code", endpoint.query)
    end

    test "show endpoint", %{conn: conn, endpoint: endpoint} do
      {:ok, view, _html} = live(conn, "/endpoints/#{endpoint.id}")
      assert has_element?(view, "h1,h2,h3,h4,h5", endpoint.name)
      assert has_element?(view, "code", endpoint.query)
      assert has_element?(view, "p", endpoint.description)

      # link to edit
      assert element(view, ".subhead a", "edit") |> render_click() =~ "/edit"
      assert_patched(view, "/endpoints/#{endpoint.id}/edit")
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

  test "index -> new endpoint", %{conn: conn} do
    {:ok, view, _html} = live(conn, "/endpoints")

    assert view
           |> element("a", "New endpoint")
           |> render_click() =~ ~r/\~\/.+endpoints.+\/new/

    assert_patch(view, "/endpoints/new")
  end

  test "index - show cache count", %{conn: conn, user: user} do
    endpoint = insert(:endpoint, user: user)
    _pid = start_supervised!({Logflare.Endpoints.ResultsCache, {endpoint, %{}}})

    {:ok, view, _html} = live(conn, "/endpoints")

    assert render(view) =~ ~r/caches:.+1/
  end

  test "new endpoint", %{conn: conn} do
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
    assert path =~ ~r/\/endpoints\/\S+/
    assert render(view) =~ "some description"
  end

  describe "parse queries on change" do
    setup do
      [valid_query: "select current_timestamp() as my_time", invalid_query: "bad_query"]
    end

    test "new endpoint", %{conn: conn, valid_query: valid_query, invalid_query: invalid_query} do
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
      assert path =~ ~r/\/endpoints\/\S+/
    end

    test "edit endpoint", %{conn: conn, user: user} do
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

      assert_patched(view, "/endpoints/#{endpoint.id}")
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
        send(pid, {:labels, opts[:body].labels})
        {:ok, TestUtils.gen_bq_response([%{"testing" => "results-123"}])}
      end)

      :ok
    end

    test "new endpoint", %{conn: conn} do
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

      assert_received {:labels, %{"endpoint_id" => "nil", "managed_by" => "logflare"}}

      assert has_element?(view, "label", "Description")
      assert has_element?(view, "h5", "Caching")
      assert has_element?(view, "label", "Cache TTL")
      assert has_element?(view, "label", "Proactive Re-querying")
      assert has_element?(view, "label", "Enable query sandboxing")
      assert has_element?(view, "label", "Max limit")
      assert has_element?(view, "label", "Enable authentication")

      assert view |> render() =~ "1 byte processed"

      assert view
             |> element("form#endpoint")
             |> render_submit(%{
               endpoint: %{
                 name: "some name",
                 query: "select current_datetime() as saved"
               }
             }) =~ "saved"
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

      assert_received {:labels, labels = %{"test" => "my_param_value", "session_id" => "nil"}}
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
end
