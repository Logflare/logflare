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

      # link to show
      view
      |> element("ul li a", endpoint.name)
      |> render_click()

      assert_patched(view, "/endpoints/#{endpoint.id}")
      assert has_element?(view, "code", endpoint.query)
    end

    test "show endpoint", %{conn: conn, endpoint: endpoint} do
      {:ok, view, _html} = live(conn, "/endpoints/#{endpoint.id}")
      assert has_element?(view, "h1", endpoint.name)
      assert has_element?(view, "code", endpoint.query)

      # link to edit
      assert element(view, ".subhead a", "edit") |> render_click() =~ "Edit Endpoint"
      assert_patched(view, "/endpoints/#{endpoint.id}/edit")
    end

    test "show endpoint -> edit endpoint", %{conn: conn, endpoint: endpoint} do
      {:ok, view, html} = live(conn, "/endpoints/#{endpoint.id}/edit")
      assert html =~ "Edit Endpoint"
      assert has_element?(view, "h1", endpoint.name)

      new_query = "select current_timestamp() as my_time"

      render_submit("form#edit-endpoint", %{
        endpoint: %{
          query: new_query
        }
      })

      # show the endpoint
      assert has_element?(view, "code", new_query)
    end

    test "delete endpoint from show", %{conn: conn, endpoint: endpoint} do
      {:ok, view, _html} = live(conn, "/endpoints/#{endpoint.id}")
      assert view |> element(".subhead a", "delete") |> render_click() =~ "has been deleted"

      # link back to list, removed from endpoints list
      assert_patched(view, "/endpoints")
      refute has_element?(view, endpoint.name)

      {:ok, view, _html} = live(conn, "/endpoints/#{endpoint.id}")
      assert has_element?(view, "#not-found")
    end
  end

  test "new endpoint", %{conn: conn} do
    {:ok, view, _html} = live(conn, "/endpoints/new")
    render_hook(view, "new-endpoint")
    assert_patched(view, "/endpoints/new")
    assert view |> has_element?("#new-endpoint")

    # edit the endpoint
    new_query = "select current_timestamp() as my_time"

    render_hook(view, "save-endpoint", %{
      endpoint: %{
        name: "some query",
        query: new_query,
        language: "bq_sql"
      }
    })

    assert view |> element("#show-endpoint") |> render() =~ new_query

    # browser list should have new endpoint
    assert view |> element("#endpoints-browser-list") |> render() =~ "some query"
  end

  describe "parse queries on change" do
    test "new endpoint", %{conn: conn} do
      {:ok, view, _html} = live(conn, "/endpoints/new")

      # Error
      assert view |> render_change("form#endpoint", %{
        query_string: "select current_datetime() in invalid"
      }) =~ "parser error"


      # no error
      refute view |> render_change("form#endpoint", %{
        query_string: "select @my_param as valid"
      }) =~ "parser error"

      # detects params correctly
      assert has_element?(view, "form label", "my_param")

      # saves the change
      assert view |>  element("button", "Save") |> render_click() =~ "select @my_param as valid"
      path= assert_patch(view)
      assert path =~ ~r/\/endpoints\/\S+/
    end

    test "edit endpoint", %{conn: conn, user: user} do
      endpoint = insert(:endpoint, user: user, query: "select @other as initial")
      {:ok, view, _html} = live(conn, "/endpoints/#{endpoint.id}/edit")

      # Error
      assert view |> render_change("form#endpoint", %{
        query_string: "select current_datetime() in invalid"
      }) =~ "parser error"


      # no error
      refute view |> render_change("form#endpoint", %{
        query_string: "select @my_param as valid"
      }) =~ "parser error"
      # detects params correctly
      assert has_element?(view, "form label", "my_param")


      # saves the change
      assert view |>  element("button", "Save") |> render_click() =~ "select @my_param as valid"
      assert_patched(view, "/endpoints/#{endpoint.id}")
      # no longer has the initail query string
      refute render(view) =~ endpoint.query
    end
  end

  describe "run queries" do
    setup do
      # mock goth behaviour
      Goth
      |> stub(:fetch, fn _mod -> {:ok, %Goth.Token{token: "auth-token"}} end)

      GoogleApi.BigQuery.V2.Api.Jobs
      |> expect(:bigquery_jobs_query, 1, fn _conn, _proj_id, _opts ->
        {:ok, TestUtils.gen_bq_response([%{"testing" => "results-123"}])}
      end)

      :ok
    end

    test "new endpoint", %{conn: conn} do
      {:ok, view, _html} = live(conn, "/endpoints/new")

      refute render(view) =~ "results-123"

      render_hook(view, "run-query", %{
        query_string: "select current_datetime() as testing",
        query_params: %{}
      })

      assert view |> render() =~ "results-123"
    end

    test "edit endpoint", %{conn: conn, user: user} do
      endpoint = insert(:endpoint, user: user)
      {:ok, view, _html} = live(conn, "/endpoints/#{endpoint.id}/edit")
      refute render(view) =~ "results-123"

      render_hook(view, "run-query", %{
        query_string: "select current_datetime() as testing",
        query_params: %{}
      })

      assert view |> render() =~ "results-123"
    end

    test "show endpoint, with params", %{conn: conn, user: user} do
      endpoint = insert(:endpoint, user: user, query: "select @test_param as param")
      {:ok, view, _html} = live(conn, "/endpoints/#{endpoint.id}")
      refute render(view) =~ "results-123"
      # sow declared params
      assert view |> render() =~ "test_param"

      render_hook(view, "run-query", %{
        query_params: %{"test_param" => "my_param_value"}
      })

      # show results
      assert view |> render() =~ "results-123"
    end
  end
end
