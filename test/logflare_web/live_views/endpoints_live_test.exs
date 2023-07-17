defmodule LogflareWeb.EndpointsLiveTest do
  @moduledoc false
  use LogflareWeb.ConnCase

  setup %{conn: conn} do
    insert(:plan)
    user = insert(:user)

    conn =
      conn
      |> login_user(user)

    {:ok, user: user, conn: conn}
  end

  test "list endpoints", %{conn: conn, user: user} do
    endpoint = insert(:endpoint, user: user)

    {:ok, view, _html} = live(conn, "/endpoints")

    html = view |> element("#endpoints-browser-list") |> render()
    assert html =~ endpoint.name
    assert has_element?(view, "#endpoints-intro")

    render_hook(view, "show-endpoint", %{
      endpoint_id: endpoint.id
    })

    assert view
           |> has_element?("#show-endpoint")

    assert_patched(view, "/endpoints/#{endpoint.id}")

    html = view |> element("#show-endpoint") |> render()
    assert html =~ endpoint.name
    assert html =~ endpoint.query

    render_hook(view, "list-endpoints")
    assert_patched(view, "/endpoints")
  end

  test "show endpoint", %{conn: conn, user: user} do
    endpoint = insert(:endpoint, user: user)

    {:ok, view, _html} = live(conn, "/endpoints/#{endpoint.id}")

    assert view |> element("#show-endpoint") |> render() =~ endpoint.name
    assert view |> element("#show-endpoint") |> render() =~ endpoint.query
  end

  test "show endpoint -> edit endpoint", %{conn: conn, user: user} do
    endpoint = insert(:endpoint, user: user)
    {:ok, view, _html} = live(conn, "/endpoints/#{endpoint.id}")

    render_hook(view, "edit-endpoint", %{endpoint_id: endpoint.id})

    assert_patched(view, "/endpoints/#{endpoint.id}/edit")

    assert view |> has_element?("#edit-endpoint")
    assert view |> element("#edit-endpoint") |> render() =~ endpoint.name

    # edit the endpoint
    new_query = "select current_timestamp() as my_time"

    render_hook(view, "save-endpoint", %{
      endpoint: %{
        query: new_query
      }
    })

    assert view |> render() =~ new_query
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

  test "delete endpoint", %{conn: conn, user: user} do
    endpoint = insert(:endpoint, user: user)
    {:ok, view, _html} = live(conn, "/endpoints/#{endpoint.id}")
    assert view |> element("#show-endpoint") |> render() =~ endpoint.name
    # should be in endpoints list
    assert view |> element("#endpoints-browser-list") |> render() =~ endpoint.name
    assert render_hook(view, "delete-endpoint", %{endpoint_id: endpoint.id}) =~ "has been deleted"
    assert_patched(view, "/endpoints")
    # removed from endpoints list
    refute view |> element("#endpoints-browser-list") |> render() =~ endpoint.name

    {:ok, view, _html} = live(conn, "/endpoints/#{endpoint.id}")
    assert view |> has_element?("#not-found")
  end

  describe "parse queries on change" do
    test "new endpoint", %{conn: conn} do
      {:ok, view, _html} = live(conn, "/endpoints/new")

      # Error
      render_hook(view, "parse-query", %{
        query_string: "select current_datetime() in invalid"
      })

      assert view |> element("#new-endpoint") |> render() =~ "parser error"

      # no error
      render_hook(view, "parse-query", %{
        query_string: "select @my_param as valid"
      })

      refute view |> element("#new-endpoint") |> render() =~ "parser error"
      assert view |> element("#new-endpoint") |> render() =~ "my_param"
    end

    test "edit endpoint", %{conn: conn, user: user} do
      endpoint = insert(:endpoint, user: user)
      {:ok, view, _html} = live(conn, "/endpoints/#{endpoint.id}/edit")

      # Error
      render_hook(view, "parse-query", %{
        query_string: "select current_datetime() in invalid"
      })

      assert view |> element("#edit-endpoint") |> render() =~ "parser error"

      # no error
      render_hook(view, "parse-query", %{
        query_string: "select @my_param as valid"
      })

      refute view |> element("#edit-endpoint") |> render() =~ "parser error"
      assert view |> element("#edit-endpoint") |> render() =~ "my_param"
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
