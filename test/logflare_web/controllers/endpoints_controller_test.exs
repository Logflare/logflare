defmodule LogflareWeb.EndpointsControllerTest do
  use LogflareWeb.ConnCase

  describe "query" do
    setup :set_mimic_global

    setup do
      source = build(:source, rules: [])
      user = insert(:user, sources: [source])
      _plan = insert(:plan, name: "Free")
      # mock sql behaviour
      Logflare.SQL
      |> stub(:source_mapping, fn query, _, _ -> {:ok, query} end)
      |> stub(:parameters, fn _query -> {:ok, %{}} end)
      |> stub(:transform, fn query, _user_id -> {:ok, query} end)

      # mock goth behaviour
      Goth
      |> stub(:fetch, fn _mod -> {:ok, %Goth.Token{token: "auth-token"}} end)

      {:ok, user: user, source: source}
    end

    test "GET query", %{conn: init_conn, user: user} do
      endpoint = insert(:endpoint, user: user, enable_auth: false)

      GoogleApi.BigQuery.V2.Api.Jobs
      |> stub(:bigquery_jobs_query, fn _conn, _proj_id, _opts ->
        {:ok, gen_mock_bq_response("2022-06-21")}
      end)

      conn =
        init_conn
        |> get("/endpoints/query/#{endpoint.token}")

      assert [%{"date" => "2022-06-21"}] = json_response(conn, 200)["result"]
      assert conn.halted == false

      conn =
        init_conn
        |> get("/api/endpoints/query/#{endpoint.token}")

      assert [%{"date" => "2022-06-21"}] = json_response(conn, 200)["result"]
      assert conn.halted == false
    end
  end

  # TODO: migrate endpoints ui to use liveview
  describe "ui" do
    test "Endpoints index", %{conn: conn} do
      plan = insert(:plan, name: "Free", type: "standard")
      user = insert(:user)
      _source = insert(:source, user: user)
      _billing_account = insert(:billing_account, user: user, stripe_plan_id: plan.stripe_id)
      user = user |> Logflare.Repo.preload(:billing_account)

      conn = conn |> put_session(:user_id, user.id) |> assign(:user, user)

      conn = get(conn, Routes.endpoints_path(conn, :index))

      assert html_response(conn, 200) =~ "/endpoints"
    end

    @tag :failing
    test "Endpoint for User", %{conn: conn} do
      _plan = insert(:plan, name: "Free", type: "standard")
      user = insert(:user)
      _source = insert(:source, user: user)
      _billing_account = insert(:billing_account, user: user)
      user = user |> Logflare.Repo.preload(:billing_account)

      params = %{
        "name" => "current date",
        "query" => "select current_date() as date"
      }

      conn =
        conn |> assign(:user, user) |> post(Routes.endpoints_path(conn, :create), query: params)

      assert %{id: id} = redirected_params(conn)
      assert redirected_to(conn) == Routes.endpoints_path(conn, :show, id)

      conn = get(conn, Routes.endpoints_path(conn, :show, id))
      assert html_response(conn, 200) =~ "/endpoints/"
    end
  end

  # this is a successful bq response retrieved manually
  defp gen_mock_bq_response(date) when is_binary(date) do
    %GoogleApi.BigQuery.V2.Model.QueryResponse{
      cacheHit: false,
      errors: nil,
      jobComplete: true,
      jobReference: %GoogleApi.BigQuery.V2.Model.JobReference{
        jobId: "job_0rQLvVW-T5P3wSz1CnHRamZj0MiM",
        location: "US",
        projectId: "logflare-dev-238720"
      },
      kind: "bigquery#queryResponse",
      numDmlAffectedRows: nil,
      pageToken: nil,
      rows: [
        %GoogleApi.BigQuery.V2.Model.TableRow{
          f: [%GoogleApi.BigQuery.V2.Model.TableCell{v: date}]
        }
      ],
      schema: %GoogleApi.BigQuery.V2.Model.TableSchema{
        fields: [
          %GoogleApi.BigQuery.V2.Model.TableFieldSchema{
            categories: nil,
            description: nil,
            fields: nil,
            mode: "NULLABLE",
            name: "date",
            policyTags: nil,
            type: "DATE"
          }
        ]
      },
      totalBytesProcessed: "0",
      totalRows: "1"
    }
  end
end
