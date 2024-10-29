defmodule Logflare.EndpointsCacheTest do
  use Logflare.DataCase

  alias Logflare.Endpoints

  describe "cache behavior" do
    setup do
      user = insert(:user)
      endpoint = insert(:endpoint, user: user, query: "select current_datetime() as testing")
      _plan = insert(:plan, name: "Free")

      %{user: user, endpoint: endpoint}
    end

    test "cache starts and serves cached results", %{endpoint: endpoint} do
      # Mock response by setting up test backend
      test_response = [%{"testing" => "123"}]

      GoogleApi.BigQuery.V2.Api.Jobs
      |> expect(:bigquery_jobs_query, 1, fn _conn, _proj_id, _opts ->
        {:ok, TestUtils.gen_bq_response(test_response)}
      end)

      # Start cache process
      {:ok, cache_pid} = start_supervised({Logflare.Endpoints.Cache, {endpoint, %{}}})
      assert Process.alive?(cache_pid)

      # First query should hit backend
      assert {:ok, %{rows: [%{"testing" => "123"}]}} = Endpoints.run_cached_query(endpoint)

      # Second query should hit cache without calling backend again
      assert {:ok, %{rows: [%{"testing" => "123"}]}} = Endpoints.run_cached_query(endpoint)
    end

    test "cache respects cache_duration_seconds", %{user: user} do
    end

    test "cache handles query parameters", %{user: user} do
    end

    test "cache process terminates when endpoint is deleted", %{endpoint: endpoint} do
    end
  end
end
