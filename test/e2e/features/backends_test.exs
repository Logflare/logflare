defmodule E2e.Features.BackendsTest do
  use Logflare.FeatureCase, async: false

  alias Logflare.SingleTenant

  setup do
    start_supervised!(Logflare.SystemMetricsSup)

    :ok
  end

  describe "clickhouse backend form" do
    TestUtils.setup_single_tenant(seed_user: true)

    setup do
      backend =
        insert(:backend,
          user: SingleTenant.get_default_user(),
          type: :clickhouse,
          config: %{
            url: "https://ingest.example.com:8443",
            username: "default",
            password: "secret",
            database: "default",
            port: 8443,
            read_only_url: "https://reads.example.com:8443",
            read_only_urls: %{"reporting" => "https://reporting.example.com:8443"},
            default_read_cluster: "reporting"
          }
        )

      %{backend: backend}
    end

    test "edit renders the read cluster editor with existing clusters", %{
      backend: backend,
      conn: conn
    } do
      conn
      |> visit(~p"/backends/#{backend.id}/edit")
      |> assert_has("label", text: "Read-Only Cluster URLs (Optional)")
      |> assert_has("button", text: "Add read cluster")
      |> assert_has("#read-cluster-row-0 input[value='reporting']")
      |> assert_has("#read-cluster-row-0 input[value='https://reporting.example.com:8443']")
      |> assert_has("input[name='backend[config][default_read_cluster]'][value='reporting']")
    end

    test "edit can add and remove read cluster rows", %{backend: backend, conn: conn} do
      conn
      |> visit(~p"/backends/#{backend.id}/edit")
      |> refute_has("#read-cluster-row-1")
      |> click_button("Add read cluster")
      |> assert_has("#read-cluster-row-1")
      |> click("#read-cluster-row-1 button[phx-click='remove_row']")
      |> refute_has("#read-cluster-row-1")
      |> assert_has("#read-cluster-row-0 input[value='reporting']")
    end
  end
end
