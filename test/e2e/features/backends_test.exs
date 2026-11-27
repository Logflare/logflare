defmodule E2e.Features.BackendsTest do
  use Logflare.FeatureCase, async: false

  import Fluffy.Playwright, only: [blur: 2, press_sequentially: 4]

  alias Fluffy.Event
  alias Logflare.SingleTenant
  alias Playwright.WebSocket

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
            read_only_urls: %{"reporting" => "https://reporting.example.com:8443"},
            default_read_cluster: "reporting"
          }
        )

      %{backend: backend}
    end

    test "edit renders the read cluster editor with existing clusters", %{
      backend: backend,
      session: session
    } do
      session
      |> visit(~p"/backends/#{backend.id}/edit")
      |> assert(visible(by_text("Read-Only Cluster URLs (Optional)")))
      |> assert(visible(by_role(:button, name: "Add read cluster")))
      |> assert(attribute(label_input(0), "value", "reporting"))
      |> assert(attribute(url_input(0), "value", "https://reporting.example.com:8443"))
      |> assert(attribute(default_cluster_input(), "value", "reporting"))
    end

    test "edit can add and remove read cluster rows", %{backend: backend, session: session} do
      session
      |> visit(~p"/backends/#{backend.id}/edit")
      |> refute(visible(row(1)))
      |> click(by_role(:button, name: "Add read cluster"))
      |> assert(visible(row(1)))
      |> click(remove_button(1))
      |> refute(visible(row(1)))
      |> assert(attribute(label_input(0), "value", "reporting"))
    end

    test "typing into a read cluster field does not sync to the server until blurred", %{
      backend: backend,
      session: session
    } do
      test_pid = self()

      session
      |> on(Event.web_socket(), fn web_socket ->
        WebSocket.on(web_socket, :frame_sent, fn %{payload: payload} ->
          if is_binary(payload) and payload =~ ~s("sync"), do: send(test_pid, :sync_sent)
        end)
      end)
      |> visit(~p"/backends/#{backend.id}/edit")
      |> clear(url_input(0))
      |> press_sequentially(url_input(0), "https://reader-b.example.com:8443", delay: 10)

      refute_received :sync_sent

      session
      |> blur(url_input(0))
      |> assert(value(url_input(0), "https://reader-b.example.com:8443"))

      assert_receive :sync_sent
      refute_receive :sync_sent, 500
    end

    test "adding a row preserves unsaved cluster and default edits", %{
      backend: backend,
      session: session
    } do
      session
      |> visit(~p"/backends/#{backend.id}/edit")
      |> fill(label_input(0), "reporting-edited")
      |> fill(url_input(0), "https://reporting-edited.example.com:8443")
      |> fill(default_cluster_input(), "reporting-edited")
      |> click(by_role(:button, name: "Add read cluster"))
      |> assert(visible(row(1)))
      |> assert(value(label_input(0), "reporting-edited"))
      |> assert(value(url_input(0), "https://reporting-edited.example.com:8443"))
      |> assert(value(default_cluster_input(), "reporting-edited"))
      |> click(by_role(:button, name: "Save changes"))
      |> assert(visible(by_text("Successfully updated backend")))

      config = Logflare.Backends.get_backend(backend.id).config

      assert config.read_only_urls == %{
               "reporting-edited" => "https://reporting-edited.example.com:8443"
             }

      assert config.default_read_cluster == "reporting-edited"
    end
  end

  describe "clickhouse read cluster row removal" do
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
            read_only_urls: %{
              "alpha" => "https://a.example.com:8443",
              "beta" => "https://b.example.com:8443",
              "gamma" => "https://c.example.com:8443"
            },
            default_read_cluster: "gamma"
          }
        )

      %{backend: backend}
    end

    test "removing a row removes that row, leaving the others addressable", %{
      backend: backend,
      session: session
    } do
      session = visit(session, ~p"/backends/#{backend.id}/edit")

      for ref <- 0..2, do: assert(session, visible(label_input(ref)))

      session
      |> click(remove_button(0))
      |> refute(visible(row(0)))
      |> assert(attribute(label_input(1), "value", "beta"))
      |> assert(attribute(label_input(2), "value", "gamma"))
      |> assert(value(label_input(1), "beta"))
      |> assert(value(label_input(2), "gamma"))
    end

    test "removing a row saves the remaining clusters", %{backend: backend, session: session} do
      session
      |> visit(~p"/backends/#{backend.id}/edit")
      |> click(remove_button(0))
      |> click(by_role(:button, name: "Save changes"))
      |> assert(visible(by_text("Successfully updated backend")))

      assert Logflare.Backends.get_backend(backend.id).config.read_only_urls == %{
               "beta" => "https://b.example.com:8443",
               "gamma" => "https://c.example.com:8443"
             }
    end

    test "removing a row preserves unsaved edits in surviving rows and the default", %{
      backend: backend,
      session: session
    } do
      session
      |> visit(~p"/backends/#{backend.id}/edit")
      |> fill(label_input(2), "gamma-edited")
      |> fill(url_input(2), "https://gamma-edited.example.com:8443")
      |> fill(default_cluster_input(), "gamma-edited")
      |> click(remove_button(0))
      |> refute(visible(row(0)))
      |> assert(value(label_input(2), "gamma-edited"))
      |> assert(value(url_input(2), "https://gamma-edited.example.com:8443"))
      |> assert(value(default_cluster_input(), "gamma-edited"))
      |> click(by_role(:button, name: "Save changes"))
      |> assert(visible(by_text("Successfully updated backend")))

      config = Logflare.Backends.get_backend(backend.id).config

      assert config.read_only_urls == %{
               "beta" => "https://b.example.com:8443",
               "gamma-edited" => "https://gamma-edited.example.com:8443"
             }

      assert config.default_read_cluster == "gamma-edited"
    end
  end

  defp row(ref), do: by_css("#read-cluster-row-#{ref}")

  defp label_input(ref), do: row(ref) |> by_placeholder("caller label")

  defp url_input(ref), do: row(ref) |> by_placeholder("https://read-cluster:8443")

  defp remove_button(ref), do: row(ref) |> by_role(:button)

  defp default_cluster_input, do: by_label("Default Read Cluster")
end
