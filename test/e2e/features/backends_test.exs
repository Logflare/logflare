defmodule E2e.Features.BackendsTest do
  use Logflare.FeatureCase, async: false

  alias Logflare.SingleTenant
  alias PlaywrightEx.Frame

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

    test "typing into a read cluster field does not sync to the server until blurred", %{
      backend: backend,
      conn: conn
    } do
      session = visit(conn, ~p"/backends/#{backend.id}/edit")

      unwrap(session, fn %{frame_id: frame_id} ->
        {:ok, _} = Frame.fill(frame_id, selector: url_selector(0), value: "", timeout: 5_000)

        {:ok, _} =
          Frame.evaluate(frame_id,
            expression: """
            (() => {
              window.__sentFrames = [];
              const originalSend = WebSocket.prototype.send;
              WebSocket.prototype.send = function (data) {
                window.__sentFrames.push(data);
                return originalSend.apply(this, arguments);
              };
              return true;
            })()
            """,
            timeout: 5_000
          )

        {:ok, _} =
          Frame.type(frame_id,
            selector: url_selector(0),
            text: "https://reader-b.example.com:8443",
            delay: 10,
            timeout: 5_000
          )

        {:ok, sent_before_blur} = sent_sync_frame_count(frame_id)
        assert sent_before_blur == 0

        {:ok, _} = Frame.blur(frame_id, selector: url_selector(0), timeout: 5_000)

        {:ok, _} =
          Frame.wait_for_function(frame_id,
            expression:
              "() => window.__sentFrames.some((f) => typeof f === 'string' && f.includes('\"sync\"'))",
            is_function: true,
            timeout: 5_000
          )

        {:ok, sent_after_blur} = sent_sync_frame_count(frame_id)
        assert sent_after_blur == 1

        assert {:ok, "https://reader-b.example.com:8443"} = input_value(frame_id, url_selector(0))
      end)
    end

    test "adding a row preserves unsaved cluster and default edits", %{
      backend: backend,
      conn: conn
    } do
      session = visit(conn, ~p"/backends/#{backend.id}/edit")

      unwrap(session, fn %{frame_id: frame_id} ->
        Frame.fill(frame_id,
          selector: label_selector(0),
          value: "reporting-edited",
          timeout: 5_000
        )

        Frame.fill(frame_id,
          selector: url_selector(0),
          value: "https://reporting-edited.example.com:8443",
          timeout: 5_000
        )

        Frame.fill(frame_id,
          selector: default_cluster_selector(),
          value: "reporting-edited",
          timeout: 5_000
        )
      end)

      session = click_button(session, "Add read cluster")

      session
      |> assert_has("#read-cluster-row-1")
      |> unwrap(fn %{frame_id: frame_id} ->
        assert {:ok, "reporting-edited"} = input_value(frame_id, label_selector(0))

        assert {:ok, "https://reporting-edited.example.com:8443"} =
                 input_value(frame_id, url_selector(0))

        assert {:ok, "reporting-edited"} =
                 input_value(frame_id, default_cluster_selector())
      end)
      |> click_button("Save changes")
      |> assert_has("*", text: "Successfully updated backend")

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
            }
          }
        )

      %{backend: backend}
    end

    test "removing a row removes that row, leaving the others addressable", %{
      backend: backend,
      conn: conn
    } do
      session = visit(conn, ~p"/backends/#{backend.id}/edit")

      unwrap(session, fn %{frame_id: frame_id} ->
        for ref <- 0..2, do: assert({:ok, _} = input_value(frame_id, ref))
      end)

      session = click(session, "#read-cluster-row-0 button[phx-click='remove_row']")

      session
      |> refute_has("#read-cluster-row-0")
      |> assert_has("#read-cluster-row-1 input[value='beta']")
      |> assert_has("#read-cluster-row-2 input[value='gamma']")

      unwrap(session, fn %{frame_id: frame_id} ->
        assert {:ok, "beta"} = input_value(frame_id, 1)
        assert {:ok, "gamma"} = input_value(frame_id, 2)
      end)
    end

    test "removing a row saves the remaining clusters", %{backend: backend, conn: conn} do
      conn
      |> visit(~p"/backends/#{backend.id}/edit")
      |> click("#read-cluster-row-0 button[phx-click='remove_row']")
      |> click_button("Save changes")
      |> assert_has("*", text: "Successfully updated backend")

      assert Logflare.Backends.get_backend(backend.id).config.read_only_urls == %{
               "beta" => "https://b.example.com:8443",
               "gamma" => "https://c.example.com:8443"
             }
    end

    test "removing a row preserves unsaved edits in surviving rows and the default", %{
      backend: backend,
      conn: conn
    } do
      session = visit(conn, ~p"/backends/#{backend.id}/edit")

      unwrap(session, fn %{frame_id: frame_id} ->
        Frame.fill(frame_id, selector: label_selector(2), value: "gamma-edited", timeout: 5_000)

        Frame.fill(frame_id,
          selector: url_selector(2),
          value: "https://gamma-edited.example.com:8443",
          timeout: 5_000
        )

        Frame.fill(frame_id,
          selector: default_cluster_selector(),
          value: "gamma-edited",
          timeout: 5_000
        )
      end)

      session = click(session, "#read-cluster-row-0 button[phx-click='remove_row']")

      session
      |> refute_has("#read-cluster-row-0")
      |> unwrap(fn %{frame_id: frame_id} ->
        assert {:ok, "gamma-edited"} = input_value(frame_id, label_selector(2))

        assert {:ok, "https://gamma-edited.example.com:8443"} =
                 input_value(frame_id, url_selector(2))

        assert {:ok, "gamma-edited"} =
                 input_value(frame_id, default_cluster_selector())
      end)
      |> click_button("Save changes")
      |> assert_has("*", text: "Successfully updated backend")

      config = Logflare.Backends.get_backend(backend.id).config

      assert config.read_only_urls == %{
               "beta" => "https://b.example.com:8443",
               "gamma-edited" => "https://gamma-edited.example.com:8443"
             }

      assert config.default_read_cluster == "gamma-edited"
    end
  end

  defp sent_sync_frame_count(frame_id) do
    Frame.evaluate(frame_id,
      expression:
        "window.__sentFrames.filter((f) => typeof f === 'string' && f.includes('\"sync\"')).length",
      timeout: 5_000
    )
  end

  defp input_value(frame_id, ref) when is_integer(ref) do
    input_value(frame_id, label_selector(ref))
  end

  defp input_value(frame_id, selector) do
    Frame.input_value(frame_id, selector: selector, timeout: 5_000)
  end

  defp label_selector(ref) do
    "#read-cluster-row-#{ref} input[name='backend[config][read_cluster_label_#{ref}]']"
  end

  defp url_selector(ref) do
    "#read-cluster-row-#{ref} input[name='backend[config][read_cluster_url_#{ref}]']"
  end

  defp default_cluster_selector do
    "input[name='backend[config][default_read_cluster]']"
  end
end
