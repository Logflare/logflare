defmodule LogflareWeb.Logs.LogLVTest do
  @moduledoc false
  use LogflareWeb.ConnCase
  import Phoenix.LiveViewTest
  alias Logflare.Sources
  alias Logflare.Repo
  alias Logflare.{Users, User}
  @endpoint LogflareWeb.Endpoint
  use Placebo
  alias Logflare.BigQuery.PredefinedTestUser
  alias Logflare.Source.RecentLogsServer, as: RLS
  alias Logflare.Lql.ChartRule
  alias Logflare.Lql
  alias LogflareWeb.Logs.LV, as: LogLV
  alias Logflare.Logs.{LogEvents, SearchQueryExecutor}
  @test_token :"2e051ba4-50ab-4d2a-b048-0dc595bfd6cf"
  import Ecto.Query
  use Mimic

  setup do
    email = System.get_env("LOGFLARE_TEST_USER_WITH_SET_IAM")
    bigquery_dataset_location = "US"
    bigquery_project_id = "logflare-dev-238720"
    bigquery_table_ttl = 60 * 60 * 24 * 365
    source_token = "2e051ba4-50ab-4d2a-b048-0dc595bfd6cf"

    user =
      User
      |> where([u], u.email == ^email)
      |> Repo.one()

    source = Sources.get_by(token: source_token)
    Logflare.Sources.Counters.start_link()
    {:ok, _} = RLS.start_link(%RLS{source_id: String.to_atom(source_token), source: source})
    Process.sleep(1000)

    %{user: user, source: [source]}
  end

  describe "log event liveview for Logflare UUID" do
    test "mounted, event not cached", %{conn: conn, source: [s | _], user: user} do
      uuid = "7530b1ca-1c7b-4bde-abc9-506e06fe1f25"

      conn =
        conn
        |> Plug.Test.init_test_session(%{user_id: user.id})
        |> get("/sources/#{s.id}/log_event?uuid=#{uuid}")

      html = html_response(conn, 200)
      assert html =~ ~s|id="log-event-loading"|
      assert not (html =~ ~s|id="log-event-not-found"|)
      assert not (html =~ ~s|id="log-event"|)
      assert not (html =~ ~s|id="log-event-error"|)

      expect(LogEvents, :fetch_event_by_id, fn _, _ -> %{} end)

      {:ok, view, html} = live(conn)

      send(view.pid, {nil, {:uuid, bq_row()}})

      html = render(view)

      le_cached = LogEvents.Cache.get!(s.token, "uuid:#{uuid}")
      assert not is_nil(le_cached)

      assert html =~ ~s|id="log-event"|
      assert not (html =~ ~s|id="log-event-not-found"|)
      assert not (html =~ ~s|id="log-event-error"|)
      assert html =~ bq_row().id
    end

    test "mounted, event cached", %{conn: conn, source: [s | _], user: user} do
      ev = generate_log_event()

      LogEvents.Cache.put(
        s.token,
        "uuid:#{ev.body.metadata.id}",
        ev
      )
      uuid = ev.id

      conn =
        conn
        |> Plug.Test.init_test_session(%{user_id: user.id})
        |> get("/sources/#{s.id}/log_event?uuid=#{uuid}")

      html = html_response(conn, 200)
      assert html =~ ~s|id="log-event"|
      assert not (html =~ ~s|id="log-event-not-found"|)
      assert not (html =~ ~s|id="log-event-loading"|)
      assert not (html =~ ~s|id="log-event-error"|)
    end
  end

  describe "log event for Vercel id" do
    test "mounted, event not cached", %{
      conn: conn,
      source: [s | _],
      user: user
    } do
      conn =
        conn
        |> Plug.Test.init_test_session(%{user_id: user.id})
        |> get("/sources/#{s.id}/log_event?vercel_id=4343434")

      html = html_response(conn, 200)

      assert html =~ ~s|id="log-event-loading"|
      assert not (html =~ ~s|id="log-event-not-found"|)
      assert not (html =~ ~s|id="log-event"|)
      assert not (html =~ ~s|id="log-event-error"|)

      expect(LogEvents, :fetch_event_by_path, fn _, _, _ -> bq_row() end)

      {:ok, view, html} = live(conn)

      send(view.pid, {nil, {:vercel, bq_row()}})

      vercel_id = bq_row() |> Map.get(:metadata) |> hd |> Map.get(:id)

      html = render(view)

      le_cached = LogEvents.Cache.get!(s.token, "vercel:#{vercel_id}")
      assert not is_nil(le_cached)

      assert html =~ ~s|id="log-event"|
      assert not (html =~ ~s|id="log-event-not-found"|)
      assert not (html =~ ~s|id="log-event-error"|)
      assert html =~ bq_row().id
    end

    test "mounted with cached log event", %{conn: conn, source: [s | _], user: user} do
      ev = generate_log_event()

      LogEvents.Cache.put(
        s.token,
        "vercel:#{ev.body.metadata.id}",
        ev
      )

      conn =
        conn
        |> Plug.Test.init_test_session(%{user_id: user.id})
        |> get("/sources/#{s.id}/log_event?vercel_id=#{ev.body.metadata.id}")

      html = html_response(conn, 200)
      assert not (html =~ ~s|id="log-event-loading"|)
      assert not (html =~ ~s|id="log-event-error"|)
      assert html =~ to_string(ev.body.metadata.id)
    end
  end

  def generate_log_event() do
    %Logflare.LogEvent{
      body: %Logflare.LogEvent.Body{
        created_at: nil,
        message: "info log 3",
        metadata: %{
          context: [
            %{
              application: "logflare_logger_pinger",
              domain: ["elixir"],
              file: "lib/logflare_pinger/log_pinger.ex",
              function: "handle_info/2",
              gl: "<0.67.0>",
              line: 39,
              mfa: ["Elixir.LogflareLoggerPinger.Server", "handle_info", "2"],
              module: "Elixir.LogflareLoggerPinger.Server",
              pid: "<0.358.0>",
              time: 1_599_750_602_725_356,
              vm: [%{node: "nonode@nohost"}]
            }
          ],
          id: 57_847_598_437_957_439_875,
          level: "info",
          some_boolean: false,
          some_list_1: [%{more_nested_lists2: [1, 2, 3]}]
        },
        timestamp: 1_599_750_602_725_000
      },
      id: "7530b1ca-1c7b-4bde-abc9-506e06fe1f25",
      ingested_at: nil,
      is_from_stale_query?: nil,
      origin_source_id: nil,
      params: nil,
      source: nil,
      sys_uint: nil,
      valid?: nil,
      validation_error: nil,
      via_rule: nil
    }
  end

  def bq_row() do
    %{
      event_message: "info log 3",
      id: "7530b1ca-1c7b-4bde-abc9-506e06fe1f25",
      metadata: [
        %{
          context: [
            %{
              application: "logflare_logger_pinger",
              domain: ["elixir"],
              file: "lib/logflare_pinger/log_pinger.ex",
              function: "handle_info/2",
              gl: "<0.67.0>",
              line: 39,
              mfa: ["Elixir.LogflareLoggerPinger.Server", "handle_info", "2"],
              module: "Elixir.LogflareLoggerPinger.Server",
              pid: "<0.358.0>",
              time: 1_599_680_402_725_000,
              vm: [%{node: "nonode@nohost"}]
            }
          ],
          id: "57847598437957439875",
          context_key: nil,
          current_user: [],
          level: "info",
          some_boolean: false,
          some_list: [],
          some_list_1: [%{more_nested_lists2: [1, 2, 3]}]
        }
      ],
      timestamp: 1_599_680_402_725_000
    }
  end
end
