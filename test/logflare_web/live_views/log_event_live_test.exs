defmodule LogflareWeb.LogEventLiveTest do
  @moduledoc false
  use LogflareWeb.ConnCase
  alias Logflare.Logs.LogEvents

  import ExUnit.CaptureLog

  setup %{conn: conn} do
    insert(:plan)
    user = insert(:user)
    source = insert(:source, user: user)
    conn = login_user(conn, user)

    on_exit(fn ->
      Cachex.clear(LogEvents.Cache)
    end)

    %{user: user, source: source, conn: conn}
  end

  test "show by uuid with no timestamp param", %{conn: conn, source: source} do
    le = build(:log_event, source: source, message: "some message")

    pid = self()
    ref = make_ref()

    expect(GoogleApi.BigQuery.V2.Api.Jobs, :bigquery_jobs_query, 1, fn _conn, _proj_id, opts ->
      send(pid, {:query, ref, opts[:body].query})

      {:ok,
       TestUtils.gen_bq_response([%{"id" => le.id, "event_message" => le.body["event_message"]}])}
    end)

    {:ok, view, _html} = live(conn, ~p"/sources/#{source.id}/event?#{%{uuid: le.id}}")

    :timer.sleep(1500)
    assert render(view) =~ le.body["event_message"]
    assert render(view) =~ le.id
    assert_receive {:query, ^ref, query}
    assert query =~ "t0.timestamp >="
    assert query =~ "t0.id ="
  end

  test "show by uuid with timestamp param", %{conn: conn, source: source} do
    le = build(:log_event, source: source, message: "some message")

    pid = self()
    ref = make_ref()

    expect(GoogleApi.BigQuery.V2.Api.Jobs, :bigquery_jobs_query, 1, fn _conn, _proj_id, opts ->
      send(pid, {:query, ref, opts[:body].query})

      {:ok,
       TestUtils.gen_bq_response([%{"id" => le.id, "event_message" => le.body["event_message"]}])}
    end)

    {:ok, view, _html} =
      live(conn, ~p"/sources/#{source.id}/event?#{%{timestamp: "2024-01-01", uuid: le.id}}")

    :timer.sleep(1500)
    assert render(view) =~ le.body["event_message"]
    assert render(view) =~ le.id
    assert_receive {:query, ^ref, query}
    assert query =~ "t0.timestamp >="
    assert query =~ "t0.id ="
  end

  test "load from event cache", %{conn: conn, source: source} do
    reject(&GoogleApi.BigQuery.V2.Api.Jobs.bigquery_jobs_query/3)

    le = build(:log_event, message: "some new message")
    LogEvents.Cache.put(source.token, {"uuid", le.id}, le)

    {:ok, view, _html} = live(conn, ~p"/sources/#{source.id}/event?#{%{uuid: le.id}}")
    assert render(view) =~ "some new message"
    assert render(view) =~ le.id
  end

  test "mounted, query returned error", %{conn: conn, source: source} do
    le = build(:log_event, message: "some err message")

    expect(GoogleApi.BigQuery.V2.Api.Jobs, :bigquery_jobs_query, 2, fn _conn, _proj_id, _opts ->
      {:error, "some error"}
    end)

    logs =
      capture_log(fn ->
        {:ok, view, _html} = live(conn, ~p"/sources/#{source.id}/event?#{%{uuid: le.id}}")

        :timer.sleep(400)

        TestUtils.retry_assert(fn ->
          assert render(view) =~ "some error"
          refute render(view) =~ "some err message"
        end)
      end)

    assert logs =~ "Error loading log event"
    assert logs =~ "some error"
  end
end
