defmodule Logflare.LogEventsTest do
  @moduledoc false
  use Logflare.DataCase
  alias Logflare.Logs.LogEvents

  test "fetch_event_by_id/3 with no partition" do
    le = build(:log_event, message: "some message")
    pid = self()
    ref = make_ref()

    expect(GoogleApi.BigQuery.V2.Api.Jobs, :bigquery_jobs_query, 1, fn _conn, _proj_id, opts ->
      query = opts[:body].query
      send(pid, {ref, query})

      {:ok,
       TestUtils.gen_bq_response([%{"id" => le.id, "event_message" => le.body["event_message"]}])}
    end)

    insert(:plan)
    user = insert(:user)
    source = insert(:source, user: user)

    assert %{"event_message" => "some message"} =
             LogEvents.fetch_event_by_id(source.token, le.id, [])

    assert_receive {^ref, query}
    refute query =~ "timestamp"
    assert query =~ ".id ="
  end

  test "fetch_event_by_id/3 with id and partition range" do
    le = build(:log_event, message: "some message")
    pid = self()
    ref = make_ref()

    expect(GoogleApi.BigQuery.V2.Api.Jobs, :bigquery_jobs_query, 1, fn _conn, _proj_id, opts ->
      query = opts[:body].query
      send(pid, {ref, query})

      {:ok,
       TestUtils.gen_bq_response([%{"id" => le.id, "event_message" => le.body["event_message"]}])}
    end)

    insert(:plan)
    user = insert(:user)
    source = insert(:source, user: user)

    assert %{"event_message" => "some message"} =
             LogEvents.fetch_event_by_id(source.token, le.id,
               partitions_range: [
                 DateTime.utc_now() |> DateTime.to_string(),
                 DateTime.utc_now() |> DateTime.to_string()
               ]
             )

    assert_receive {^ref, query}
    assert query =~ "timestamp <="
    assert query =~ "timestamp >="
    assert query =~ ".id ="
  end
end
