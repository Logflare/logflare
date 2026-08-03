defmodule Logflare.LogEventsTest do
  @moduledoc false
  use Logflare.DataCase
  use ExUnitProperties

  alias Logflare.Logs.LogEvents

  setup do
    [
      plan: insert(:plan),
      user: insert(:user)
    ]
  end

  test "fetch_event_by_id/3 with id and partition range", %{user: user} do
    le = build(:log_event, message: "some message")
    pid = self()
    ref = make_ref()

    expect(GoogleApi.BigQuery.V2.Api.Jobs, :bigquery_jobs_query, 1, fn _conn, _proj_id, opts ->
      query = opts[:body].query
      send(pid, {ref, query})

      {:ok,
       TestUtils.gen_bq_response([%{"id" => le.id, "event_message" => le.body["event_message"]}])}
    end)

    source = insert(:source, user: user)
    insert(:source_schema, source: source)

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

  test "fetch_event_by_id/3 with legacy partition type uses _PARTITIONTIME", %{user: user} do
    le = build(:log_event, message: "pseudo partition message")
    pid = self()
    ref = make_ref()

    expect(GoogleApi.BigQuery.V2.Api.Jobs, :bigquery_jobs_query, 1, fn _conn, _proj_id, opts ->
      query = opts[:body].query
      send(pid, {ref, query})

      {:ok,
       TestUtils.gen_bq_response([%{"id" => le.id, "event_message" => le.body["event_message"]}])}
    end)

    source = insert(:source, user: user, bq_table_partition_type: :pseudo)
    insert(:source_schema, source: source)

    assert %{"event_message" => "pseudo partition message"} =
             LogEvents.fetch_event_by_id(source.token, le.id,
               partitions_range: [DateTime.utc_now(), DateTime.utc_now()]
             )

    assert_receive {^ref, query}
    assert query =~ "_PARTITIONTIME"
    assert query =~ ".id ="
  end

  test "get_event_with_fallback/3 requires second precision timestamp" do
    timestamp = DateTime.from_naive!(~N[2026-01-01 00:00:00.123456], "Etc/UTC")

    assert_raise ArgumentError, "timestamp must be second precision", fn ->
      LogEvents.get_event_with_fallback(:source_token, "log_id", timestamp: timestamp)
    end
  end

  property "fetch_event_by_id/3 WHERE clause: timestamp < id < LQL regex filters", %{user: user} do
    schema =
      TestUtils.build_bq_schema(%{
        "metadata" => %{
          "context" => %{"pid" => "x", "vm" => %{"node" => "x"}},
          "error_string" => "x",
          "tesla_response" => "x",
          "status" => "x",
          "host" => "x"
        }
      })

    source = insert(:source, user: user)
    insert(:source_schema, source: source, bigquery_schema: schema)

    le = build(:log_event, message: "some message")
    pid = self()

    stub(GoogleApi.BigQuery.V2.Api.Jobs, :bigquery_jobs_query, fn _conn, _proj_id, opts ->
      send(pid, {:bq_query, opts[:body].query})
      {:ok, TestUtils.gen_bq_response([%{"id" => le.id, "event_message" => "some message"}])}
    end)

    lql_filter_gen =
      bind(
        member_of(
          ~w[m.context.pid:~ m.context.vm.node:~ m.error_string:~ m.tesla_response:~ m.status:~ m.host:~]
        ),
        fn prefix -> map(string(:alphanumeric, min_length: 2, max_length: 8), &(prefix <> &1)) end
      )

    check all lql_string <-
                map(list_of(lql_filter_gen, min_length: 1, max_length: 5), &Enum.join(&1, " ")) do
      LogEvents.fetch_event_by_id(source.token, le.id,
        partitions_range: [
          DateTime.utc_now() |> DateTime.to_string(),
          DateTime.utc_now() |> DateTime.to_string()
        ],
        lql: lql_string
      )

      assert_receive {:bq_query, sql}

      conditions = sql |> String.split("WHERE ") |> List.last() |> String.split(" AND ")
      idx = fn term -> Enum.find_index(conditions, &String.contains?(&1, term)) end

      assert idx.("timestamp >=") < idx.(".id =")
      assert idx.("timestamp <=") < idx.(".id =")
      assert idx.(".id =") < idx.("REGEXP_CONTAINS")
    end
  end
end
