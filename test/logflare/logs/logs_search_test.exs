defmodule Logflare.Logs.SearchTest do
  @moduledoc false
  alias Logflare.Sources
  alias Logflare.Logs.Search
  alias Logflare.Logs.Search.{SearchOpts, SearchResult}
  alias Logflare.Google.BigQuery
  alias Logflare.Google.BigQuery.GenUtils
  alias Logflare.Source.BigQuery.Pipeline
  use Logflare.DataCase
  import Logflare.DummyFactory

  setup do
    u = insert(:user, email: System.get_env("LOGFLARE_TEST_USER_WITH_SET_IAM"))
    s = insert(:source, user_id: u.id)
    s = Sources.get_by(id: s.id)
    {:ok, sources: [s], users: [u]}
  end

  describe "Search" do
    test "utc_today for source and regex", %{sources: [source | _], users: [user | _]} do
      les =
        for x <- 1..5, y <- 100..101 do
          build(:log_event, message: "x#{x} y#{y}", source: source)
        end

      bq_rows = Enum.map(les, &Pipeline.le_to_bq_row/1)
      project_id = GenUtils.get_project_id(source.token)

      assert {:ok, _} = BigQuery.create_dataset("#{user.id}", project_id)
      assert {:ok, _} = BigQuery.create_table(source.token)
      assert {:ok, _} = BigQuery.stream_batch!(source.token, bq_rows)

      {:ok, %{rows: rows}} = Search.search(%SearchOpts{source: source, regex: ~S|\d\d1|})

      assert length(rows) == 5
    end
  end

  describe "Query builder" do
    test "succeeds for basic query", %{sources: [source | _]} do
      assert Search.to_sql(%SearchOpts{source: source}) == {~s|SELECT t0."timestamp", t0."event_message" FROM "#{source.bq_table_id}" AS t0|, []}
    end

    test "converts Ecto PG sql to BQ sql" do
      ecto_pg_sql = "SELECT t0.\"timestamp\", t0.\"event_message\" FROM \"`logflare-dev-238720`.96465_test.4114dde8_1fa0_4efa_93b1_0fe6e4021f3c\" AS t0 WHERE (REGEXP_CONTAINS(t0.\"event_message\", $1))"
      assert Search.ecto_pg_sql_to_bq_sql(ecto_pg_sql) == "SELECT t0.timestamp, t0.event_message FROM `logflare-dev-238720`.96465_test.4114dde8_1fa0_4efa_93b1_0fe6e4021f3c AS t0 WHERE (REGEXP_CONTAINS(t0.event_message, ?))"

    end
  end
end
