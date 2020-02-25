defmodule Logflare.LogsTest do
  @moduledoc false
  use Logflare.DataCase
  use Placebo
  import Logflare.Factory
  alias Logflare.Logs
  alias Logflare.Rules
  alias Logflare.User
  alias Logflare.Users
  alias Logflare.Sources
  alias Logflare.Source.{BigQuery.Buffer}
  alias Logflare.Google.BigQuery
  alias Logflare.Google.BigQuery.{Query, GenUtils}
  alias Logflare.SystemMetricsSup
  alias Logflare.Source.BigQuery.SchemaBuilder
  alias Logflare.Sources.Counters
  alias Logflare.Source.RecentLogsServer, as: RLS
  alias Logflare.Lql
  @test_dataset_location "us-east4"

  describe "log event ingest" do
    @describetag :skip
    test "succeeds for floats", %{sources: [s | _]} do
      conn = GenUtils.get_conn()
      project_id = GenUtils.get_project_id(s.token)
      dataset_id = "dev_dataset_#{s.user.id}"

      assert {:ok, _} =
               BigQuery.create_dataset(
                 "#{s.user_id}",
                 dataset_id,
                 @test_dataset_location,
                 project_id
               )

      assert {:ok, table} = BigQuery.create_table(s.token, dataset_id, project_id, 300_000)

      table_id = table.id |> String.replace(":", ".")
      sql = "SELECT * FROM `#{table_id}`"

      Logs.ingest_logs([%{"message" => "test", "metadata" => %{"float" => 0.001}}], s)
      Process.sleep(3_000)
      {:ok, response} = Query.query(conn, project_id, sql)
      assert response.rows == [%{"log_message" => "test", "metadata" => %{"float" => 0.001}}]
    end
  end

  describe "log event ingest for source with regex rules" do
    setup do
      u = insert(:user, email: System.get_env("LOGFLARE_TEST_USER_2"))
      u = Users.get(u.id)
      [sink1, sink2] = insert_list(2, :source, user_id: u.id)

      rule1 = build(:rule, sink: sink1.token, regex: "pattern2")
      rule2 = build(:rule, sink: sink2.token, regex: "pattern3")

      s1 = insert(:source, token: Faker.UUID.v4(), rules: [rule1, rule2], user_id: u.id)

      conn = GenUtils.get_conn()
      project_id = GenUtils.get_project_id(s1.token)
      dataset_id = User.generate_bq_dataset_id(u)

      assert {:ok, _} =
               BigQuery.create_dataset(
                 "#{u.id}",
                 dataset_id,
                 @test_dataset_location,
                 project_id
               )

      assert {:ok, table} = BigQuery.create_table(s1.token, dataset_id, project_id, 300_000)

      schema =
        SchemaBuilder.build_table_schema(
          %{"level" => "warn"},
          SchemaBuilder.initial_table_schema()
        )

      {:ok, _} =
        BigQuery.patch_table(
          s1.token,
          schema,
          dataset_id,
          project_id
        )

      s1 =
        Sources.get_by(token: s1.token)
        |> Sources.preload_defaults()
        |> Sources.put_bq_table_data()

      SystemMetricsSup.start_link()
      Counters.start_link()
      Buffer.start_link(%RLS{source_id: s1.token})
      Buffer.start_link(%RLS{source_id: sink1.token})
      Buffer.start_link(%RLS{source_id: sink2.token})
      {:ok, sources: [s1], sinks: [sink1, sink2], users: [u]}
    end

    test "sink source routing", %{sources: [s1 | _], sinks: [sink1, sink2 | _]} do
      log_params_batch = [
        %{"message" => "pattern"},
        %{"message" => "pattern2"},
        %{"message" => "pattern3"}
      ]

      assert Logs.ingest_logs(log_params_batch, s1) == :ok
      assert Sources.Counters.log_count(s1) == 3
      assert Buffer.get_count(s1) == 3

      assert Sources.Counters.log_count(sink1) == 1
      assert Buffer.get_count(sink1) == 1

      assert Sources.Counters.log_count(sink2) == 1
      assert Buffer.get_count(sink2) == 1
    end

    test "sink routing is allowed for one depth level only", %{
      users: [u],
      sources: [s1],
      sinks: [first_sink, last_sink | _]
    } do
      _first_sink_rule =
        insert(:rule, sink: last_sink.token, regex: "test", source_id: first_sink.id)

      _s1rule1 = insert(:rule, sink: first_sink.token, regex: "test", source_id: s1.id)

      log_params_batch = [
        %{"message" => "test"}
      ]

      s1 =
        Sources.get_by_and_preload(id: s1.id)
        |> Sources.put_bq_table_data()

      assert Logs.ingest_logs(log_params_batch, s1) == :ok

      assert Sources.Counters.log_count(s1) == 1
      assert Buffer.get_count(s1) == 1

      assert Sources.Counters.log_count(first_sink) == 1
      assert Buffer.get_count(first_sink) == 1

      assert Sources.Counters.log_count(last_sink) == 0
      assert Buffer.get_count(last_sink) == 0
    end
  end

  describe "log event ingest for source with LQL rules" do
    setup do
      u = insert(:user, email: System.get_env("LOGFLARE_TEST_USER_2"))
      u = Users.get(u.id)
      [sink1, sink2] = insert_list(2, :source, user_id: u.id)

      s1 = insert(:source, token: Faker.UUID.v4(), rules: [], user_id: u.id)

      conn = GenUtils.get_conn()
      project_id = GenUtils.get_project_id(s1.token)
      dataset_id = User.generate_bq_dataset_id(u)

      assert {:ok, _} =
               BigQuery.create_dataset(
                 "#{u.id}",
                 dataset_id,
                 @test_dataset_location,
                 project_id
               )

      assert {:ok, table} = BigQuery.create_table(s1.token, dataset_id, project_id, 300_000)

      schema =
        SchemaBuilder.build_table_schema(
          %{"level" => "warn"},
          SchemaBuilder.initial_table_schema()
        )

      {:ok, _} =
        BigQuery.patch_table(
          s1.token,
          schema,
          dataset_id,
          project_id
        )

      {:ok, lql_rule1} =
        Rules.create_rule(%{"sink" => sink1.token, "lql_string" => "warning"}, s1)

      {:ok, lql_rule2} =
        Rules.create_rule(
          %{"sink" => sink2.token, "lql_string" => "crash metadata.level:error"},
          s1
        )

      SystemMetricsSup.start_link()
      Counters.start_link()

      Buffer.start_link(%RLS{source_id: s1.token})
      Buffer.start_link(%RLS{source_id: sink1.token})
      Buffer.start_link(%RLS{source_id: sink2.token})

      s1 =
        Sources.get_by(token: s1.token)
        |> Sources.preload_defaults()
        |> Sources.put_bq_table_data()

      {:ok, sources: [s1], sinks: [sink1, sink2], users: [u]}
    end

    test "sink source routing", %{sources: [s1 | _], sinks: [sink1, sink2 | _]} do
      log_params_batch = [
        %{"message" => "pattern", "metadata" => %{"level" => "info"}},
        %{"message" => "pattern2 warning", "metadata" => %{"level" => "error"}},
        %{"message" => "pattern2 warning", "metadata" => %{"level" => "warn"}},
        %{"message" => "pattern3 crash", "metadata" => %{"level" => "warn"}},
        %{"message" => "pattern3 crash", "metadata" => %{"level" => "error"}}
      ]

      assert Logs.ingest_logs(log_params_batch, s1) == :ok

      assert Sources.Counters.log_count(s1) == 5
      assert Buffer.get_count(s1) == 5

      assert Sources.Counters.log_count(sink1) == 2
      assert Buffer.get_count(sink1) == 2

      assert Sources.Counters.log_count(sink2) == 1
      assert Buffer.get_count(sink2) == 1
    end

    test "sink routing is allowed for one depth level only", %{
      users: [u],
      sources: [s1],
      sinks: [first_sink, last_sink | _]
    } do
      _first_sink_rule =
        insert(:rule,
          sink: last_sink.token,
          lql_string: "test",
          lql_filters: [
            %Lql.FilterRule{operator: :"~", value: "test", modifiers: [], path: "event_message"}
          ],
          source_id: first_sink.id
        )

      _s1rule1 =
        insert(:rule,
          sink: first_sink.token,
          lql_string: "test",
          lql_filters: [
            %Lql.FilterRule{
              operator: :"~",
              value: "test",
              modifiers: [],
              path: "event_message"
            }
          ],
          source_id: s1.id
        )

      log_params_batch = [
        %{"message" => "test"}
      ]

      s1 =
        Sources.get_by_and_preload(id: s1.id)
        |> Sources.put_bq_table_data()

      assert Logs.ingest_logs(log_params_batch, s1) == :ok

      assert Sources.Counters.log_count(s1) == 1
      assert Buffer.get_count(s1) == 1

      assert Sources.Counters.log_count(first_sink) == 1
      assert Buffer.get_count(first_sink) == 1

      assert Sources.Counters.log_count(last_sink) == 0
      assert Buffer.get_count(last_sink) == 0
    end
  end
end
