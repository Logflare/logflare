defmodule Logflare.LogsTest do
  @moduledoc false
  use Logflare.DataCase
  import Logflare.Factory
  alias Logflare.Logs
  alias Logflare.Rules
  alias Logflare.User
  alias Logflare.Users
  alias Logflare.Source
  alias Logflare.Source.BigQuery.Schema, as: BigQuerySchemaGS
  alias Logflare.Source.{BigQuery.Buffer}
  alias Logflare.Source.BigQuery.SchemaBuilder
  alias Logflare.Source.RecentLogsServer, as: RLS
  alias Logflare.Sources
  alias Logflare.Sources.Counters
  alias Logflare.Google.BigQuery
  alias Logflare.Google.BigQuery.GenUtils
  alias Logflare.SystemMetricsSup
  alias Logflare.Lql
  @test_dataset_location "us-east4"
  @plan %{limit_source_fields_limit: 500}
  @moduletag :this

  setup_all do
    Counters.start_link() |> IO.inspect()
    :ok
  end

  describe "log event ingest for source with regex rules" do
    setup do
      {:ok, u} =
        Users.insert_or_update_user(
          params_for(:user, email: System.get_env("LOGFLARE_TEST_USER_2"))
        )

      u = Users.get_user(u.id)
      [sink1, sink2] = insert_list(2, :source, user_id: u.id)

      rule1 = build(:rule, sink: sink1.token, regex: "pattern2")
      rule2 = build(:rule, sink: sink2.token, regex: "pattern3")

      {:ok, s1} =
        Sources.create_source(
          params_for(:source, token: Faker.UUID.v4(), rules: [rule1, rule2]),
          u
        )

      BigQuerySchemaGS.start_link(%RLS{source_id: s1.token, plan: @plan})
      BigQuerySchemaGS.start_link(%RLS{source_id: sink1.token, plan: @plan})
      BigQuerySchemaGS.start_link(%RLS{source_id: sink2.token, plan: @plan})

      project_id = GenUtils.get_project_id(s1.token)
      dataset_id = User.generate_bq_dataset_id(u)

      BigQuery.create_dataset(
        "#{u.id}",
        dataset_id,
        @test_dataset_location,
        project_id
      )

      BigQuery.create_table(s1.token, dataset_id, project_id, 300_000)

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
      users: [_u],
      sources: [s1],
      sinks: [first_sink, last_sink | _]
    } do
      _first_sink_rule =
        insert(:rule, sink: last_sink.token, regex: "test", source_id: first_sink.id)

      _s1rule1 = insert(:rule, sink: first_sink.token, regex: "test", source_id: s1.id)

      log_params_batch = [
        %{"message" => "test"}
      ]

      s1 = Sources.get_by_and_preload(id: s1.id)

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
      {:ok, u} =
        Users.insert_or_update_user(
          params_for(:user, email: System.get_env("LOGFLARE_TEST_USER_2"))
        )

      u = Users.get_user(u.id)
      [sink1, sink2] = insert_list(2, :source, user_id: u.id)

      {:ok, s1} =
        Sources.create_source(
          params_for(:source, token: Faker.UUID.v4(), rules: []),
          u
        )

      project_id = GenUtils.get_project_id(s1.token)
      dataset_id = User.generate_bq_dataset_id(u)

      BigQuery.create_dataset(
        "#{u.id}",
        dataset_id,
        @test_dataset_location,
        project_id
      )

      BigQuerySchemaGS.start_link(%RLS{
        source_id: s1.token,
        plan: %{limit_source_fields_limit: 500}
      })

      BigQuerySchemaGS.start_link(%RLS{
        source_id: sink1.token,
        plan: %{limit_source_fields_limit: 500}
      })

      Source.BigQuery.Schema.start_link(%RLS{source_id: sink2.token, plan: @plan})

      BigQuery.create_table(s1.token, dataset_id, project_id, 300_000)

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

      Source.BigQuery.Schema.update(s1.token, schema)
      Source.BigQuery.Schema.update(sink1.token, schema)
      Source.BigQuery.Schema.update(sink2.token, schema)
      Process.sleep(100)

      {:ok, _lql_rule1} =
        Rules.create_rule(%{"sink" => sink1.token, "lql_string" => "warning"}, s1)

      {:ok, _lql_rule2} =
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
      users: [_u],
      sources: [s1],
      sinks: [first_sink, last_sink | _]
    } do
      _first_sink_rule =
        insert(:rule,
          sink: last_sink.token,
          lql_string: "test",
          lql_filters: [
            %Lql.FilterRule{operator: :"~", value: "test", modifiers: %{}, path: "event_message"}
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
              modifiers: %{},
              path: "event_message"
            }
          ],
          source_id: s1.id
        )

      log_params_batch = [
        %{"message" => "test"}
      ]

      s1 = Sources.get_by_and_preload(id: s1.id)

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
