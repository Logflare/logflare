defmodule Logflare.LogsTest do
  @moduledoc false
  use Logflare.DataCase
  use Logflare.Commons
  import Logflare.Factory
  alias Logflare.Source.BigQuery.Schema, as: BigQuerySchemaGS
  alias Logflare.Source.{BigQuery.Buffer}
  alias Logflare.Source.BigQuery.SchemaBuilder
  alias Logflare.Source.RecentLogsServer, as: RLS
  alias Logflare.Sources.Counters
  alias Logflare.Google.BigQuery
  alias Logflare.Google.BigQuery.GenUtils
  alias Logflare.SystemMetricsSup
  alias Ecto.Adapters.SQL.Sandbox
  @test_dataset_location "us-east4"
  @plan %{limit_source_fields_limit: 500}
  @moduletag :unboxed
  # @moduletag :this

  setup_all do
    Counters.start_link()
    :ok
  end

  describe "log event ingest for source with regex rules" do
    setup do
      {:ok, u} =
        Users.insert_or_update_user(
          params_for(:user, email: System.get_env("LOGFLARE_TEST_USER_2"))
        )

      u = Users.get_user(u.id)
      {:ok, sink1} = Sources.create_source(params_for(:source), u)
      {:ok, sink2} = Sources.create_source(params_for(:source), u)

      rule1 = build(:rule, sink: sink1.token, regex: "pattern2")
      rule2 = build(:rule, sink: sink2.token, regex: "pattern3")

      {:ok, s1} =
        Sources.create_source(
          params_for(:source, token: Faker.UUID.v4(), rules: [rule1, rule2]),
          u
        )

      {:ok, pid1} = BigQuerySchemaGS.start_link(%RLS{source_id: s1.token, plan: @plan})
      {:ok, pid2} = BigQuerySchemaGS.start_link(%RLS{source_id: sink1.token, plan: @plan})
      {:ok, pid3} = BigQuerySchemaGS.start_link(%RLS{source_id: sink2.token, plan: @plan})

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

    @tag :skip
    test "sink source routing", %{sources: [s1 | _], sinks: [sink1, sink2 | _]} do
      log_params_batch = [
        %{"message" => "pattern"},
        %{"message" => "pattern2"},
        %{"message" => "pattern3"}
      ]

      assert Logs.ingest_logs(log_params_batch, s1) == :ok
      assert Sources.refresh_source_metrics(s1).metrics.inserts == 3
      assert Buffer.get_count(s1) == 3

      assert Sources.refresh_source_metrics(sink1).metrics.inserts == 1
      assert Buffer.get_count(sink1) == 1

      assert Sources.refresh_source_metrics(sink2).metrics.inserts == 1
      assert Buffer.get_count(sink2) == 1
    end

    test "sink routing is allowed for one depth level only", %{
      users: [_u],
      sources: [s1],
      sinks: [first_sink, last_sink | _]
    } do
      _first_sink_rule =
        Rules.create_rule(
          string_params_for(:rule, sink: last_sink.token, lql_string: "test"),
          first_sink
        )

      _s1rule1 =
        Rules.create_rule(
          string_params_for(:rule, sink: first_sink.token, lql_string: "test"),
          s1
        )

      log_params_batch = [
        %{"message" => "test"}
      ]

      s1 = Sources.get_by_and_preload(id: s1.id)

      assert Logs.ingest_logs(log_params_batch, s1) == :ok

      assert Sources.refresh_source_metrics(s1).metrics.inserts == 1
      assert Buffer.get_count(s1) == 1

      assert Sources.refresh_source_metrics(first_sink).metrics.inserts == 1
      assert Buffer.get_count(first_sink) == 1

      assert Sources.refresh_source_metrics(last_sink).metrics.inserts == 0
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

      {:ok, sink1} =
        Sources.create_source(
          params_for(:source, token: Faker.UUID.v4()),
          u
        )

      {:ok, sink2} =
        Sources.create_source(
          params_for(:source, token: Faker.UUID.v4()),
          u
        )

      {:ok, s1} =
        Sources.create_source(
          params_for(:source, token: Faker.UUID.v4()),
          u
        )

      s1 = Sources.preload_defaults(s1)

      project_id = GenUtils.get_project_id(s1.token)
      dataset_id = User.generate_bq_dataset_id(u)

      BigQuery.create_dataset(
        "#{u.id}",
        dataset_id,
        @test_dataset_location,
        project_id
      )

      {:ok, pid1} =
        BigQuerySchemaGS.start_link(%RLS{
          source_id: s1.token,
          plan: %{limit_source_fields_limit: 500}
        })

      {:ok, pid2} =
        BigQuerySchemaGS.start_link(%RLS{
          source_id: sink1.token,
          plan: %{limit_source_fields_limit: 500}
        })

      {:ok, pid3} = Source.BigQuery.Schema.start_link(%RLS{source_id: sink2.token, plan: @plan})

      Sandbox.allow(Repo, self(), pid1)
      Sandbox.allow(Repo, self(), pid2)
      Sandbox.allow(Repo, self(), pid3)

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

      :ok = Source.BigQuery.Schema.update(s1.token, schema)
      :ok = Source.BigQuery.Schema.update(sink1.token, schema)
      :ok = Source.BigQuery.Schema.update(sink2.token, schema)
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

      assert Sources.refresh_source_metrics(s1).metrics.inserts == 5
      assert Buffer.get_count(s1) == 5

      assert Sources.refresh_source_metrics(sink1).metrics.inserts == 2
      assert Buffer.get_count(sink1) == 2

      assert Sources.refresh_source_metrics(sink2).metrics.inserts == 1
      assert Buffer.get_count(sink2) == 1
    end

    @tag :this
    test "sink routing is allowed for one depth level only", %{
      users: [_u],
      sources: [s1],
      sinks: [first_sink, last_sink | _]
    } do
      {:ok, _rule} =
        Rules.create_rule(%{"lql_string" => "~test", "sink" => last_sink.token}, first_sink)

      {:ok, _rule} = Rules.create_rule(%{"lql_string" => "~test", "sink" => first_sink.token}, s1)

      log_params_batch = [
        %{"message" => "test"}
      ]

      s1 = Sources.get_by_and_preload(id: s1.id)
      first_sink = Sources.get_by_and_preload(id: first_sink.id)
      last_sink = Sources.get_by_and_preload(id: last_sink.id)

      assert Logs.ingest_logs(log_params_batch, s1) == :ok

      assert Sources.refresh_source_metrics(s1).metrics.inserts == 1
      assert Buffer.get_count(s1) == 1

      assert Sources.refresh_source_metrics(first_sink).metrics.inserts == 1
      assert Buffer.get_count(first_sink) == 1

      assert Sources.refresh_source_metrics(last_sink).metrics.inserts == 0
      assert Buffer.get_count(last_sink) == 0
    end
  end
end
