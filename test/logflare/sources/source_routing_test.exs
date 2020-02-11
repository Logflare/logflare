defmodule Logflare.Logs.SourceRoutingTest do
  @moduledoc false
  use Logflare.DataCase
  use Placebo
  import Logflare.Factory
  alias Logflare.{Rules}
  alias Logflare.{Users}
  alias Logflare.Source
  alias Logflare.LogEvent, as: LE
  alias Logflare.Sources
  alias Logflare.Logs.SourceRouting
  alias Logflare.Source.BigQuery.SchemaBuilder
  alias Logflare.Google.BigQuery

  describe "LQL rules source routing" do
    test "regex routing successfull" do
      {:ok, _} = Source.Supervisor.start_link()
      u = Users.get_by_and_preload(email: System.get_env("LOGFLARE_TEST_USER_WITH_SET_IAM"))

      {:ok, s1} =
        params_for(:source, token: Faker.UUID.v4(), rules: [], user_id: u.id)
        |> Sources.create_source(u)

      {:ok, sink} =
        params_for(:source, token: Faker.UUID.v4(), rules: [], user_id: u.id)
        |> Sources.create_source(u)

      Process.sleep(1_000)

      schema =
        SchemaBuilder.build_table_schema(
          %{"request" => %{"url" => "/api/sources"}},
          SchemaBuilder.initial_table_schema()
        )

      {:ok, _} =
        BigQuery.patch_table(
          s1.token,
          schema,
          u.bigquery_dataset_id,
          u.bigquery_project_id || Application.get_env(:logflare, Logflare.Google)[:project_id]
        )

      Process.sleep(1_000)

      {:ok, rule} =
        Rules.create_rule(
          %{"lql_string" => ~S|"count: \d\d\d" m.request.url:~"sources$"|, "sink" => sink.token},
          s1
        )

      le =
        LE.make(
          %{
            "message" => "info count: 113",
            "metadata" => %{"request" => %{"url" => "/api/user/4/sources"}}
          },
          %{
            source: s1
          }
        )

      assert SourceRouting.route_with_lql_rules?(le, rule)

      le =
        LE.make(
          %{
            "message" => "info count: 113",
            "metadata" => %{"request" => %{"url" => "/api/user/4/sources$/4/5"}}
          },
          %{
            source: s1
          }
        )

      refute SourceRouting.route_with_lql_rules?(le, rule)
    end
  end
end
