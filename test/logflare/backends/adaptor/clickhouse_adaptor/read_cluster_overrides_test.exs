defmodule Logflare.Backends.Adaptor.ClickHouseAdaptor.ReadClusterOverridesTest do
  use Logflare.DataCase

  alias Logflare.Backends
  alias Logflare.Backends.Adaptor.ClickHouseAdaptor
  alias Logflare.Backends.Adaptor.ClickHouseAdaptor.ReadClusterOverrides
  alias Logflare.Endpoints.EndpointQuery

  @rule %{
    "match" => %{"endpoint_id" => 238, "label" => "api_paid", "project" => "example"},
    "read_cluster" => "quarantine",
    "note" => "incident response"
  }

  test "matches all conditions and keeps the first match" do
    fallback = %{"match" => %{"endpoint_name" => "logs"}, "read_cluster" => "alternate"}
    config = %{read_cluster_overrides: [@rule, fallback]}
    endpoint = %EndpointQuery{id: 238, name: "logs", parsed_labels: %{"project" => "example"}}

    assert {"quarantine", 1} = ReadClusterOverrides.match(config, endpoint, "api_paid")
    assert {"alternate", 2} = ReadClusterOverrides.match(config, endpoint, nil)

    assert {"alternate", 2} =
             ReadClusterOverrides.match(config, %{endpoint | parsed_labels: %{}}, "api_paid")

    refute ReadClusterOverrides.match(config, %{endpoint | name: "other", id: 123}, "api_paid")
    refute ReadClusterOverrides.match(config, nil, "api_paid")
  end

  test "only accepts configured clusters or classes and rejects malformed rules" do
    assert :ok = ReadClusterOverrides.validate([@rule], %{}, %{})
    rule = %{@rule | "read_cluster" => "api_free"}

    assert {:error, "Rule 1: target must have a read cluster or query-class policy"} =
             ReadClusterOverrides.validate([rule], %{}, %{"default" => %{"priority" => 5}})

    assert :ok = ReadClusterOverrides.validate([rule], %{"api_free" => "http://localhost"}, %{})
    assert :ok = ReadClusterOverrides.validate([rule], %{}, %{"api_free" => %{"priority" => 10}})

    for invalid <- [
          %{@rule | "match" => %{}},
          %{@rule | "match" => %{"endpoint_id" => "238"}},
          Map.put(@rule, "unexpected", true)
        ] do
      assert {:error, "Rule 1:" <> _} = ReadClusterOverrides.validate([invalid], %{}, %{})
    end
  end

  test "only a persisted administrator can save or remove routing rules" do
    insert(:plan)
    admin = insert(:user, admin: true)
    owner = insert(:user)

    backend =
      insert(:backend,
        user: owner,
        type: :clickhouse,
        config: %{url: "http://localhost:8123", port: 8123, database: "default"}
      )
      |> then(&Backends.get_backend(&1.id))

    assert {:error, :forbidden} =
             Backends.configure_read_cluster_overrides(%{owner | admin: true}, backend, [@rule])

    assert {:error, changeset} =
             Backends.update_backend(backend, %{config: %{read_cluster_overrides: [@rule]}})

    assert Keyword.has_key?(changeset.errors, :"config.read_cluster_overrides")

    assert {:ok, configured} = Backends.configure_read_cluster_overrides(admin, backend, [@rule])
    assert Backends.get_backend(backend.id).config.read_cluster_overrides == [@rule]

    refute Map.has_key?(
             ClickHouseAdaptor.redact_config(configured.config),
             :read_cluster_overrides
           )

    assert {:error, changeset} =
             Backends.update_backend(configured, %{config: %{read_cluster_overrides: []}})

    assert Keyword.has_key?(changeset.errors, :"config.read_cluster_overrides")
    assert {:ok, _} = Backends.configure_read_cluster_overrides(admin, configured, [])
  end

  test "executes matched endpoint reads with the final class on the default cluster" do
    insert(:plan)
    admin = insert(:user, admin: true)
    {_source, original} = setup_clickhouse_test()
    assert {:ok, backend} = Backends.configure_read_cluster_overrides(admin, original, [@rule])
    start_supervised!({ClickHouseAdaptor, backend})
    TestUtils.attach_forwarder([:logflare, :clickhouse, :read_cluster_override])

    endpoint = %EndpointQuery{id: 238, parsed_labels: %{"project" => "example"}}
    sql = "SELECT toUInt64(getSetting('max_threads')) AS threads"

    assert {:ok, %{rows: [%{"threads" => 2}]}} =
             ClickHouseAdaptor.execute_query(backend, {sql, [], %{}, endpoint},
               query_type: :endpoint,
               read_cluster: "api_paid"
             )

    backend_id = backend.id

    assert_receive {:telemetry_event, [:logflare, :clickhouse, :read_cluster_override],
                    %{count: 1},
                    %{
                      backend_id: ^backend_id,
                      requested_label: "api_paid",
                      final_label: "quarantine",
                      rule_index: 1
                    }}
  end
end
