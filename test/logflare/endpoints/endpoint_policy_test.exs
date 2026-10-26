defmodule Logflare.Endpoints.EndpointPolicyTest do
  use Logflare.DataCase

  alias Logflare.Endpoints
  alias Logflare.Repo
  alias PaperTrail.Version

  setup do
    owner = insert(:user)
    admin = insert(:user, admin: true)
    backend = insert(:backend, user: owner, type: :clickhouse)

    endpoint =
      insert(:endpoint,
        user: owner,
        backend: backend,
        language: :ch_sql,
        query: "SELECT 1 AS n"
      )

    %{owner: owner, admin: admin, endpoint: endpoint}
  end

  test "policy changes audit the actual operator and authoritative previous settings", context do
    member = insert(:team_user, team: context.owner.team, email: context.admin.email)

    for {settings, previous} <- [
          {%{"max_rows_to_read" => 10}, %{}},
          {%{"max_execution_time" => 7},
           %{"max_rows_to_read" => 10, "read_overflow_mode" => "throw"}},
          {%{}, %{"max_execution_time" => 7, "timeout_overflow_mode" => "throw"}}
        ] do
      assert {:ok, current} =
               Endpoints.configure_enforced_clickhouse_settings(
                 member,
                 context.endpoint,
                 settings
               )

      audit =
        Version
        |> where([v], v.item_type == "EndpointQuery" and v.item_id == ^current.id)
        |> order_by([v], desc: v.id)
        |> limit(1)
        |> Repo.one!()

      assert audit.origin == context.admin.email
      assert audit.event == "update"

      assert audit.item_changes["enforced_clickhouse_settings"] ==
               current.enforced_clickhouse_settings

      assert audit.meta == %{
               "kind" => "endpoint_policy",
               "operator_id" => context.admin.id,
               "previous_settings" => previous,
               "settings" => current.enforced_clickhouse_settings
             }
    end
  end

  test "scoped replacements preserve authoritative settings and audit the merged policy",
       context do
    previous = %{"priority" => 9, "max_threads" => 1, "max_memory_usage" => 1000}

    assert {:ok, _} =
             Endpoints.configure_enforced_clickhouse_settings(
               context.admin,
               context.endpoint,
               previous
             )

    assert {:ok, updated} =
             Endpoints.configure_enforced_clickhouse_settings(
               context.admin,
               context.endpoint,
               %{"max_memory_usage" => 500},
               replace_keys: ["max_memory_usage"]
             )

    assert updated.enforced_clickhouse_settings == Map.put(previous, "max_memory_usage", 500)

    audit =
      Version
      |> where([v], v.item_type == "EndpointQuery" and v.item_id == ^updated.id)
      |> order_by([v], desc: v.id)
      |> limit(1)
      |> Repo.one!()

    assert audit.meta["previous_settings"] == previous
    assert audit.meta["settings"] == updated.enforced_clickhouse_settings

    assert {:ok, cleared} =
             Endpoints.configure_enforced_clickhouse_settings(context.admin, updated, %{})

    assert cleared.enforced_clickhouse_settings == %{}
  end

  test "scoped replacements still require persisted admin access and valid settings", context do
    settings = %{"priority" => 3, "max_threads" => 2}

    assert {:ok, endpoint} =
             Endpoints.configure_enforced_clickhouse_settings(
               context.admin,
               context.endpoint,
               settings
             )

    opts = [replace_keys: ["max_memory_usage"]]

    assert {:error, :forbidden} =
             Endpoints.configure_enforced_clickhouse_settings(context.owner, endpoint, %{}, opts)

    assert {:error, _} =
             Endpoints.configure_enforced_clickhouse_settings(
               context.admin,
               endpoint,
               %{"max_memory_usage" => 0.5},
               opts
             )

    assert Endpoints.get_endpoint_query(endpoint.id).enforced_clickhouse_settings == settings
    assert Repo.aggregate(Version, :count) == 1
  end

  test "policy audits neither advance SQL versions nor make the current SQL restorable",
       context do
    assert {:ok, first} =
             Endpoints.update_query(
               context.owner,
               context.endpoint,
               %{description: "first"},
               context.owner
             )

    assert {:ok, _} =
             Endpoints.configure_enforced_clickhouse_settings(context.admin, first, %{
               "max_execution_time" => 7
             })

    assert Endpoints.get_endpoint_query_version(first.id, 1)
    assert Endpoints.get_endpoint_query_version(first.id, 2) == nil

    assert {:error, changeset} =
             Endpoints.restore_query_version(context.owner, first, 1, context.owner)

    assert "Version is already current" in errors_on(changeset).base

    assert {:ok, second} =
             Endpoints.update_query(context.owner, first, %{description: "second"}, context.owner)

    assert Endpoints.get_endpoint_query_version(second.id, 2)

    assert {:ok, _} =
             Endpoints.configure_enforced_clickhouse_settings(context.admin, second, %{
               "max_execution_time" => 3
             })

    assert {:ok, restored, 1} =
             Endpoints.restore_query_version(context.owner, second, 1, context.owner)

    assert restored.description == "first"

    assert restored.enforced_clickhouse_settings == %{
             "max_execution_time" => 3,
             "timeout_overflow_mode" => "throw"
           }

    assert Endpoints.get_endpoint_query_version(restored.id, 3)
    assert Endpoints.get_endpoint_query_version(restored.id, 4) == nil
  end

  test "ordinary backend changes preserve dormant policy and reactivate it on ClickHouse",
       context do
    postgres = insert(:postgres_backend, user: context.owner)

    assert {:ok, limited} =
             Endpoints.configure_enforced_clickhouse_settings(context.admin, context.endpoint, %{
               "max_execution_time" => 7
             })

    assert {:ok, dormant} =
             Endpoints.update_query(
               context.owner,
               limited,
               %{backend_id: postgres.id},
               context.owner
             )

    assert dormant.language == :pg_sql

    assert dormant.enforced_clickhouse_settings == %{
             "max_execution_time" => 7,
             "timeout_overflow_mode" => "throw"
           }

    assert {:ok, sql} = Endpoints.get_transformed_query(dormant)
    refute sql =~ "max_execution_time"

    assert {:ok, returned} =
             Endpoints.update_query(
               context.owner,
               dormant,
               %{backend_id: limited.backend_id},
               context.owner
             )

    assert {:ok, sql} = Endpoints.get_transformed_query(returned)
    assert sql =~ "max_execution_time = 7"
  end

  test "only admins can clear dormant policy and nonempty settings still require ClickHouse",
       context do
    postgres = insert(:postgres_backend, user: context.owner)

    assert {:ok, limited} =
             Endpoints.configure_enforced_clickhouse_settings(context.admin, context.endpoint, %{
               "max_execution_time" => 7
             })

    assert {:ok, dormant} =
             Endpoints.update_query(
               context.owner,
               limited,
               %{backend_id: postgres.id},
               context.owner
             )

    assert {:error, :forbidden} =
             Endpoints.configure_enforced_clickhouse_settings(context.owner, dormant, %{})

    assert {:error, :not_clickhouse_endpoint} =
             Endpoints.configure_enforced_clickhouse_settings(context.admin, dormant, %{
               "max_execution_time" => 9
             })

    assert {:ok, cleared} =
             Endpoints.configure_enforced_clickhouse_settings(context.admin, dormant, %{})

    assert cleared.enforced_clickhouse_settings == %{}

    assert {:ok, returned} =
             Endpoints.update_query(
               context.owner,
               cleared,
               %{backend_id: limited.backend_id},
               context.owner
             )

    assert {:ok, sql} = Endpoints.get_transformed_query(returned)
    refute sql =~ "max_execution_time"
  end

  test "invalid and unauthorized changes do not create policy audit records", context do
    assert {:error, :forbidden} =
             Endpoints.configure_enforced_clickhouse_settings(
               context.owner,
               context.endpoint,
               %{}
             )

    assert {:error, _} =
             Endpoints.configure_enforced_clickhouse_settings(context.admin, context.endpoint, %{
               "max_execution_time" => 0
             })

    assert Repo.aggregate(Version, :count) == 0
    assert Endpoints.get_endpoint_query(context.endpoint.id).enforced_clickhouse_settings == %{}
  end
end
