defmodule Logflare.Backends.QueryClassSettingsTest do
  use Logflare.DataCase

  alias Logflare.Backends
  alias Logflare.Backends.Adaptor
  alias Logflare.Backends.Backend
  alias Logflare.Cluster.Utils, as: ClusterUtils
  alias Logflare.ContextCache
  alias Logflare.QueryContext
  alias Logflare.Repo

  @policy %{
    "default" => %{"priority" => 5},
    "api_paid" => %{"priority" => 10, "max_threads" => 4}
  }

  setup do
    insert(:plan)
    owner = insert(:user)
    admin = insert(:user, admin: true)

    backend =
      insert(:backend,
        user: owner,
        type: :clickhouse,
        config: %{url: "http://localhost:8123", port: 8123, database: "default"}
      )
      |> then(&Backends.get_backend(&1.id))

    %{owner: owner, admin: admin, backend: backend}
  end

  test "policy updates multicast fenced query-context evictions", context do
    %{admin: admin, backend: backend, owner: owner} = context
    refute Map.has_key?(QueryContext.get_backend(backend.id).config, :query_class_settings)
    assert [_] = QueryContext.list_backends(owner.id)

    test_pid = self()

    expect(ClusterUtils, :rpc_multicast, fn ContextCache, :bust_keys, [keys] ->
      send(test_pid, {:policy_evictions, keys})
      ContextCache.bust_keys(keys)
    end)

    assert {:ok, _} =
             Repo.transact(fn ->
               result = Backends.configure_query_class_settings(admin, backend, @policy)
               refute_received {:policy_evictions, _}
               result
             end)

    assert_received {:policy_evictions, keys}
    assert {QueryContext, [backend_id: backend.id, owner_id: owner.id]} in keys
    assert QueryContext.get_backend(backend.id).config.query_class_settings == @policy
    assert [cached] = QueryContext.list_backends(owner.id)
    assert cached.config.query_class_settings == @policy
  end

  test "policy saves skip source preloads and adaptor config notifications", context do
    reject(&Adaptor.on_backend_config_changed/1)

    assert {:ok, updated} =
             Backends.configure_query_class_settings(context.owner, context.backend, @policy)

    assert updated.config.query_class_settings == @policy
    refute Ecto.assoc_loaded?(updated.sources)
  end

  test "rolled-back policy updates do not multicast uncommitted evictions", context do
    %{admin: admin, backend: backend} = context
    reject(&ClusterUtils.rpc_multicast/3)
    snapshot = QueryContext.get_backend(backend.id)

    assert {:error, :cancelled} =
             Repo.transact(fn ->
               assert {:ok, _} = Backends.configure_query_class_settings(admin, backend, @policy)
               Repo.rollback(:cancelled)
             end)

    assert QueryContext.get_backend(backend.id) == snapshot
  end

  test "policy is validated, persisted and protected from ordinary backend changes", context do
    %{owner: owner, admin: admin, backend: backend} = context
    outsider = insert(:user)

    assert {:error, :forbidden} =
             Backends.configure_query_class_settings(outsider, backend, @policy)

    assert {:error, :forbidden} =
             Backends.configure_query_class_settings(%{outsider | admin: true}, backend, @policy)

    assert {:error, changeset} =
             Backends.update_backend(backend, %{config: %{query_class_settings: @policy}})

    assert Keyword.has_key?(changeset.errors, :"config.query_class_settings")

    assert {:ok, configured} = Backends.configure_query_class_settings(owner, backend, @policy)
    assert configured.config.query_class_settings == @policy

    assert {:ok, configured} = Backends.configure_query_class_settings(admin, backend, @policy)
    assert configured.config.query_class_settings == @policy
    assert Backends.get_backend(backend.id).config.query_class_settings == @policy

    refute Map.has_key?(
             Jason.decode!(Jason.encode!(configured))["config"],
             "query_class_settings"
           )

    for policy <- [%{}, nil, %{"default" => %{"priority" => 1}}] do
      assert {:error, _} =
               Backends.update_backend(configured, %{
                 "config" => %{"query_class_settings" => policy}
               })
    end

    changeset =
      Backend.changeset(configured, %{
        type: :postgres,
        config: %{url: "postgresql://localhost/logflare"}
      })

    assert Keyword.has_key?(changeset.errors, :"config.query_class_settings")

    assert {:ok, updated} = Backends.update_backend(backend, %{config: %{read_pool_size: 4}})
    assert Map.get(updated.config, :query_class_settings) == @policy
    assert {:ok, _} = Backends.update_backend(updated, %{name: "renamed"})

    assert {:error, _} =
             Backends.configure_query_class_settings(admin, backend, %{
               "default" => %{"priority" => 0}
             })

    assert Backends.get_backend(backend.id).config.query_class_settings == @policy
    assert {:ok, cleared} = Backends.configure_query_class_settings(admin, backend, %{})
    assert cleared.config.query_class_settings == %{}
  end

  for {kind, attrs} <- [
        {"name", %{name: "renamed"}},
        {"config", %{config: %{read_pool_size: 4}}},
        {"default ingest", %{default_ingest?: true}},
        {"alerts", %{alert_queries: []}}
      ] do
    @tag update_attrs: attrs
    test "deleted backends return changeset errors for #{kind} updates", context do
      Repo.delete!(context.backend)

      assert {:error, %Ecto.Changeset{} = changeset} =
               Backends.update_backend(context.backend, context.update_attrs)

      assert changeset.data.id == context.backend.id
      assert errors_on(changeset).base == ["Backend no longer exists"]
    end
  end

  test "ordinary connection edits preserve a legacy timeout rejected by explicit policy writes",
       %{backend: backend, owner: owner} do
    policy = %{
      "default" => %{
        "priority" => 5,
        "max_execution_time" => 0.0000005,
        "timeout_overflow_mode" => "throw"
      }
    }

    persisted = Repo.reload!(backend)

    persisted
    |> Ecto.Changeset.change(
      config_encrypted: Map.put(persisted.config_encrypted, "query_class_settings", policy)
    )
    |> Repo.update!()

    assert {:ok, _} = Backends.update_backend(backend, %{config: %{database: "renamed"}})

    assert {:error, changeset} = Backends.configure_query_class_settings(owner, backend, policy)
    assert [message] = errors_on(changeset)[:"config.query_class_settings"]
    assert message =~ "positive signed 64-bit microsecond duration"

    assert %{"database" => "renamed", "query_class_settings" => ^policy} =
             Repo.reload!(backend).config_encrypted
  end

  test "invalid ordinary saves do not add a misleading policy authorization error", context do
    assert {:ok, configured} =
             Backends.configure_query_class_settings(context.admin, context.backend, @policy)

    for attrs <- [%{name: ""}, %{config: %{url: "not a URL"}}] do
      assert {:error, changeset} = Backends.update_backend(configured, attrs)
      refute changeset.valid?
      refute Keyword.has_key?(changeset.errors, :"config.query_class_settings")
      assert Backends.get_backend(configured.id).config.query_class_settings == @policy
    end
  end

  test "configuration checks the locked backend and returns changeset validation errors",
       context do
    reject(Backends, :get_backend, 1)

    assert {:ok, configured} =
             Backends.configure_query_class_settings(context.admin, context.backend, @policy)

    assert {:error, %Ecto.Changeset{} = changeset} =
             Backends.configure_query_class_settings(context.admin, configured, %{
               "default" => %{"priority" => 0}
             })

    assert Keyword.has_key?(changeset.errors, :"config.query_class_settings")

    Repo.delete!(context.backend)

    assert {:error, :not_found} =
             Backends.configure_query_class_settings(context.admin, configured, @policy)
  end

  test "create and schema changesets cannot introduce operator policy", %{
    owner: owner,
    backend: backend
  } do
    attrs = %{
      name: "customer",
      type: :clickhouse,
      config: Map.put(backend.config, :query_class_settings, @policy)
    }

    assert {:error, changeset} = Backends.create_backend(owner, attrs)
    assert Keyword.has_key?(changeset.errors, :"config.query_class_settings")
    refute Backend.changeset(backend, %{config: %{query_class_settings: @policy}}).valid?
  end

  test "owners and team members can save and clear policy", context do
    team = insert(:team, user: context.owner)
    member = insert(:team_user, team: team)

    for actor <- [context.owner, member] do
      assert {:ok, configured} =
               Backends.configure_query_class_settings(actor, context.backend, @policy)

      assert configured.config.query_class_settings == @policy

      assert {:ok, cleared} =
               Backends.configure_query_class_settings(actor, configured, %{})

      assert cleared.config.query_class_settings == %{}
    end
  end

  test "deleted and moved team memberships are rechecked on every save", context do
    team = insert(:team, user: context.owner)
    member = insert(:team_user, team: team)
    other_team = insert(:team)

    member |> Ecto.Changeset.change(team_id: other_team.id) |> Repo.update!()

    assert {:error, :forbidden} =
             Backends.configure_query_class_settings(member, context.backend, @policy)

    Repo.delete!(member)

    assert {:error, :forbidden} =
             Backends.configure_query_class_settings(member, context.backend, @policy)
  end

  test "team user ids cannot be interpreted as platform-admin ids", context do
    team = insert(:team)
    member = insert(:team_user, id: context.admin.id, team: team)

    assert {:error, :forbidden} =
             Backends.configure_query_class_settings(member, context.backend, @policy)
  end

  test "authorization uses the current backend owner rather than the caller's snapshot",
       context do
    next_owner = insert(:user)
    context.backend |> Ecto.Changeset.change(user_id: next_owner.id) |> Repo.update!()

    assert {:error, :forbidden} =
             Backends.configure_query_class_settings(context.owner, context.backend, @policy)

    assert {:ok, configured} =
             Backends.configure_query_class_settings(next_owner, context.backend, @policy)

    assert configured.config.query_class_settings == @policy
  end

  test "revoked administrators and non-ClickHouse backends cannot configure policy", context do
    %{admin: admin, backend: backend} = context
    other = insert(:backend, user: context.owner, type: :bigquery)

    assert {:error, :not_clickhouse_backend} =
             Backends.configure_query_class_settings(admin, other, @policy)

    admin |> Ecto.Changeset.change(admin: false) |> Repo.update!()
    assert {:error, :forbidden} = Backends.configure_query_class_settings(admin, backend, @policy)
  end
end
