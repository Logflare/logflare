defmodule Logflare.ContextCache.CacheBusterTest do
  use Logflare.DataCase

  alias Cainophile.Changes.DeletedRecord
  alias Cainophile.Changes.NewRecord
  alias Cainophile.Changes.Transaction
  alias Cainophile.Changes.UpdatedRecord
  alias Logflare.Alerting
  alias Logflare.Backends
  alias Logflare.Billing
  alias Logflare.ContextCache
  alias Logflare.ContextCache.CacheBuster
  alias Logflare.ContextCache.Tombstones
  alias Logflare.Endpoints
  alias Logflare.Sources
  alias Logflare.Sources.Catalog
  alias Logflare.Users

  setup do
    plan = insert(:plan, name: "Free")
    user = insert(:user)
    source = insert(:source, user: user)

    for child_spec <- ContextCache.Supervisor.buster_specs() do
      start_supervised!(child_spec)
    end

    Cachex.clear!(Alerting.Cache)
    Cachex.clear!(Catalog.Cache)
    Cachex.clear!(Endpoints.Cache)

    on_exit(fn ->
      Cachex.clear!(Alerting.Cache)
      Cachex.clear!(Catalog.Cache)
      Cachex.clear!(Endpoints.Cache)
    end)

    [plan: plan, source: source, user: user]
  end

  test "cache buster", %{source: %{id: source_id, token: source_token}} do
    Sources.Cache.get_by(token: source_token)
    assert Cachex.size!(Sources.Cache) == 1

    change = %DeletedRecord{
      relation: {"public", "sources"},
      old_record: %{"id" => Integer.to_string(source_id)}
    }

    test_pid = self()

    Mimic.expect(ContextCache, :bust_keys, fn arg ->
      Mimic.call_original(ContextCache, :bust_keys, [arg])
      send(test_pid, arg)
    end)

    send(CacheBuster, %Transaction{changes: [change]})
    assert_receive [{Sources, ^source_id}], 500
    assert Cachex.size!(Sources.Cache) == 0
  end

  test "user writes invalidate only that user's plan", %{user: user, plan: plan} do
    other_user = insert(:user)
    assert Billing.Cache.get_plan_by_user(user) == plan
    assert Billing.Cache.get_plan_by_user(other_user) == plan
    Users.Cache.get(user.id)

    user = Repo.update!(Ecto.Changeset.change(user, billing_enabled: false))

    process_wal(%UpdatedRecord{
      relation: {"public", "users"},
      record: %{"id" => Integer.to_string(user.id)}
    })

    assert Cachex.get!(Users.Cache, {:get, [user.id]}) == nil
    assert Billing.Cache.get_plan_by_user(user).name == "Legacy"
    assert Cachex.get!(Billing.Cache, {:get_plan_by_user, [other_user.id]}) == {:cached, plan}

    Users.Cache.get(user.id)

    process_wal(%DeletedRecord{
      relation: {"public", "users"},
      old_record: %{"id" => Integer.to_string(user.id)}
    })

    assert Cachex.get!(Users.Cache, {:get, [user.id]}) == nil
    assert Cachex.get!(Billing.Cache, {:get_plan_by_user, [user.id]}) == nil
    assert Cachex.get!(Billing.Cache, {:get_plan_by_user, [other_user.id]}) == {:cached, plan}
  end

  test "billing account lifecycle invalidates plans and cached account misses", %{user: user} do
    lifetime = insert(:plan, name: "Lifetime")
    other_user = insert(:user)
    other_account = insert(:billing_account, user: other_user, lifetime_plan: true)
    other_account_id = other_account.id
    assert Billing.Cache.get_plan_by_user(other_user) == lifetime
    assert Billing.Cache.get_billing_account_by(user_id: other_user.id).id == other_account_id
    assert Billing.Cache.get_billing_account_by(user_id: user.id) == nil
    assert Billing.Cache.get_plan_by_user(user).name == "Free"
    free = Billing.Cache.get_plan_by(name: "Free")

    account = insert(:billing_account, user: user, lifetime_plan: true)

    process_wal(%NewRecord{
      relation: {"public", "billing_accounts"},
      record: %{
        "id" => Integer.to_string(account.id),
        "user_id" => Integer.to_string(user.id)
      }
    })

    assert Billing.Cache.get_billing_account_by(user_id: user.id).id == account.id
    assert Billing.Cache.get_plan_by_user(user) == lifetime
    assert Cachex.get!(Billing.Cache, {:get_plan_by_user, [other_user.id]}) == {:cached, lifetime}

    assert {:cached, %{id: ^other_account_id}} =
             Cachex.get!(Billing.Cache, {:get_billing_account_by, [[user_id: other_user.id]]})

    account =
      Repo.update!(
        Ecto.Changeset.change(account, lifetime_plan: false, stripe_subscriptions: nil)
      )

    process_wal(%UpdatedRecord{
      relation: {"public", "billing_accounts"},
      record: %{
        "id" => Integer.to_string(account.id),
        "user_id" => Integer.to_string(user.id)
      }
    })

    refute Billing.Cache.get_billing_account_by(user_id: user.id).lifetime_plan
    assert Billing.Cache.get_plan_by_user(user).name == "Free"
    assert Tombstones.Cache.tombstoned?(Billing.Cache, account.id)
    assert Cachex.get!(Billing.Cache, {:get_plan_by_user, [other_user.id]}) == {:cached, lifetime}

    assert {:cached, %{id: ^other_account_id}} =
             Cachex.get!(Billing.Cache, {:get_billing_account_by, [[user_id: other_user.id]]})

    Cachex.del!(Tombstones.Cache, {Billing.Cache, account.id})
    Repo.delete!(account)

    process_wal(%DeletedRecord{
      relation: {"public", "billing_accounts"},
      old_record: %{"id" => Integer.to_string(account.id)}
    })

    assert Tombstones.Cache.tombstoned?(Billing.Cache, account.id)
    assert Cachex.get!(Billing.Cache, {:get_plan_by_user, [user.id]}) == nil
    assert Billing.Cache.get_billing_account_by(user_id: user.id) == nil
    assert Cachex.get!(Billing.Cache, {:get_plan_by_user, [other_user.id]}) == nil

    assert Cachex.get!(Billing.Cache, {:get_billing_account_by, [[user_id: other_user.id]]}) ==
             nil

    assert Cachex.get!(Billing.Cache, {:get_plan_by, [[name: "Free"]]}) == {:cached, free}
  end

  test "plan writes invalidate every user of that plan", %{user: user, plan: plan} do
    other_user = insert(:user)
    enterprise = insert(:plan, name: "Enterprise")
    upgraded_user = insert(:user, partner_upgraded: true)
    assert Billing.Cache.get_plan_by_user(user) == plan
    assert Billing.Cache.get_plan_by_user(other_user) == plan
    assert Billing.Cache.get_plan_by_user(upgraded_user) == enterprise

    plan = Repo.update!(Ecto.Changeset.change(plan, limit_source_ttl: 123_456))

    process_wal(%UpdatedRecord{
      relation: {"public", "plans"},
      record: %{"id" => Integer.to_string(plan.id)}
    })

    assert Billing.Cache.get_plan_by_user(user).limit_source_ttl == 123_456
    assert Billing.Cache.get_plan_by_user(other_user).limit_source_ttl == 123_456

    assert Cachex.get!(Billing.Cache, {:get_plan_by_user, [upgraded_user.id]}) ==
             {:cached, enterprise}

    Repo.delete!(plan)

    process_wal(%DeletedRecord{
      relation: {"public", "plans"},
      old_record: %{"id" => Integer.to_string(plan.id)}
    })

    assert Cachex.get!(Billing.Cache, {:get_plan_by_user, [user.id]}) == nil
    assert Cachex.get!(Billing.Cache, {:get_plan_by_user, [other_user.id]}) == nil
  end

  test "a default backend update refreshes the owner's default ingest lookup from the WAL user_id",
       %{
         user: user
       } do
    stub(Backends, :sync_backend_across_cluster, fn _backend_id -> :ok end)

    backend =
      insert(:backend,
        user: user,
        type: :clickhouse,
        config: %{url: "http://localhost", database: "test", port: 8123}
      )

    assert [] = Backends.Cache.list_default_ingest_backends(user.id)
    backend |> Ecto.Changeset.change(default_ingest?: true) |> Repo.update!()

    change = %UpdatedRecord{
      relation: {"public", "backends"},
      record: %{
        "id" => Integer.to_string(backend.id),
        "user_id" => Integer.to_string(user.id),
        "default_ingest" => "t"
      }
    }

    send(CacheBuster, %Transaction{changes: [change]})

    TestUtils.retry_assert(fn ->
      assert [%{id: id}] = Backends.Cache.list_default_ingest_backends(user.id)
      assert id == backend.id
    end)
  end

  test "a non-default backend update does not refresh the owner's default ingest lookup", %{
    user: user
  } do
    stub(Backends, :sync_backend_across_cluster, fn _backend_id -> :ok end)

    backend =
      insert(:backend,
        user: user,
        type: :webhook,
        config: %{url: "http://test.com"}
      )

    assert [] = Backends.Cache.list_default_ingest_backends(user.id)
    test_pid = self()

    stub(Backends, :refresh_default_ingest_backends_cache, fn user_id ->
      send(test_pid, {:refreshed, user_id})
      :ok
    end)

    Mimic.expect(ContextCache, :bust_keys, fn arg ->
      result = Mimic.call_original(ContextCache, :bust_keys, [arg])
      send(test_pid, {:busted, arg})
      result
    end)

    change = %UpdatedRecord{
      relation: {"public", "backends"},
      record: %{
        "id" => Integer.to_string(backend.id),
        "user_id" => Integer.to_string(user.id),
        "default_ingest" => "f"
      }
    }

    send(CacheBuster, %Transaction{changes: [change]})

    backend_id = backend.id
    assert_receive {:busted, [{Backends, ^backend_id}]}, 500
    refute_receive {:refreshed, _user_id}, 300
  end

  test "alert changes bust alert catalogs", %{user: %{id: user_id} = user} do
    alert = insert(:alert, user: user)
    Alerting.Cache.list_by_user_id(user_id)

    alert_id = alert.id
    id = Integer.to_string(alert_id)
    user_id_string = Integer.to_string(user_id)

    changes = [
      %UpdatedRecord{
        relation: {"public", "alert_queries"},
        record: %{"id" => id},
        old_record: %{}
      },
      %NewRecord{
        relation: {"public", "alert_queries"},
        record: %{"id" => id, "user_id" => user_id_string}
      },
      %DeletedRecord{
        relation: {"public", "alert_queries"},
        old_record: %{"id" => id}
      }
    ]

    test_pid = self()

    Mimic.expect(ContextCache, :bust_keys, fn arg ->
      Mimic.call_original(ContextCache, :bust_keys, [arg])
      send(test_pid, arg)
    end)

    send(CacheBuster, %Transaction{changes: changes})

    assert_receive [
                     {Alerting, ^alert_id},
                     {Alerting, [user_id: ^user_id]},
                     {Alerting, ^alert_id}
                   ],
                   500

    assert Cachex.size!(Alerting.Cache) == 0
  end

  test "endpoint inserts bust negative lookups and owner catalogs", %{user: %{id: user_id} = user} do
    endpoint = insert(:endpoint, user: user)
    Endpoints.Cache.list_by_user_id(user_id)

    change = %NewRecord{
      relation: {"public", "endpoint_queries"},
      record: %{"id" => Integer.to_string(endpoint.id), "user_id" => Integer.to_string(user_id)}
    }

    test_pid = self()

    Mimic.expect(ContextCache, :bust_keys, fn arg ->
      Mimic.call_original(ContextCache, :bust_keys, [arg])
      send(test_pid, arg)
    end)

    send(CacheBuster, %Transaction{changes: [change]})

    assert_receive [
                     {Endpoints, :not_found},
                     {Endpoints, [user_id: ^user_id]}
                   ],
                   500

    assert Cachex.size!(Endpoints.Cache) == 0
  end

  test "source updates bust the entity and owner catalog", %{
    source: %{id: source_id, token: source_token},
    user: %{id: user_id} = user
  } do
    Sources.Cache.get_by(token: source_token)
    Catalog.Cache.list_by_user(user)

    change = %UpdatedRecord{
      relation: {"public", "sources"},
      record: %{"id" => Integer.to_string(source_id), "user_id" => Integer.to_string(user_id)},
      old_record: %{}
    }

    assert_source_catalog_bust(change, source_id, user_id)
    assert Cachex.size!(Sources.Cache) == 0
    assert Cachex.size!(Catalog.Cache) == 0
  end

  for field <- ~w(log_events_updated_at updated_at description favorite) do
    @field field

    test "source #{@field} updates preserve the catalog", %{source: source, user: user} do
      Catalog.Cache.list_by_user(user.id)
      record = source_record(source)

      change = %UpdatedRecord{
        relation: {"public", "sources"},
        old_record: Map.put(record, @field, "old"),
        record: Map.put(record, @field, "new")
      }

      assert [{Sources, source_id}] = bust_records(change)
      assert source_id == source.id
      assert Cachex.size!(Catalog.Cache) == 1
    end
  end

  for field <- ~w(name token user_id) do
    @field field

    test "source #{@field} updates invalidate the catalog", %{source: source, user: user} do
      Catalog.Cache.list_by_user(user.id)
      record = source_record(source)
      changed = if @field == "user_id", do: to_string(user.id + 1), else: "changed"

      change = %UpdatedRecord{
        relation: {"public", "sources"},
        old_record: record,
        record: Map.put(record, @field, changed)
      }

      assert {Catalog, [user_id: user.id]} in bust_records(change)
      assert Cachex.size!(Catalog.Cache) == 0
    end
  end

  test "source transfers invalidate both owner catalogs", %{source: source, user: previous_owner} do
    next_owner = insert(:user)
    insert(:source, user: next_owner)
    Catalog.Cache.list_by_user(previous_owner.id)
    Catalog.Cache.list_by_user(next_owner.id)
    source |> Ecto.Changeset.change(user_id: next_owner.id) |> Repo.update!()

    change = %UpdatedRecord{
      relation: {"public", "sources"},
      record: %{"id" => to_string(source.id), "user_id" => to_string(next_owner.id)},
      old_record: %{"id" => to_string(source.id), "user_id" => to_string(previous_owner.id)}
    }

    test_pid = self()

    Mimic.expect(ContextCache, :bust_keys, fn records ->
      result = Mimic.call_original(ContextCache, :bust_keys, [records])
      send(test_pid, {:busted, records})
      result
    end)

    send(CacheBuster, %Transaction{changes: [change]})
    assert_receive {:busted, records}, 500
    assert {Catalog, [user_id: previous_owner.id]} in records
    assert {Catalog, [user_id: next_owner.id]} in records
    assert [] = Catalog.Cache.list_by_user(previous_owner.id)
    assert length(Catalog.Cache.list_by_user(next_owner.id)) == 2
  end

  test "source inserts bust negative entity lookups and owner catalogs", %{
    source: %{id: source_id},
    user: %{id: user_id}
  } do
    change = %NewRecord{
      relation: {"public", "sources"},
      record: %{"id" => Integer.to_string(source_id), "user_id" => Integer.to_string(user_id)}
    }

    assert_source_catalog_bust(change, :not_found, user_id)
  end

  test "source deletes bust the entity and owner catalog", %{
    source: %{id: source_id},
    user: %{id: user_id}
  } do
    change = %DeletedRecord{
      relation: {"public", "sources"},
      old_record: %{"id" => Integer.to_string(source_id), "user_id" => Integer.to_string(user_id)}
    }

    assert_source_catalog_bust(change, source_id, user_id)
  end

  defp source_record(source) do
    %{
      "id" => to_string(source.id),
      "user_id" => to_string(source.user_id),
      "name" => source.name,
      "token" => to_string(source.token)
    }
  end

  defp bust_records(change) do
    test_pid = self()

    expect(ContextCache, :bust_keys, fn records ->
      result = Mimic.call_original(ContextCache, :bust_keys, [records])
      send(test_pid, {:busted, records})
      result
    end)

    send(CacheBuster, %Transaction{changes: [change]})
    assert_receive {:busted, records}, 1_000
    records
  end

  defp assert_source_catalog_bust(change, source_key, user_id) do
    test_pid = self()

    Mimic.expect(ContextCache, :bust_keys, fn arg ->
      Mimic.call_original(ContextCache, :bust_keys, [arg])
      send(test_pid, arg)
    end)

    send(CacheBuster, %Transaction{changes: [change]})

    assert_receive [
                     {Sources, ^source_key},
                     {Catalog, [user_id: ^user_id]}
                   ],
                   500
  end

  @spec process_wal(struct()) :: :ok
  defp process_wal(change) do
    test_pid = self()

    Mimic.expect(ContextCache, :bust_keys, fn arg ->
      Mimic.call_original(ContextCache, :bust_keys, [arg])
      send(test_pid, :wal_processed)
    end)

    send(CacheBuster, %Transaction{changes: [change]})
    assert_receive :wal_processed, 500

    :ok
  end
end
