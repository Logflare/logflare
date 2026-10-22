defmodule Logflare.Backends.QueryClassSettingsTest do
  use Logflare.DataCase

  alias Logflare.Backends
  alias Logflare.Backends.Backend
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

  test "operator policy is validated, persisted and protected from customer changes", context do
    %{owner: owner, admin: admin, backend: backend} = context

    assert {:error, :forbidden} = Backends.configure_query_class_settings(owner, backend, @policy)

    assert {:error, :forbidden} =
             Backends.configure_query_class_settings(%{owner | admin: true}, backend, @policy)

    assert {:error, changeset} =
             Backends.update_backend(backend, %{config: %{query_class_settings: @policy}})

    assert Keyword.has_key?(changeset.errors, :"config.query_class_settings")

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

    changeset = Backend.changeset(configured, %{type: :postgres})
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

  test "revoked administrators and non-ClickHouse backends cannot configure policy", context do
    %{admin: admin, backend: backend} = context
    other = insert(:backend, user: context.owner, type: :bigquery)

    assert {:error, :not_clickhouse_backend} =
             Backends.configure_query_class_settings(admin, other, @policy)

    admin |> Ecto.Changeset.change(admin: false) |> Repo.update!()
    assert {:error, :forbidden} = Backends.configure_query_class_settings(admin, backend, @policy)
  end
end
