defmodule Logflare.SingleTenantTest do
  @moduledoc false
  use Logflare.DataCase
  alias Logflare.SingleTenant
  alias Logflare.Billing
  alias Logflare.Users
  alias Logflare.User
  alias Logflare.Billing.Plan
  alias Logflare.Sources
  alias Logflare.Endpoints
  alias Logflare.Source.BigQuery.Schema
  alias Logflare.Source
  alias Logflare.Backends.Backend

  describe "single tenant mode using Big Query" do
    TestUtils.setup_single_tenant()

    test "create_default_plan/0 creates default enterprise plan if not present" do
      assert {:ok, plan} = SingleTenant.create_default_plan()
      assert plan.name == "Enterprise"
      assert {:error, :already_created} = SingleTenant.create_default_plan()
    end

    test "get_default_user/0" do
      assert {:ok, _plan} = SingleTenant.create_default_plan()
      assert {:ok, _user} = SingleTenant.create_default_user()
      assert %User{name: "default"} = SingleTenant.get_default_user()
    end

    test "get_default_plan/0" do
      assert {:ok, _plan} = SingleTenant.create_default_plan()
      assert %Plan{name: "Enterprise"} = SingleTenant.get_default_plan()
    end

    test "create_default_user/0, get_default_user/0 :inserts a default enterprise user if not present" do
      assert {:ok, _plan} = SingleTenant.create_default_plan()
      assert {:ok, user} = SingleTenant.create_default_user()
      assert user.email_preferred
      assert user.endpoints_beta

      # api key should be based on env var
      assert user.api_key == Application.get_env(:logflare, :api_key)

      plan = Billing.get_plan_by_user(user)

      assert plan.name == "Enterprise"
      assert {:error, :already_created} = SingleTenant.create_default_user()
    end

    test "single_tenant? returns true when in single tenant mode" do
      assert SingleTenant.single_tenant?()
    end

    test "Logflare.Application.startup_tasks/0 should insert plan and user" do
      Logflare.Application.startup_tasks()

      assert [_] = Billing.list_plans()
      assert 1 = Users.count_users()
    end

    test "if :postgres_backend_url is set and single tenant, creates a source with a postgres backend" do

      %{username: username, password: password, database: database, hostname: hostname} =
        Application.get_env(:logflare, Logflare.Repo) |> Map.new()

      url = "postgresql://#{username}:#{password}@#{hostname}/#{database}"

      prev = Application.get_env(:logflare, :postgres_backend_adapter)
      Application.put_env(:logflare, :postgres_backend_adapter, url: url)
      Logflare.Application.startup_tasks()
      user = SingleTenant.get_default_user()
      %{id: user_id} = user

      on_exit(fn ->
        Application.put_env( :logflare, :postgres_backend_adapter, prev )
      end)

      assert {:ok, source} = Sources.create_source(%{name: TestUtils.random_string()}, user)
      assert %Source{user_id: ^user_id, v2_pipeline: true} = source
      assert [%Backend{type: :postgres}] = Logflare.Backends.list_backends(source)
      assert %Backend{type: :postgres} = SingleTenant.get_default_backend()
    end

  end

  test "single_tenant? returns false when not in single tenant mode" do
    refute SingleTenant.single_tenant?()
  end

  describe "supabase_mode=true using Big Query" do
    TestUtils.setup_single_tenant(seed_user: true, supabase_mode: true)

    setup do
      stub(Schema, :update, fn _token, _le -> :ok end)
      :ok
    end

    test "create_supabase_sources/0, create_supabase_endpoints/0" do
      assert {:ok, [_ | _]} = SingleTenant.create_supabase_sources()
      assert {:error, :already_created} = SingleTenant.create_supabase_sources()

      # must have sources created first
      assert {:ok, [_ | _]} = SingleTenant.create_supabase_endpoints()
      assert {:error, :already_created} = SingleTenant.create_supabase_endpoints()
    end

    test "startup tasks inserts log sources/endpoints" do
      expect(Schema, :update, 9, fn _source_token, _log_event -> :ok end)

      SingleTenant.create_supabase_sources()
      SingleTenant.create_supabase_endpoints()
      SingleTenant.update_supabase_source_schemas()

      user = SingleTenant.get_default_user()
      sources = Sources.list_sources_by_user(user)

      assert length(sources) > 0
      assert Endpoints.list_endpoints_by(user_id: user.id) |> length() > 0
    end

    test "supabase_mode_status/0" do
      stub(Schema, :get_state, fn _ -> %{field_count: 3} end)
      SingleTenant.create_supabase_sources()

      assert %{
               seed_user: :ok,
               seed_plan: :ok,
               seed_sources: :ok,
               seed_endpoints: nil,
               source_schemas_updated: nil
             } = SingleTenant.supabase_mode_status()

      stub(Schema, :get_state, fn _ -> %{field_count: 5} end)

      assert %{source_schemas_updated: :ok} = SingleTenant.supabase_mode_status()
    end
  end

  describe "single tenant mode using Postgres" do
    TestUtils.setup_single_tenant(backend_type: :postgres)

    test "create_default_plan/0 creates default enterprise plan if not present" do
      assert {:ok, plan} = SingleTenant.create_default_plan()
      assert plan.name == "Enterprise"
      assert {:error, :already_created} = SingleTenant.create_default_plan()
    end

    test "get_default_user/0" do
      assert {:ok, _plan} = SingleTenant.create_default_plan()
      assert {:ok, _user} = SingleTenant.create_default_user()
      assert %User{name: "default"} = SingleTenant.get_default_user()
    end

    test "get_default_plan/0" do
      assert {:ok, _plan} = SingleTenant.create_default_plan()
      assert %Plan{name: "Enterprise"} = SingleTenant.get_default_plan()
    end

    test "create_default_user/0, get_default_user/0 :inserts a default enterprise user if not present" do
      assert {:ok, _plan} = SingleTenant.create_default_plan()
      assert {:ok, user} = SingleTenant.create_default_user()
      assert user.email_preferred
      assert user.endpoints_beta

      # api key should be based on env var
      assert user.api_key == Application.get_env(:logflare, :api_key)

      plan = Billing.get_plan_by_user(user)

      assert plan.name == "Enterprise"
      assert {:error, :already_created} = SingleTenant.create_default_user()
    end

    test "single_tenant? returns true when in single tenant mode" do
      assert SingleTenant.single_tenant?()
    end

    test "Logflare.Application.startup_tasks/0 should insert plan and user" do
      Logflare.Application.startup_tasks()

      assert [_] = Billing.list_plans()
      assert 1 = Users.count_users()
    end
  end

  describe "supabase_mode=true using Postgres" do
    TestUtils.setup_single_tenant(backend_type: :postgres, seed_user: true, supabase_mode: true)

    setup do
      stub(Schema, :update, fn _token, _le -> :ok end)
      :ok
    end

    test "create_supabase_sources/0, create_supabase_endpoints/0" do
      assert {:ok, [_ | _]} = SingleTenant.create_supabase_sources()
      assert {:error, :already_created} = SingleTenant.create_supabase_sources()

      # must have sources created first
      assert {:ok, [_ | _]} = SingleTenant.create_supabase_endpoints()
      assert {:error, :already_created} = SingleTenant.create_supabase_endpoints()
    end

    test "startup tasks inserts log sources/endpoints" do
      expect(Schema, :update, 9, fn _source_token, _log_event -> :ok end)

      SingleTenant.create_supabase_sources()
      SingleTenant.create_supabase_endpoints()
      SingleTenant.update_supabase_source_schemas()

      user = SingleTenant.get_default_user()
      sources = Sources.list_sources_by_user(user)

      assert length(sources) > 0
      assert Endpoints.list_endpoints_by(user_id: user.id) |> length() > 0
    end

    test "supabase_mode_status/0" do
      stub(Schema, :get_state, fn _ -> %{field_count: 3} end)
      SingleTenant.create_supabase_sources()

      assert %{
               seed_user: :ok,
               seed_plan: :ok,
               seed_sources: :ok,
               seed_endpoints: nil,
               source_schemas_updated: :ok
             } = SingleTenant.supabase_mode_status()

      stub(Schema, :get_state, fn _ -> %{field_count: 5} end)

      assert %{source_schemas_updated: :ok} = SingleTenant.supabase_mode_status()
    end
  end
end
