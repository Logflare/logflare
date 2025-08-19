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
  alias Logflare.Auth
  alias Logflare.Backends.Backend
  alias Logflare.Backends

  describe "single tenant mode using Big Query" do
    TestUtils.setup_single_tenant()

    test "create_default_plan/0 creates default enterprise plan if not present" do
      assert {:ok, plan} = SingleTenant.create_default_plan()
      assert plan.name == "Enterprise"
      assert {:error, :already_created} = SingleTenant.create_default_plan()
    end

    test "create_default_plan/0 with existing data" do
      insert(:plan, name: "Enterprise")
      insert(:plan, name: "Enterprise")
      assert {:ok, plan} = SingleTenant.create_default_plan()
      assert plan.name == "Enterprise"
      assert Billing.get_plan_by(name: "Enterprise") == plan
      assert {:error, :already_created} = SingleTenant.create_default_plan()
    end

    test "create_default_plan/0 will override previous values" do
      assert {:ok, correct_plan} = SingleTenant.create_default_plan()
      Repo.update_all(Plan, set: [limit_sources: 999])
      assert {:ok, fetched} = SingleTenant.create_default_plan()
      assert fetched.limit_sources == correct_plan.limit_sources
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

    test "bug: get_default_plan/0 handles past value changes" do
      assert {:ok, _plan} = SingleTenant.create_default_plan()
      Repo.update_all(Plan, set: [limit_sources: 999])
      assert %Plan{name: "Enterprise"} = SingleTenant.get_default_plan()
    end

    test "create_default_user/0, get_default_user/0 :inserts a default enterprise user if not present" do
      assert {:ok, _plan} = SingleTenant.create_default_plan()
      assert {:ok, user} = SingleTenant.create_default_user()
      assert {:ok, _user} = SingleTenant.create_access_tokens()
      assert user.email_preferred
      assert user.endpoints_beta

      # create two access tokens
      assert_access_tokens(user)

      # create the default plan
      plan = Billing.get_plan_by_user(user)
      assert plan.name == "Enterprise"
      assert {:error, :already_created} = SingleTenant.create_default_user()
    end

    test "get_default_backend" do
      assert {:ok, _plan} = SingleTenant.create_default_plan()
      assert {:ok, user} = SingleTenant.create_default_user()
      assert %Backend{type: :bigquery} = SingleTenant.get_default_backend()
      assert %Backend{type: :bigquery} = Backends.get_default_backend(user)
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

  describe ":postgres_backend_url" do
    TestUtils.setup_single_tenant()

    setup do
      %{username: username, password: password, database: database, hostname: hostname} =
        Application.get_env(:logflare, Logflare.Repo) |> Map.new()

      url = "postgresql://#{username}:#{password}@#{hostname}/#{database}"

      prev = Application.get_env(:logflare, :postgres_backend_adapter)
      Application.put_env(:logflare, :postgres_backend_adapter, url: url)

      on_exit(fn ->
        Application.put_env(:logflare, :postgres_backend_adapter, prev)
      end)

      [url: url, password: password]
    end

    test "if is set and single tenant, default backend uses postgres backend", %{url: url} do
      Logflare.Application.startup_tasks()
      %{id: user_id} = user = SingleTenant.get_default_user()
      assert {:ok, source} = Sources.create_source(%{name: TestUtils.random_string()}, user)
      assert %Source{user_id: ^user_id, v2_pipeline: true} = source
      assert [] == Logflare.Backends.list_backends(source_id: source.id)
      assert %Backend{type: :postgres, config: %{url: ^url}} = SingleTenant.get_default_backend()
      assert %Backend{type: :postgres, config: %{url: ^url}} = Backends.get_default_backend(user)
    end

    test "if :postgres_backend_url is set and single tenant, updates an existing postgres backend",
         %{url: url, password: password} do
      Logflare.Application.startup_tasks()
      new_url = String.replace(url, ":" <> password <> "@", ":new_password@")
      Application.put_env(:logflare, :postgres_backend_adapter, url: new_url)
      Logflare.Application.startup_tasks()

      assert %Backend{type: :postgres, config: %{url: ^new_url}} =
               SingleTenant.get_default_backend()
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
      SingleTenant.create_supabase_sources()

      assert %{
               seed_user: :ok,
               seed_plan: :ok,
               seed_sources: :ok,
               seed_endpoints: nil,
               source_schemas_updated: nil
             } = SingleTenant.supabase_mode_status()
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
      assert {:ok, _user} = SingleTenant.create_access_tokens()
      assert user.email_preferred
      assert user.endpoints_beta

      # provision access tokens
      assert_access_tokens(user)

      # create plans
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
    TestUtils.setup_single_tenant(
      backend_type: :postgres,
      seed_user: true,
      supabase_mode: true,
      seed_backend: true
    )

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
      SingleTenant.create_supabase_sources()

      assert %{
               seed_user: :ok,
               seed_plan: :ok,
               seed_sources: :ok,
               seed_endpoints: nil,
               source_schemas_updated: :ok
             } = SingleTenant.supabase_mode_status()

      assert %{source_schemas_updated: :ok} = SingleTenant.supabase_mode_status()
    end
  end

  describe "create_access_tokens/0 - changing of public/private access token envs" do
    TestUtils.setup_single_tenant(
      backend_type: :postgres,
      seed_user: true
    )

    test "on change, should revoke only default provisioned access tokens" do
      initial_public_access_token = Application.get_env(:logflare, :public_access_token)

      initial_private_access_token =
        Application.get_env(:logflare, :private_access_token)

      SingleTenant.create_access_tokens()

      new_public_access_token = Logflare.TestUtils.random_string(12)
      new_private_access_token = Logflare.TestUtils.random_string(12)
      Application.put_env(:logflare, :public_access_token, new_public_access_token)
      Application.put_env(:logflare, :private_access_token, new_private_access_token)

      SingleTenant.create_access_tokens()
      user = SingleTenant.get_default_user()
      # revokes initial access tokens
      refute Auth.get_valid_access_token(user, initial_public_access_token)
      refute Auth.get_valid_access_token(user, initial_private_access_token)
      # provisions the new keys
      assert Auth.get_valid_access_token(user, new_public_access_token)
      assert Auth.get_valid_access_token(user, new_private_access_token)

      # user.api_key field
      assert user.api_key != initial_public_access_token
    end
  end

  defp assert_access_tokens(%_{id: user_id} = user) do
    assert length(Auth.list_valid_access_tokens(user)) == 2
    public = Application.get_env(:logflare, :public_access_token)
    assert {:ok, _token, %_{id: ^user_id}} = Auth.verify_access_token(public, "public")
    private = Application.get_env(:logflare, :private_access_token)
    assert {:ok, _token, %_{id: ^user_id}} = Auth.verify_access_token(private, "private")
  end
end
