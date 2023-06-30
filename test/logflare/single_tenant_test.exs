defmodule Logflare.SingleTenantTest do
  use Logflare.DataCase, async: false

  alias Logflare.Billing
  alias Logflare.Billing.Plan
  alias Logflare.Endpoints
  alias Logflare.Repo
  alias Logflare.SingleTenant
  alias Logflare.Sources
  alias Logflare.User
  alias Logflare.Users

  describe "single tenant mode" do
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
  end

  test "single_tenant? returns false when not in single tenant mode" do
    refute SingleTenant.single_tenant?()
  end

  describe "supabase_mode=true" do
    TestUtils.setup_single_tenant(seed_user: true, supabase_mode: true)

    setup do
      %{username: username, password: password, database: database, hostname: hostname} =
        Application.get_env(:logflare, Logflare.Repo) |> Map.new()

      url = "postgresql://#{username}:#{password}@#{hostname}/#{database}"
      previous_url = Application.get_env(:logflare, :single_instance_postgres_url)
      Application.put_env(:logflare, :single_instance_postgres_url, url)

      on_exit(fn ->
        Application.put_env(:logflare, :single_instance_postgres_url, previous_url)
      end)

      %{url: url}
    end

    test "create_supabase_sources/0, create_supabase_endpoints/0", %{url: url} do
      assert {:ok, sources} = SingleTenant.create_supabase_sources()
      assert {:error, :already_created} = SingleTenant.create_supabase_sources()

      assert [url] ==
               sources
               |> Enum.map(&Repo.preload(&1, :source_backends))
               |> Enum.map(fn %{source_backends: [%{config: %{"url" => url}}]} -> url end)
               |> Enum.uniq()

      # must have sources created first
      assert {:ok, [_ | _]} = SingleTenant.create_supabase_endpoints()
      assert {:error, :already_created} = SingleTenant.create_supabase_endpoints()
    end

    test "startup tasks inserts log sources/endpoints" do
      SingleTenant.create_supabase_sources()
      SingleTenant.create_supabase_endpoints()

      user = SingleTenant.get_default_user()
      sources = Sources.list_sources_by_user(user)
      assert length(sources) > 0
      assert Endpoints.list_endpoints_by(user_id: user.id) |> length() > 0
    end

    test "supabase_mode_status/0" do
      SingleTenant.create_supabase_sources()
      SingleTenant.create_supabase_endpoints()
      started = SingleTenant.ensure_supabase_sources_started() |> Enum.map(&elem(&1, 1))

      assert %{
               seed_user: :ok,
               seed_plan: :ok,
               seed_sources: :ok,
               seed_endpoints: :ok
             } = SingleTenant.supabase_mode_status()

      on_exit(fn ->
        Enum.each(started, &Process.exit(&1, :normal))
      end)
    end
  end
end
