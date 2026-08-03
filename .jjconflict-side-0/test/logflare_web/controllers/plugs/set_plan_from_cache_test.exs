defmodule LogflareWeb.Plugs.SetPlanFromCacheTest do
  use LogflareWeb.ConnCase, async: false

  alias Logflare.Billing
  alias Logflare.Billing.Plan
  alias Logflare.Sources
  alias Logflare.SystemMetrics.AllLogsLogged
  alias Logflare.Users
  alias LogflareWeb.Plugs.SetPlanFromCache

  describe "init/1" do
    test "returns nil" do
      assert SetPlanFromCache.init([]) == nil
    end
  end

  describe "call/2" do
    test "assigns plan from cache when user is present", %{conn: conn} do
      plan = insert(:plan)
      user = insert(:user)

      # first call populates the cache
      result =
        conn
        |> assign(:user, user)
        |> SetPlanFromCache.call([])

      assert %Plan{} = result.assigns.plan
      assert result.assigns.plan.id == plan.id

      # delete the plan from DB — a direct DB call would fail now
      Billing.delete_plan(plan)

      # plug still returns the plan because it reads from cache
      result =
        conn
        |> assign(:user, user)
        |> SetPlanFromCache.call([])

      assert result.assigns.plan.id == plan.id
    end

    test "assigns legacy plan from cache when billing is disabled", %{conn: conn} do
      insert(:plan)
      user = insert(:user, billing_enabled: false)

      result =
        conn
        |> assign(:user, user)
        |> SetPlanFromCache.call([])

      assert result.assigns.plan == Billing.legacy_plan()
    end

    test "passes conn through when no user is assigned", %{conn: conn} do
      result = SetPlanFromCache.call(conn, [])

      refute Map.has_key?(result.assigns, :plan)
    end

    test "passes conn through when user assign is nil", %{conn: conn} do
      result =
        conn
        |> assign(:user, nil)
        |> SetPlanFromCache.call([])

      refute Map.has_key?(result.assigns, :plan)
    end
  end

  describe "ingest pipeline integration" do
    setup do
      start_supervised!(AllLogsLogged)
      plan = insert(:plan, name: "Free")
      user = insert(:user)
      source = insert(:source, user_id: user.id)

      insert(:backend, sources: [source], type: :webhook, config: %{url: "some url"})

      # warm caches so the pipeline resolves user/source/plan from cache
      Sources.Cache.get_by_and_preload_rules(token: Atom.to_string(source.token))
      Sources.Cache.get_source_by_token(source.token)
      Users.Cache.get(user.id)
      Users.Cache.get_by(api_key: user.api_key)
      Billing.Cache.get_plan_by_user(user)

      on_exit(fn ->
        Cachex.clear(Users.Cache)
        Cachex.clear(Sources.Cache)
        Cachex.clear(Billing.Cache)
      end)

      [plan: plan, user: user, source: source]
    end

    test "plan is assigned from cache for authenticated ingest requests", %{
      conn: conn,
      plan: plan,
      user: user,
      source: source
    } do
      reject(&Billing.get_plan_by_user/1)

      conn =
        conn
        |> put_req_header("x-api-key", user.api_key)
        |> put_req_header("content-type", "application/json")
        |> post("/logs?source=#{source.token}", Jason.encode!(%{"event_message" => "test"}))

      assert conn.assigns.plan.id == plan.id
    end
  end
end
