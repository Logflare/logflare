defmodule LogflareWeb.LogControllerTest do
  @moduledoc false
  use LogflareWeb.ConnCase
  alias Logflare.Backends.Adaptor.WebhookAdaptor
  alias Logflare.Source.RecentLogsServer
  alias Logflare.Sources.Counters
  alias Logflare.Sources.RateCounters
  alias Logflare.SingleTenant
  alias Logflare.Users
  alias Logflare.Sources

  @valid %{"some" => "valid log entry", "event_message" => "hi!"}
  @valid_batch [
    %{"some" => "valid log entry", "event_message" => "hi!"},
    %{"some" => "valid log entry 2", "event_message" => "hi again!"}
  ]

  setup do
    Logflare.Sources.Counters
    |> stub(:increment, fn v -> v end)

    Logflare.SystemMetrics.AllLogsLogged
    |> stub(:increment, fn v -> v end)

    # mock goth behaviour
    Goth
    |> stub(:fetch, fn _mod -> {:ok, %Goth.Token{token: "auth-token"}} end)

    :ok
  end

  describe "v2 pipeline" do
    setup do
      user = insert(:user)
      source = insert(:source, user_id: user.id, v2_pipeline: true)
      _plan = insert(:plan, name: "Free")

      start_supervised!(Counters)
      start_supervised!(RateCounters)

      source_backend =
        insert(:source_backend, source_id: source.id, type: :webhook, config: %{url: "some url"})

      {:ok, source: source, user: user, source_backend: source_backend}
    end
    setup [:warm_caches, :reject_context_functions]

    test "valid ingestion", %{conn: conn, source: source, user: user} do
      WebhookAdaptor
      |> expect(:ingest, fn _, _ -> :ok end)

      conn =
        conn
        |> put_req_header("x-api-key", user.api_key)
        |> post(Routes.log_path(conn, :create, source: source.token), @valid)

      assert json_response(conn, 200) == %{"message" => "Logged!"}
      :timer.sleep(2000)
    end

    test "invaild source token uuid checks", %{conn: conn, user: user} do
      conn =
        conn
        |> put_req_header("x-api-key", user.api_key)
        |> post(Routes.log_path(conn, :create, source: ":signin"), @valid)

      assert json_response(conn, 401)
    end
  end

  describe "v1 pipeline with legacy user.api_key" do
    setup [:v1_pipeline_setup, :warm_caches, :reject_context_functions]

    setup %{user: user, conn: conn} do
      conn = put_req_header(conn, "x-api-key", user.api_key)

      {:ok, user: user, conn: conn}
    end

    test ":create ingestion by source_name", %{conn: conn, source: source} do
      conn =
        conn
        |> post(Routes.log_path(conn, :create, source_name: source.name), @valid)

      assert json_response(conn, 200) == %{"message" => "Logged!"}
      # wait for all logs to be ingested before removing all stubs
      :timer.sleep(2000)
    end

    test ":create ingestion", %{conn: conn, source: source} do
      conn =
        conn
        |> post(Routes.log_path(conn, :create, source: source.token), @valid)

      assert json_response(conn, 200) == %{"message" => "Logged!"}
      # wait for all logs to be ingested before removing all stubs
      :timer.sleep(2000)
    end

    test ":create ingestion with gzip", %{conn: conn, source: source} do
      batch = for _i <- 1..500, do: @valid
      payload = :zlib.gzip(Jason.encode!(%{"batch" => batch}))
      payload  |> :erlang.term_to_binary() |> :erlang.external_size() |> dbg() # 178 bytes
      conn =
        conn
        |> Plug.Conn.put_req_header("content-encoding", "gzip" )
        |> Plug.Conn.put_req_header("content-type", "application/json" )
        |> post(~p"/logs?#{[source: source.token]}", payload)

      assert json_response(conn, 200) == %{"message" => "Logged!"}
      # wait for all logs to be ingested before removing all stubs
      :timer.sleep(2000)
    end
    test ":create ingestion batch with `batch` key", %{conn: conn, source: source} do
      conn =
        conn
        |> post(Routes.log_path(conn, :create, source: source.token), %{"batch" => @valid_batch})

      assert json_response(conn, 200) == %{"message" => "Logged!"}
      # wait for all logs to be ingested before removing all stubs
      :timer.sleep(2000)
    end

    test ":create ingestion batch with array body", %{conn: conn, source: source} do
      conn =
        conn
        |> put_req_header("content-type", "application/json")
        |> post(Routes.log_path(conn, :create, source: source.token), Jason.encode!(@valid_batch))

      assert json_response(conn, 200) == %{"message" => "Logged!"}
      # wait for all logs to be ingested before removing all stubs
      :timer.sleep(2000)
    end

    test ":cloudflare ingestion", %{conn: new_conn, source: source} do
      path = Routes.log_path(new_conn, :cloudflare, source: source.token)

      assert new_conn |> post(path, @valid) |> json_response(200) == %{"message" => "Logged!"}

      assert new_conn
             |> post(path, %{batch: [@valid]})
             |> json_response(200) == %{
               "message" => "Logged!"
             }

      # wait for all logs to be ingested before removing all stubs
      :timer.sleep(2000)
    end
  end

  describe "v1 pipeline with access tokens" do
    setup [:v1_pipeline_setup]

    setup %{user: user, conn: conn} do
      {:ok, access_token} = Logflare.Auth.create_access_token(user)
      conn = put_req_header(conn, "x-api-key", access_token.token)
      {:ok, user: user, conn: conn}
    end
    setup [:warm_caches, :reject_context_functions]


    test ":create ingestion by source_name", %{conn: conn, source: source} do
      conn =
        conn
        |> post(Routes.log_path(conn, :create, source_name: source.name), @valid)

      assert json_response(conn, 200) == %{"message" => "Logged!"}
      # wait for all logs to be ingested before removing all stubs
      :timer.sleep(2000)
    end
  end


  describe "single tenant" do
    TestUtils.setup_single_tenant(seed_user: true)

    setup %{conn: conn} do
      # get single tenant user
      user = SingleTenant.get_default_user()

      # insert the source
      source = insert(:source, user: user)

      # ingestion setup
      rls = %RecentLogsServer{source: source, source_id: source.token}
      start_supervised!(Counters)
      start_supervised!(RateCounters)
      start_supervised!({RecentLogsServer, rls})
      :timer.sleep(500)

      Logflare.Logs
      |> expect(:broadcast, 1, fn le ->
        le
      end)

      conn =
        conn
        |> put_req_header("x-api-key", user.api_key)

      [source: source, conn: conn]
    end

    test ":create ingestion", %{conn: conn, source: source} do
      conn =
        conn
        |> post(Routes.log_path(conn, :create, source: source.token), @valid)

      assert json_response(conn, 200) == %{"message" => "Logged!"}

      # wait for all logs to be ingested before removing all stubs
      :timer.sleep(2000)
    end

  end

  defp v1_pipeline_setup(%{conn: conn}) do
    insert(:plan, name: "Free")
    user = insert(:user)
    source = insert(:source, user: user)


    rls = %RecentLogsServer{source: source, source_id: source.token}
    start_supervised!(Counters)
    start_supervised!(RateCounters)
    start_supervised!({RecentLogsServer, rls})
    :timer.sleep(500)

    Logflare.Logs
    |> expect(:broadcast, 1, fn le ->
      assert match?(@valid, le.body)
      assert le.body["event_message"] != nil
      assert Map.keys(le.body) |> length() == 4, inspect(Map.keys(le.body))

      le
    end)

    {:ok, source: source, user: user, conn: conn}
  end

  defp warm_caches(%{user: user, source: source}) do

    # hit the caches
    Sources.Cache.get_by_and_preload_rules(token: Atom.to_string(source.token))
    Sources.Cache.get_by_and_preload_rules(name: source.name, user_id: user.id)
    Users.Cache.get_by_and_preload(api_key: user.api_key)
    Users.Cache.preload_defaults(user)
    Users.Cache.get(user.id)
    on_exit(fn ->
      Cachex.clear(Users.Cache)
      Cachex.clear(Sources.Cache)
    end)
    :ok
  end
  defp reject_context_functions(_) do
    reject(&Sources.get_source_by_token/1)
    reject(&Sources.get/1)
    reject(&Sources.get_by/1)
    reject(&Sources.get_by_and_preload_rules/1)
    reject(&Sources.preload_defaults/1)
    reject(&Users.get/1)
    reject(&Users.get_by/1)
    reject(&Users.get_by_and_preload/1)
    reject(&Users.preload_team/1)
    reject(&Users.preload_billing_account/1)
    :ok
  end
end
