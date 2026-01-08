defmodule LogflareWeb.ConnCase do
  @moduledoc """
  This module defines the test case to be used by
  tests that require setting up a connection.

  Such tests rely on `Phoenix.ConnTest` and also
  import other functionality to make it easier
  to build common datastructures and query the data layer.

  Finally, if the test case interacts with the database,
  it cannot be async. For this reason, every test runs
  inside a transaction which is reset at the beginning
  of the test unless the test case is marked as async.
  """

  @session Plug.Session.init(
             store: :cookie,
             key: "_app",
             encryption_salt: "yadayada",
             signing_salt: "yadayada"
           )

  use ExUnit.CaseTemplate

  using _opts do
    quote do
      use Mimic

      require Logflare.TestUtils

      import Logflare.Factory
      import LogflareWeb.Router.Helpers
      import Phoenix.ConnTest
      import Phoenix.LiveViewTest
      import Phoenix.VerifiedRoutes
      import PhoenixTest
      import Plug.Conn
      import unquote(__MODULE__)

      alias Logflare.TestUtils
      alias Logflare.TestUtilsGrpc
      alias Logflare.User
      alias Logflare.Partners.Partner
      alias LogflareWeb.Router.Helpers, as: Routes

      # The default endpoint for testing
      @router LogflareWeb.Router
      @endpoint LogflareWeb.Endpoint

      setup context do
        Mimic.verify_on_exit!(context)

        stub(ConfigCat, :get_value, fn _, _ -> true end)
        stub(Goth, :fetch, fn _mod -> {:ok, %Goth.Token{token: "auth-token"}} end)

        stub(Logflare.Cluster.Utils, :rpc_call, fn _node, func ->
          func.()
        end)

        caches = Logflare.ContextCache.Supervisor.list_caches()
        Enum.each(caches, &Cachex.reset(&1, hooks: [Cachex.Stats]))

        on_exit(fn ->
          Logflare.Backends.IngestEventQueue.delete_all_mappings()
          Logflare.PubSubRates.Cache.clear()
          Logflare.Backends.Adaptor.ClickHouseAdaptor.QueryConnectionSup.terminate_all()
        end)

        :ok
      end
    end
  end

  setup tags do
    pid = Ecto.Adapters.SQL.Sandbox.start_owner!(Logflare.Repo, shared: not tags[:async])
    on_exit(fn -> Ecto.Adapters.SQL.Sandbox.stop_owner(pid) end)

    unless tags[:async] do
      # for global Mimic mocs
      Mimic.set_mimic_global(tags)
    end

    {:ok,
     conn:
       Phoenix.ConnTest.build_conn()
       |> Plug.Session.call(@session)
       |> Plug.Conn.fetch_session()}
  end

  # for browser use
  def login_user(conn, user, team_user) do
    conn
    |> login_user(user)
    |> Plug.Conn.assign(:team_user, team_user)
    |> Plug.Conn.put_session(:current_email, team_user.email)
  end

  def login_user(conn, user) do
    conn
    |> Plug.Test.init_test_session(%{current_email: user.email})
  end

  # for api use
  def add_partner_access_token(conn, partner) do
    add_access_token(conn, partner, ~w(partner))
  end

  def add_access_token(conn, user, scopes \\ ~w(public))

  def add_access_token(conn, %Logflare.User{} = user, scopes) do
    scopes = if is_list(scopes), do: Enum.join(scopes, " "), else: scopes
    {:ok, access_token} = Logflare.Auth.create_access_token(user, %{scopes: scopes})

    Plug.Conn.put_req_header(conn, "authorization", "Bearer #{access_token.token}")
  end

  def add_access_token(conn, %Logflare.Partners.Partner{} = partner, scopes) do
    scopes = if is_list(scopes), do: Enum.join(scopes, " "), else: scopes
    {:ok, access_token} = Logflare.Auth.create_access_token(partner, %{scopes: scopes})

    Plug.Conn.put_req_header(conn, "authorization", "Bearer #{access_token.token}")
  end

  def assert_schema(data, schema_name) do
    OpenApiSpex.TestAssertions.assert_schema(data, schema_name, LogflareWeb.ApiSpec.spec())
  end
end
