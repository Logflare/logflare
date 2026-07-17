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

  alias Logflare.Partners.Partner

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

      alias Logflare.Backends.Adaptor.ClickHouseAdaptor
      alias Logflare.Backends.IngestEventQueue
      alias Logflare.PubSubRates
      alias Logflare.TestUtils
      alias Logflare.TestUtilsGrpc
      alias Logflare.User
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
          IngestEventQueue.delete_all_mappings()
          PubSubRates.Cache.clear()
          ClickHouseAdaptor.QueryConnectionSup.terminate_all()
        end)

        :ok
      end
    end
  end

  setup tags do
    Logflare.DataCase.setup_sandbox(tags)
    Logflare.DataCase.setup_mocking(tags)

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
    |> Plug.Conn.put_private(:logflare_test_team_id, team_user.team_id)
    |> Plug.Conn.assign(:team_user, team_user)
    |> Plug.Conn.put_session(:current_email, team_user.email)
  end

  def login_user(conn, user) do
    conn
    |> Plug.Conn.put_private(:logflare_test_team_id, loaded_team_id(user))
    |> Plug.Conn.put_private(:logflare_test_user_id, user.id)
    |> Plug.Test.init_test_session(%{current_email: user.email})
  end

  defp loaded_team_id(%{team: %Logflare.Teams.Team{id: id}}), do: id
  defp loaded_team_id(_user), do: nil

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

  def add_access_token(conn, %Partner{} = partner, scopes) do
    scopes = if is_list(scopes), do: Enum.join(scopes, " "), else: scopes
    {:ok, access_token} = Logflare.Auth.create_access_token(partner, %{scopes: scopes})

    Plug.Conn.put_req_header(conn, "authorization", "Bearer #{access_token.token}")
  end

  def assert_schema(data, schema_name) do
    OpenApiSpex.TestAssertions.assert_schema(data, schema_name, LogflareWeb.ApiSpec.spec())
  end

  @doc """
  Calls `live/3` with the signed-in user's selected team and follows the first
  application redirect when the resource belongs to another team.

  Supplying the common `t=` parameter up front avoids mounting the LiveView
  twice solely to discover the default team.
  """
  defmacro live_with_redirect(conn, path \\ nil, opts \\ []) do
    quote bind_quoted: [conn: conn, path: path, opts: opts] do
      path = LogflareWeb.ConnCase.put_default_team_param(conn, path)
      result = Phoenix.LiveViewTest.live(conn, path, opts)

      case result do
        {:ok, _view, _html} ->
          result

        {:error, {:live_redirect, %{to: to}}} ->
          if LogflareWeb.ConnCase.redirected_to_different_team?(path, to) do
            Phoenix.LiveViewTest.live(conn, to, opts)
          else
            result
          end

        {:error, {:redirect, %{to: to}}} ->
          {:ok, Phoenix.ConnTest.get(conn, to)}

        _ ->
          result
      end
    end
  end

  @doc false
  def redirected_to_different_team?(from, to) when is_binary(from) and is_binary(to) do
    from_team = from |> URI.parse() |> then(&URI.decode_query(&1.query || "")) |> Map.get("t")
    to_team = to |> URI.parse() |> then(&URI.decode_query(&1.query || "")) |> Map.get("t")

    not is_nil(to_team) and to_team != from_team
  end

  def redirected_to_different_team?(_from, _to), do: false

  @doc false
  def put_default_team_param(_conn, nil), do: nil

  def put_default_team_param(conn, path) when is_binary(path) do
    uri = URI.parse(path)
    query = URI.decode_query(uri.query || "")

    if Map.has_key?(query, "t") do
      path
    else
      case default_team_id(conn) do
        nil -> path
        team_id -> %{uri | query: URI.encode_query(Map.put(query, "t", team_id))} |> to_string()
      end
    end
  end

  defp default_team_id(conn) do
    conn.private[:logflare_test_team_id] || default_user_team_id(conn)
  end

  defp default_user_team_id(conn) do
    case conn.private[:logflare_test_user_id] do
      nil ->
        nil

      user_id ->
        case Logflare.Teams.get_team_by(user_id: user_id) do
          nil -> create_default_team(user_id)
          team -> team.id
        end
    end
  end

  defp create_default_team(user_id) do
    case Logflare.Users.get(user_id) do
      nil ->
        nil

      user ->
        {:ok, team} =
          Logflare.Teams.create_team(user, %{name: Logflare.Generators.team_name()})

        team.id
    end
  end
end
