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

  The HTTP request helpers imported from this module (`get/3`, `post/3`, and
  the other verbs) dispatch through `dispatch_and_assert_open_api_response/5`.
  Responses from documented `/api` routes are therefore checked against their
  OpenAPI operation and documented status. Tests that intentionally need to
  bypass this validation can call `Phoenix.ConnTest.dispatch/5` directly.
  """

  @session Plug.Session.init(
             store: :cookie,
             key: "_app",
             encryption_salt: "yadayada",
             signing_salt: "yadayada"
           )

  use ExUnit.CaseTemplate

  import ExUnit.Assertions

  alias Logflare.Partners.Partner
  alias LogflareWeb.Router
  alias OpenApiSpex.Plug.PutApiSpec

  @http_methods [:get, :post, :put, :patch, :delete, :options, :connect, :trace, :head]
  @conn_test_request_macros for method <- @http_methods, arity <- [2, 3], do: {method, arity}
  @open_api_methods Map.new(@http_methods, &{&1 |> Atom.to_string() |> String.upcase(), &1})

  using _opts do
    quote do
      use Mimic

      require Logflare.TestUtils

      import Logflare.Factory
      import LogflareWeb.Router.Helpers
      import Phoenix.ConnTest, except: unquote(@conn_test_request_macros)
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

  def add_access_token(conn, %Partner{} = partner, scopes) do
    scopes = if is_list(scopes), do: Enum.join(scopes, " "), else: scopes
    {:ok, access_token} = Logflare.Auth.create_access_token(partner, %{scopes: scopes})

    Plug.Conn.put_req_header(conn, "authorization", "Bearer #{access_token.token}")
  end

  # Preserve Phoenix.ConnTest's request helpers while adding OpenAPI response validation.
  for method <- @http_methods do
    defmacro unquote(method)(conn, path_or_action, params_or_body \\ nil) do
      method = unquote(method)

      quote do
        LogflareWeb.ConnCase.dispatch_and_assert_open_api_response(
          unquote(conn),
          @endpoint,
          unquote(method),
          unquote(path_or_action),
          unquote(params_or_body)
        )
      end
    end
  end

  @spec dispatch_and_assert_open_api_response(
          Plug.Conn.t(),
          module(),
          atom(),
          String.t() | atom(),
          term()
        ) :: Plug.Conn.t()
  def dispatch_and_assert_open_api_response(
        conn,
        endpoint,
        method,
        path_or_action,
        params_or_body
      ) do
    conn = Phoenix.ConnTest.dispatch(conn, endpoint, method, path_or_action, params_or_body)
    assert_open_api_response(conn)
  end

  @spec assert_open_api_response(Plug.Conn.t()) :: Plug.Conn.t()
  def assert_open_api_response(conn) do
    case open_api_operation(conn) do
      nil ->
        conn

      operation ->
        assert_documented_response!(operation, conn)
        OpenApiSpex.TestAssertions.assert_operation_response(conn, operation.operationId)
        conn
    end
  end

  defp open_api_operation(conn) do
    with %{route: route} <-
           Phoenix.Router.route_info(Router, conn.method, conn.request_path, conn.host),
         true <- String.starts_with?(route, "/api"),
         {spec, _operation_lookup} <-
           PutApiSpec.get_spec_and_operation_lookup(conn),
         path_item when not is_nil(path_item) <- Map.get(spec.paths, open_api_path(route)) do
      Map.get(path_item, Map.fetch!(@open_api_methods, conn.method))
    else
      _ -> nil
    end
  end

  defp assert_documented_response!(operation, conn) do
    if Map.has_key?(operation.responses, conn.status) ||
         Map.has_key?(operation.responses, :default) do
      :ok
    else
      flunk(
        "No OpenAPI response is documented for #{conn.method} #{conn.request_path} with status #{conn.status}"
      )
    end
  end

  defp open_api_path(path) do
    Regex.replace(~r|:([^/]+)|, path, fn _, parameter -> "{#{parameter}}" end)
  end

  @doc """
  Call live/3 and automatically follow the first live redirect.

  Useful for the common case of a live view redirecting to add the `t=` param.
  """
  defmacro live_with_redirect(conn, path \\ nil, opts \\ []) do
    quote bind_quoted: [conn: conn, path: path, opts: opts] do
      result = Phoenix.LiveViewTest.live(conn, path, opts)

      case result do
        {:ok, _view, _html} ->
          result

        {:error, {:live_redirect, %{to: to}}} ->
          Phoenix.LiveViewTest.live(conn, to, opts)

        {:error, {:redirect, %{to: to}}} ->
          {:ok, Phoenix.ConnTest.get(conn, to)}

        _ ->
          result
      end
    end
  end
end
