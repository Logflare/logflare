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

      import Plug.Conn
      import Phoenix.ConnTest
      import LogflareWeb.Router.Helpers
      import Logflare.Factory
      import Phoenix.LiveViewTest

      alias Logflare.TestUtils
      require Logflare.TestUtils
      alias LogflareWeb.Router.Helpers, as: Routes

      # The default endpoint for testing
      @endpoint LogflareWeb.Endpoint

      setup context do
        Mimic.verify_on_exit!(context)

        ConfigCat
        |> stub(:get_value, fn _, _ -> true end)

        :ok
      end

      def login_user(conn, user) do
        conn
        |> Plug.Test.init_test_session(%{user_id: user.id})
        |> Plug.Conn.assign(:user, user)
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
end
