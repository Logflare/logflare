defmodule LogflareWeb.UnboxedConnCase do
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

  use ExUnit.CaseTemplate

  using do
    quote do
      import Plug.Conn
      import Phoenix.ConnTest
      import LogflareWeb.Router.Helpers
      alias LogflareWeb.Router.Helpers, as: Routes
      import Logflare.Factory
      import Phoenix.LiveViewTest
      use Logflare.Commons

      # The default endpoint for testing
      @endpoint LogflareWeb.Endpoint
    end
  end

  setup tags do
    if tags[:unboxed] do
      :ok = Ecto.Adapters.SQL.Sandbox.checkout(Logflare.Repo, sandbox: false)
      on_exit(&Logflare.EctoSQLUnboxedHelpers.truncate_all/0)
    else
      :ok = Ecto.Adapters.SQL.Sandbox.checkout(Logflare.Repo)
    end

    unless tags[:async] || tags[:unboxed] do
      Ecto.Adapters.SQL.Sandbox.mode(Logflare.Repo, {:shared, self()})
    end

    {:ok, conn: Phoenix.ConnTest.build_conn()}
  end
end
