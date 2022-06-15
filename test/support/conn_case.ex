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

  use ExUnit.CaseTemplate

  using opts do
    opts = opts |> Enum.into(%{mock_sql: false})

    mock_sql =
      if opts.mock_sql do
        quote do
          setup do
            Logflare.SQL
            |> Mimic.stub(:source_mapping, fn _, _, _ -> {:ok, "the query"} end)
            |> Mimic.stub(:parameters, fn _ -> :ok end)

            :ok
          end
        end
      end

    quote do
      import Plug.Conn
      import Phoenix.ConnTest
      import LogflareWeb.Router.Helpers
      alias LogflareWeb.Router.Helpers, as: Routes
      import Logflare.Factory
      import Phoenix.LiveViewTest
      use Mimic
      unquote(mock_sql)

      # The default endpoint for testing
      @endpoint LogflareWeb.Endpoint

      setup context do
        Mimic.verify_on_exit!(context)
      end
    end
  end

  setup tags do
    :ok = Ecto.Adapters.SQL.Sandbox.checkout(Logflare.Repo)

    unless tags[:async] do
      Ecto.Adapters.SQL.Sandbox.mode(Logflare.Repo, {:shared, self()})
      # for global Mimic mocs
      Mimic.set_mimic_global(tags)
    end

    {:ok, conn: Phoenix.ConnTest.build_conn()}
  end
end
