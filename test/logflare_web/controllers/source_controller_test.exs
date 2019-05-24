defmodule LogflareWeb.SourceControllerTest do
  @moduledoc false
  import LogflareWeb.Router.Helpers
  use LogflareWeb.ConnCase

  alias Logflare.{SystemCounter, Sources, Repo}
  import Logflare.DummyFactory

  setup do
    :ok = Ecto.Adapters.SQL.Sandbox.mode(Repo, {:shared, self()})
    s1 = insert(:source, token: Faker.UUID.v4())
    s2 = insert(:source, token: Faker.UUID.v4())
    u = insert(:user, sources: [s1, s2])
    SystemCounter.start_link()
    {:ok, users: [u], sources: [s1, s2], conn: Phoenix.ConnTest.build_conn()}
  end

  describe "dashboard" do
    test "renders dashboard", %{conn: conn, users: [u]} do
      conn =
        conn
        |> assign(:user, u)
        |> get("/dashboard")

      assert html_response(conn, 200) =~ "dashboard"
    end
  end
end
