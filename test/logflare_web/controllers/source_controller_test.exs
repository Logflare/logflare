defmodule LogflareWeb.SourceControllerTest do
  @moduledoc false
  import LogflareWeb.Router.Helpers
  use LogflareWeb.ConnCase

  alias Logflare.{SystemCounter, Sources, Repo}
  alias Logflare.Logs.RejectedEvents
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
    test "renders dashboard", %{conn: conn, users: [u], sources: [s1, s2]} do
      conn =
        conn
        |> assign(:user, u)
        |> get("/dashboard")

      dash_sources = Enum.map(conn.assigns.sources, & &1.metrics)
      dash_source_1 = hd(dash_sources)

      source_stat_fields = ~w[avg buffer inserts latest max rate id]a

      assert is_list(dash_sources)
      assert source_stat_fields -- Map.keys(dash_source_1) === []
      assert hd(conn.assigns.sources).id == s1.id
      assert hd(conn.assigns.sources).token == s1.token
      assert html_response(conn, 200) =~ "dashboard"
    end

    test "renders rejected logs page", %{conn: conn, users: [u], sources: [s1, s2]} do
      RejectedEvents.injest(%{
        error: Logflare.Validator.DeepFieldTypes,
        batch: [%{"no_log_entry" => true, "timestamp" => ""}],
        source: s1
      })

      conn =
        conn
        |> assign(:user, u)
        |> get("/sources/#{s1.id}/rejected")

      assert html_response(conn, 200) =~ "dashboard"

      assert [
               %{
                 message:
                   "Metadata validation error: values with the same field path must have the same type.",
                 payload: [%{"no_log_entry" => true, "timestamp" => ""}],
                 timestamp: _
               }
             ] = conn.assigns.logs
    end

    test "update with valid params", %{conn: conn, users: [u], sources: [s1, s2]} do
      new_name = Faker.String.base64()
      params = %{"id" => s1.id, "source" => %{"favorite" => true, "name" => new_name}}

      conn =
        conn
        |> assign(:user, u)
        |> patch("/sources/#{s1.id}", params)

      s1_new = Sources.get_by_id(s1.token)

      assert html_response(conn, 302) =~ "redirected"
      assert s1_new.name == new_name
      assert s1_new.favorite == true
    end
  end
end
