defmodule LogflareWeb.Plugs.VerifyDeclaredSourcesTest do
  @moduledoc false
  use LogflareWeb.ConnCase

  alias LogflareWeb.Plugs.VerifyDeclaredSources

  setup do
    insert(:plan)
    user = insert(:user)
    {:ok, user: user}
  end

  defp build_ingest_conn(body, user) do
    build_conn(:post, "/logs", body)
    |> assign(:user, user)
    |> assign(:resource_type, :source)
  end

  describe "passthrough" do
    test "no __LF_SOURCE on first event is a no-op for all body shapes", %{user: user} do
      for body <- [
            %{"message" => "hi"},
            %{"batch" => [%{"message" => "a"}, %{"message" => "b"}]},
            %{"_json" => [%{"message" => "a"}]}
          ] do
        conn =
          build_ingest_conn(body, user)
          |> VerifyDeclaredSources.call(%{})

        refute conn.halted
        refute Map.get(conn.assigns, :declared_sources)
      end
    end

    test "missing resource_type", %{user: user} do
      conn =
        build_conn(:post, "/logs", %{"batch" => [%{"__LF_SOURCE" => "x"}]})
        |> assign(:user, user)
        |> VerifyDeclaredSources.call(%{})

      refute conn.halted
      refute Map.get(conn.assigns, :declared_sources)
    end

    test "missing user" do
      conn =
        build_conn(:post, "/logs", %{"batch" => [%{"__LF_SOURCE" => "x"}]})
        |> assign(:resource_type, :source)
        |> VerifyDeclaredSources.call(%{})

      refute conn.halted
      refute Map.get(conn.assigns, :declared_sources)
    end
  end

  describe "multi-source mode" do
    test "resolves single declared source across all body shapes", %{user: user} do
      source = insert(:source, user: user)
      token = Atom.to_string(source.token)

      for body <- [
            %{"batch" => [%{"__LF_SOURCE" => token, "message" => "a"}]},
            %{"_json" => [%{"__LF_SOURCE" => token, "message" => "a"}]},
            %{"__LF_SOURCE" => token, "message" => "a"}
          ] do
        conn =
          build_ingest_conn(body, user)
          |> VerifyDeclaredSources.call(%{})

        refute conn.halted
        assert conn.assigns.declared_sources[token].id == source.id
      end
    end

    test "resolves multiple distinct declared sources", %{user: user} do
      source_a = insert(:source, user: user)
      source_b = insert(:source, user: user)
      token_a = Atom.to_string(source_a.token)
      token_b = Atom.to_string(source_b.token)

      conn =
        build_ingest_conn(
          %{
            "batch" => [
              %{"__LF_SOURCE" => token_a, "message" => "a"},
              %{"__LF_SOURCE" => token_b, "message" => "b"},
              %{"__LF_SOURCE" => token_a, "message" => "a2"}
            ]
          },
          user
        )
        |> VerifyDeclaredSources.call(%{})

      refute conn.halted
      assert map_size(conn.assigns.declared_sources) == 2
      assert conn.assigns.declared_sources[token_a].id == source_a.id
      assert conn.assigns.declared_sources[token_b].id == source_b.id
    end
  end

  describe "authorization failures" do
    test "halts with 401 for unauthorized sources", %{user: user} do
      source_a = insert(:source, user: user)
      other_source = insert(:source, user: insert(:user))
      {:ok, scoped_token} =
        Logflare.Auth.create_access_token(user, %{scopes: "ingest:source:#{source_a.id}"})

      for {label, body, extra_assigns} <- [
            {"source owned by another user",
             %{"batch" => [%{"__LF_SOURCE" => Atom.to_string(other_source.token)}]}, []},
            {"non-existent UUID",
             %{"batch" => [%{"__LF_SOURCE" => Ecto.UUID.generate()}]}, []},
            {"one of many sources unauthorized",
             %{
               "batch" => [
                 %{"__LF_SOURCE" => Atom.to_string(source_a.token)},
                 %{"__LF_SOURCE" => Atom.to_string(other_source.token)}
               ]
             }, []},
            {"scope does not cover declared source",
             %{"batch" => [%{"__LF_SOURCE" => Atom.to_string(source_a.token)}]},
             [access_token: scoped_token]}
          ] do
        conn =
          build_ingest_conn(body, user)
          |> then(fn c ->
            Enum.reduce(extra_assigns, c, fn {k, v}, c -> assign(c, k, v) end)
          end)
          |> VerifyDeclaredSources.call(%{})

        assert conn.halted, "expected halt for: #{label}"
        assert conn.status == 401, "expected 401 for: #{label}"
      end
    end

    test "allows when access token scope covers all declared sources", %{user: user} do
      source = insert(:source, user: user)
      {:ok, access_token} = Logflare.Auth.create_access_token(user, %{scopes: "ingest"})
      token = Atom.to_string(source.token)

      conn =
        build_ingest_conn(
          %{"batch" => [%{"__LF_SOURCE" => token, "message" => "a"}]},
          user
        )
        |> assign(:access_token, access_token)
        |> VerifyDeclaredSources.call(%{})

      refute conn.halted
      assert conn.assigns.declared_sources[token].id == source.id
    end
  end

  describe "invalid __LF_SOURCE values" do
    test "non-UUID value triggers multi-source mode but resolves no sources", %{user: user} do
      conn =
        build_ingest_conn(
          %{"batch" => [%{"__LF_SOURCE" => "not-a-uuid", "message" => "a"}]},
          user
        )
        |> VerifyDeclaredSources.call(%{})

      refute conn.halted
      assert conn.assigns.declared_sources == %{}
    end
  end
end
