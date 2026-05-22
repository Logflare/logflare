defmodule LogflareWeb.Plugs.VerifyDeclaredSourcesTest do
  @moduledoc false
  use LogflareWeb.ConnCase

  alias LogflareWeb.Plugs.VerifyDeclaredSources

  setup do
    insert(:plan)
    user = insert(:user)
    source_a = insert(:source, user: user)
    source_b = insert(:source, user: user)
    other_user = insert(:user)
    other_source = insert(:source, user: other_user)

    {:ok,
     user: user,
     source_a: source_a,
     source_b: source_b,
     other_user: other_user,
     other_source: other_source}
  end

  defp build_ingest_conn(body, user) do
    build_conn(:post, "/logs", body)
    |> assign(:user, user)
    |> assign(:resource_type, :source)
  end

  describe "passthrough" do
    test "no __LF_SOURCE on first event - single event map", %{user: user} do
      conn =
        build_ingest_conn(%{"message" => "hi"}, user)
        |> VerifyDeclaredSources.call(%{})

      refute conn.halted
      refute Map.get(conn.assigns, :declared_sources)
    end

    test "no __LF_SOURCE on first event - batch", %{user: user} do
      conn =
        build_ingest_conn(%{"batch" => [%{"message" => "a"}, %{"message" => "b"}]}, user)
        |> VerifyDeclaredSources.call(%{})

      refute conn.halted
      refute Map.get(conn.assigns, :declared_sources)
    end

    test "no __LF_SOURCE on first event - _json batch", %{user: user} do
      conn =
        build_ingest_conn(%{"_json" => [%{"message" => "a"}]}, user)
        |> VerifyDeclaredSources.call(%{})

      refute conn.halted
      refute Map.get(conn.assigns, :declared_sources)
    end

    test "missing resource_type", %{user: user} do
      conn =
        build_conn(:post, "/logs", %{"batch" => [%{"__LF_SOURCE" => "x"}]})
        |> assign(:user, user)
        |> VerifyDeclaredSources.call(%{})

      refute conn.halted
      refute Map.get(conn.assigns, :declared_sources)
    end

    test "missing user", %{} do
      conn =
        build_conn(:post, "/logs", %{"batch" => [%{"__LF_SOURCE" => "x"}]})
        |> assign(:resource_type, :source)
        |> VerifyDeclaredSources.call(%{})

      refute conn.halted
      refute Map.get(conn.assigns, :declared_sources)
    end
  end

  describe "multi-source mode" do
    test "resolves single declared source in batch", %{user: user, source_a: source_a} do
      token = Atom.to_string(source_a.token)

      conn =
        build_ingest_conn(
          %{"batch" => [%{"__LF_SOURCE" => token, "message" => "a"}]},
          user
        )
        |> VerifyDeclaredSources.call(%{})

      refute conn.halted
      declared = conn.assigns.declared_sources
      assert Map.has_key?(declared, token)
      assert declared[token].id == source_a.id
    end

    test "resolves multiple distinct declared sources", %{
      user: user,
      source_a: source_a,
      source_b: source_b
    } do
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

    test "single event map with __LF_SOURCE", %{user: user, source_a: source_a} do
      token = Atom.to_string(source_a.token)

      conn =
        build_ingest_conn(%{"__LF_SOURCE" => token, "message" => "a"}, user)
        |> VerifyDeclaredSources.call(%{})

      refute conn.halted
      assert conn.assigns.declared_sources[token].id == source_a.id
    end

    test "_json batch shape with __LF_SOURCE", %{user: user, source_a: source_a} do
      token = Atom.to_string(source_a.token)

      conn =
        build_ingest_conn(
          %{"_json" => [%{"__LF_SOURCE" => token, "message" => "a"}]},
          user
        )
        |> VerifyDeclaredSources.call(%{})

      refute conn.halted
      assert conn.assigns.declared_sources[token].id == source_a.id
    end
  end

  describe "authorization failures" do
    test "halts when declared source is owned by a different user", %{
      user: user,
      other_source: other_source
    } do
      token = Atom.to_string(other_source.token)

      conn =
        build_ingest_conn(
          %{"batch" => [%{"__LF_SOURCE" => token, "message" => "a"}]},
          user
        )
        |> VerifyDeclaredSources.call(%{})

      assert conn.halted
      assert conn.status == 401
    end

    test "halts when declared source UUID does not exist", %{user: user} do
      unknown_uuid = Ecto.UUID.generate()

      conn =
        build_ingest_conn(
          %{"batch" => [%{"__LF_SOURCE" => unknown_uuid, "message" => "a"}]},
          user
        )
        |> VerifyDeclaredSources.call(%{})

      assert conn.halted
      assert conn.status == 401
    end

    test "halts when one of many sources is unauthorized", %{
      user: user,
      source_a: source_a,
      other_source: other_source
    } do
      token_a = Atom.to_string(source_a.token)
      token_other = Atom.to_string(other_source.token)

      conn =
        build_ingest_conn(
          %{
            "batch" => [
              %{"__LF_SOURCE" => token_a, "message" => "a"},
              %{"__LF_SOURCE" => token_other, "message" => "b"}
            ]
          },
          user
        )
        |> VerifyDeclaredSources.call(%{})

      assert conn.halted
      assert conn.status == 401
    end

    test "halts when access token scope does not cover declared source", %{
      user: user,
      source_a: source_a,
      source_b: source_b
    } do
      {:ok, access_token} =
        Logflare.Auth.create_access_token(user, %{scopes: "ingest:source:#{source_a.id}"})

      token_b = Atom.to_string(source_b.token)

      conn =
        build_ingest_conn(
          %{"batch" => [%{"__LF_SOURCE" => token_b, "message" => "b"}]},
          user
        )
        |> assign(:access_token, access_token)
        |> VerifyDeclaredSources.call(%{})

      assert conn.halted
      assert conn.status == 401
    end

    test "allows when access token scope covers all declared sources", %{
      user: user,
      source_a: source_a
    } do
      {:ok, access_token} =
        Logflare.Auth.create_access_token(user, %{scopes: "ingest"})

      token_a = Atom.to_string(source_a.token)

      conn =
        build_ingest_conn(
          %{"batch" => [%{"__LF_SOURCE" => token_a, "message" => "a"}]},
          user
        )
        |> assign(:access_token, access_token)
        |> VerifyDeclaredSources.call(%{})

      refute conn.halted
      assert conn.assigns.declared_sources[token_a].id == source_a.id
    end
  end

  describe "invalid __LF_SOURCE values" do
    test "non-UUID value on first event still triggers multi-source mode but no declared sources resolved",
         %{user: user} do
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
