defmodule LogflareWeb.Plugs.LogEventParamsValidationTest do
  @moduledoc false
  use LogflareWeb.ConnCase
  alias LogflareWeb.Plugs.LogEventParamsValidation

  setup do
    :ok = Ecto.Adapters.SQL.Sandbox.mode(Logflare.Repo, {:shared, self()})
  end

  setup do
    s1 = insert(:source)
    u1 = insert(:user, %{api_key: @api_key})
    {:ok, users: [u1], sources: [s1]}
  end

  describe "Plugs.LogEventParamsValidation" do
    test "halts conn if invalid", %{users: [u], sources: [s]} do
      conn =
        build_conn(:post, "/logs")
        |> assign(:user, u)
        |> assign(:source, s)
        |> LogEventParamsValidation.call(%{
          batch => [
            %{
              "valid_batch" => true
            }
          ]
        })

      assert conn.halted == true
    end

    test "doesn't halt conn if valid", %{users: [u], sources: [s]} do
      conn =
        build_conn(:post, "/logs")
        |> assign(:user, u)
        |> assign(:source, s)
        |> LogEventParamsValidation.call(%{
          batch => [
            %{
              "valid_batch" => false
            }
          ]
        })

      assert conn.halted == false
    end
  end
end
