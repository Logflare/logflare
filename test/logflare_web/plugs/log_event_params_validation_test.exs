defmodule LogflareWeb.Plugs.LogEventParamsValidationTest do
  @moduledoc false
  use LogflareWeb.ConnCase
  import Logflare.DummyFactory
  alias Logflare.Repo
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
        |> assign(:source, %{s | user: u})
        |> assign(:log_events, [
          %{
            "metadata" => %{
              "users" => [
                %{
                  "id" => 1
                },
                %{
                  "id" => "2"
                }
              ]
            },
            "log_entry" => true
          }
        ])
        |> LogEventParamsValidation.call(%{})

      assert conn.halted == true
    end

    test "doesn't halt conn if valid", %{users: [u], sources: [s]} do
      conn =
        build_conn(:post, "/logs")
        |> assign(:user, u)
        |> assign(:source, s)
        |> assign(:log_events, [
          %{
            "users" => [
              %{
                "id" => 1
              },
              %{
                "id" => 2
              }
            ]
          }
        ])
        |> LogEventParamsValidation.call(%{})

      assert conn.halted == false
    end
  end
end
