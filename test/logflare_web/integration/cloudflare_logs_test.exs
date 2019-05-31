defmodule LogflareWeb.CloudflareLogsTest do
  @moduledoc false
  use LogflareWeb.ConnCase
  @moduletag integration: true

  setup do
    import Logflare.DummyFactory
    s = insert(:source)
    u1 = insert(:user, sources: [s])
    u2 = insert(:user)

    {:ok, users: [u1, u2], sources: [s]}
  end

  describe "/logs/cloudflare POST request" do
    test "fails without an API token", %{conn: conn, users: [u | _], sources: [s | _]} do
      conn = post(conn, log_path(conn, :create), %{"log_entry" => "valid log entry"})
      assert json_response(conn, 401) == %{"message" => "Error: please set API token"}
    end

    test "fails without source or source_name", %{conn: conn, users: [u | _], sources: [s | _]} do
      conn =
        conn
        |> put_req_header("x-api-key", u.api_key)
        |> post(log_path(conn, :create), %{"log_entry" => "valid log entry"})

      assert json_response(conn, 406) == %{"message" => "Source or source_name needed."}
    end

    test "fails with nil or empty log_entry", %{conn: conn, users: [u | _], sources: [s | _]} do
      err_message = %{"message" => "Log entry needed."}

      for log_entry <- [%{}, nil, [], ""] do
        conn = conn
               |> put_req_header("x-api-key", u.api_key)
               |> post(
                    log_path(conn, :create),
                    %{
                      "log_entry" => log_entry,
                      "source" => Atom.to_string(s.token),
                      "metadata" => metadata()
                    }
                  )
        assert json_response(conn, 406) == err_message
      end
    end

    test "succeeds with source_name", %{conn: conn, users: [u | _], sources: [s]} do
      conn =
        conn
        |> put_req_header("x-api-key", u.api_key)
        |> post(
             log_path(conn, :create),
             %{
               "log_entry" => "log binary message",
               "source_name" => s.name,
               "metadata" => metadata()
             }
           )

      assert json_response(conn, 200) == %{"message" => "Logged!"}
    end

    test "succeeds with source (source token)", %{conn: conn, users: [u | _], sources: [s]} do
      conn =
        conn
        |> put_req_header("x-api-key", u.api_key)
        |> post(
             log_path(conn, :create),
             %{
               "log_entry" => "log binary message",
               "source" => Atom.to_string(s.token),
               "metadata" => metadata()
             }
           )

      assert json_response(conn, 200) == %{"message" => "Logged!"}
    end
  end

  def metadata() do
    %{
      "datacenter" => "aws",
      "ip_address" => "100.100.100.100",
      "request_headers" => %{
        "connection" => "close",
        "servers" => %{
          "blah" => "water",
          "home" => "not home",
          "deep_nest" => [
            %{
              "more_deep_nest" => %{
                "a" => 1
              }
            },
            %{
              "more_deep_nest2" => %{
                "a" => 2
              }
            }
          ]
        },
        "user_agent" => "chrome"
      },
      "request_method" => "POST"
    }
  end
end
