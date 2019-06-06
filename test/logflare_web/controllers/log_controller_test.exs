defmodule LogflareWeb.LogControllerTest do
  @moduledoc false
  use LogflareWeb.ConnCase
  alias Logflare.{Sources.Counters, Users, SystemCounter, Sources}
  alias Logflare.Source.BigQuery.Buffer, as: SourceBuffer
  use Placebo

  setup do
    import Logflare.DummyFactory

    u1 = insert(:user)
    u2 = insert(:user)

    s =
      insert(:source, user_id: u1.id)
      |> Sources.preload_defaults()

    u1 = Users.default_preloads(u1)
    u2 = Users.default_preloads(u2)

    {:ok, users: [u1, u2], sources: [s]}
  end

  describe "/logs/cloudflare POST request fails" do
    test "without an API token", %{conn: conn, users: [u | _]} do
      conn = post(conn, log_path(conn, :create), %{"log_entry" => "valid log entry"})
      assert json_response(conn, 401) == %{"message" => "Error: please set API token"}
    end

    test "without source or source_name", %{conn: conn, users: [u | _]} do
      conn =
        conn
        |> put_req_header("x-api-key", u.api_key)
        |> post(log_path(conn, :create), %{"log_entry" => "valid log entry"})

      assert json_response(conn, 406) == %{
               "message" => "Source or source_name is nil, empty or not found."
             }
    end

    test "to a unknown source and source_name", %{conn: conn, users: [u | _]} do
      conn =
        conn
        |> put_req_header("x-api-key", u.api_key)
        |> post(log_path(conn, :create), %{
          "log_entry" => "valid log entry",
          "source_name" => "%%%unknown%%%"
        })

      assert json_response(conn, 406) == %{
               "message" => "Source or source_name is nil, empty or not found."
             }

      conn =
        conn
        |> recycle()
        |> put_req_header("x-api-key", u.api_key)
        |> post(log_path(conn, :create), %{
          "log_entry" => "valid log entry",
          "source" => Faker.UUID.v4()
        })

      assert json_response(conn, 406) == %{
               "message" => "Source or source_name is nil, empty or not found."
             }
    end

    test "with nil or empty log_entry", %{conn: conn, users: [u | _], sources: [s | _]} do
      err_message = %{"message" => "Log entry needed."}

      for log_entry <- [%{}, nil, [], ""] do
        conn =
          conn
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

    test "fails for unauthorized user", %{conn: conn, users: [u1, u2], sources: [s]} do
      conn =
        conn
        |> put_req_header("x-api-key", u2.api_key)
        |> post(
          log_path(conn, :create),
          %{
            "log_entry" => "log binary message",
            "source" => Atom.to_string(s.token),
            "metadata" => metadata()
          }
        )

      assert json_response(conn, 403) == %{"message" => "Source is not owned by this user."}
    end
  end

  describe "/logs/cloudflare POST request succeeds" do
    setup [:allow_mocks]

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
      assert_called_modules_from_logs_context(s.token)
    end

    test "succeeds with source (token)", %{conn: conn, users: [u | _], sources: [s]} do
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
      assert_called_modules_from_logs_context(s.token)
    end
  end

  describe "/logs/elixir/logger POST request succeeds" do
    setup [:allow_mocks]

    test "with valid batch", %{conn: conn, users: [u | _], sources: [s | _]} do
      log_event = build_log_event()

      conn =
        conn
        |> assign(:user, u)
        |> assign(:source, s)
        |> post(log_path(conn, :elixir_logger), %{"batch" => [log_event, log_event, log_event]})

      assert json_response(conn, 200) == %{"message" => "Logged!"}
      assert_called Sources.Counters.incriment(s.token), times(3)
      assert_called Sources.Counters.get_total_inserts(s.token), times(3)
      assert_called SourceBuffer.push("#{s.token}", any()), times(3)
      assert_called SystemCounter.incriment(any()), times(3)
      assert_called SystemCounter.log_count(any()), times(3)
    end
  end

  defp assert_called_modules_from_logs_context(token) do
    assert_called Sources.Counters.incriment(token), once()
    assert_called Sources.Counters.get_total_inserts(token), once()
    assert_called SourceBuffer.push("#{token}", any()), once()
    assert_called SystemCounter.incriment(any()), once()
    assert_called SystemCounter.log_count(any()), once()
  end

  defp allow_mocks(_context) do
    allow Sources.Counters.incriment(any()), return: :ok
    allow SourceBuffer.push(any(), any()), return: :ok
    allow Sources.Counters.get_total_inserts(any()), return: {:ok, 1}
    allow SystemCounter.incriment(any()), return: :ok
    allow SystemCounter.log_count(any()), return: {:ok, 1}
    :ok
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

  def build_log_event() do
    %{
      "message" => "log message",
      "metadata" => %{},
      "timestamp" => NaiveDateTime.to_iso8601(NaiveDateTime.utc_now()),
      "level" => "info"
    }
  end
end
