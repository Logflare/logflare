defmodule LogflareWeb.LogControllerTest do
  @moduledoc false
  use LogflareWeb.ConnCase
  alias Logflare.SystemMetrics.AllLogsLogged
  alias Logflare.{Users, Sources}
  alias Logflare.Source
  alias Logflare.Tracker
  alias Logflare.Source.RecentLogsServer, as: RLS
  alias Logflare.Source.BigQuery.Buffer, as: SourceBuffer
  alias Logflare.SystemMetricsSup

  setup do
    import Logflare.Factory

    [u1, u2] = insert_list(2, :user)

    u1 = Users.preload_defaults(u1)
    u2 = Users.preload_defaults(u2)

    s = insert(:source, user_id: u1.id, api_quota: 50)

    s = Sources.get_by_and_preload(id: s.id)

    Tracker.SourceNodeRates.start_link()

    SystemMetricsSup.start_link()
    Sources.Counters.start_link()
    Sources.RateCounters.start_link()

    # {:ok, _} = RLS.start_link(%RLS{source_id: s.token})

    Source.RateCounterServer.start_link(%RLS{source_id: s.token})
    SourceBuffer.start_link(%RLS{source_id: s.token})

    # Process.sleep(1000)
    # Tracker.Cache.cache_cluster_rates()

    {:ok, users: [u1, u2], sources: [s]}
  end

  describe "/logs/cloudflare POST request fails" do
    test "without an API token", %{conn: conn, users: _} do
      conn = post(conn, log_path(conn, :create), %{"log_entry" => "valid log entry"})
      assert json_response(conn, 401) == %{"message" => "Error: please set ingest API key"}
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
        |> post(
          log_path(conn, :create),
          %{
            "log_entry" => "valid log entry",
            "source_name" => "%%%unknown%%%"
          }
        )

      assert json_response(conn, 406) == %{
               "message" => "Source or source_name is nil, empty or not found."
             }

      conn =
        conn
        |> recycle()
        |> put_req_header("x-api-key", u.api_key)
        |> post(
          log_path(conn, :create),
          %{
            "log_entry" => "valid log entry",
            "source" => Faker.UUID.v4()
          }
        )

      assert json_response(conn, 406) == %{
               "message" => "Source or source_name is nil, empty or not found."
             }
    end

    test "with nil or empty log_entry", %{conn: conn, users: [u | _], sources: [s | _]} do
      err_message = %{"message" => ["body: %{message: [\"can't be blank\"]}\n"]}

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

    test "with invalid source token", %{conn: conn, users: [u | _], sources: _} do
      err_message = %{
        "message" => "Source or source_name is nil, empty or not found."
      }

      conn =
        conn
        |> put_req_header("x-api-key", u.api_key)
        |> post(
          log_path(conn, :create),
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
            "source" => "signin",
            "log_entry" => "valid"
          }
        )

      assert json_response(conn, 406) == err_message
    end

    test "with invalid field types", %{conn: conn, users: [u | _], sources: [s | _]} do
      err_message = %{
        "message" => [
          "Metadata validation error: values with the same field path must have the same type."
        ]
      }

      conn =
        conn
        |> put_req_header("x-api-key", u.api_key)
        |> post(
          log_path(conn, :create),
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
            "source" => Atom.to_string(s.token),
            "log_entry" => "valid"
          }
        )

      assert json_response(conn, 406) == err_message
    end

    test "fails for unauthorized user", %{conn: conn, users: [_u1, u2], sources: [s]} do
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
      assert SourceBuffer.get_count(s.token) == 1
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
      assert SourceBuffer.get_count(s.token) == 1
    end
  end

  describe "/logs/elixir/logger POST request succeeds" do
    test "with valid batch", %{conn: conn, users: [u | _], sources: [s | _]} do
      log_params = build_log_params()

      conn =
        conn
        |> assign(:user, u)
        |> assign(:source, s)
        |> post(
          log_path(conn, :elixir_logger),
          %{"batch" => [log_params, log_params, log_params]}
        )

      assert json_response(conn, 200) == %{"message" => "Logged!"}
      assert SourceBuffer.get_count("#{s.token}") == 3
    end

    test "with nil and empty map metadata", %{conn: conn, users: [u | _], sources: [s | _]} do
      conn =
        conn
        |> assign(:user, u)
        |> assign(:source, s)
        |> post(
          log_path(conn, :create),
          %{
            "log_entry" => "valid",
            "level" => "info",
            "metadata" => metadata()
          }
        )

      assert json_response(conn, 200) == %{"message" => "Logged!"}
    end
  end

  describe "ZEIT log params ingest" do
    @describetag :run
    test "with valid batch", %{conn: conn, users: [u | _], sources: [s | _]} do
      log_param = %{
        "buildId" => "identifier of build only on build logs",
        "deploymentId" => "identifier of deployement",
        "host" => "hostname",
        "id" => "identifier",
        "message" => "log message",
        "path" => "path",
        "projectId" => "identifier of project",
        "proxy" => %{
          "cacheId" => "original request id when request is served from cache",
          "clientIp" => "client IP",
          "host" => "hostname",
          "method" => "method of request",
          "path" => "path of proxy request",
          "region" => "region request is processed",
          "scheme" => "protocol of request",
          "statusCode" => 200,
          "timestamp" => 1_580_845_449_483,
          "userAgent" => nil
        },
        "requestId" => "identifier of request only on runtime logs",
        "source" => "source",
        "statusCode" => 200,
        "timestamp" => 1_580_845_449_483
      }

      build_zeit_log_params = fn log_param ->
        log_params = %{
          "_json" => [log_param],
          "api_key" => "H-a2QUCFTAFR",
          "source_id" => "9e885e3b-f9c0-4d2f-a30d-b78dfbf2d7ef"
        }
      end

      user_agents = [
        ["One User Agent"],
        "Two User Agent",
        ["One User Agent", "Two User Agent"],
        []
      ]

      conn =
        conn
        |> assign(:user, u)
        |> assign(:source, s)

      for ua <- user_agents do
        log_params =
          log_param
          |> put_in(["proxy", "userAgent"], ua)
          |> build_zeit_log_params.()

        conn =
          conn
          |> post(
            log_path(conn, :zeit_ingest),
            log_params
          )

        assert json_response(conn, 200) == %{"message" => "Logged!"}
      end

      assert SourceBuffer.get_count("#{s.token}") == 4
    end
  end

  describe "SetVerifySource for HTML routes" do
    test "without a valid source", %{conn: conn, users: [u | _], sources: [s | _]} do
      log_params = build_log_params()

      conn =
        conn
        |> assign(:user, u)
        |> get(source_path(conn, :show, 100))

      assert get_flash(conn) == %{"error" => "Source not found!"}
      assert redirected_to(conn, 302) == "/"
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

  def build_log_params() do
    %{
      "message" => "log message",
      "metadata" => %{},
      "timestamp" => System.system_time(:microsecond)
    }
  end
end
