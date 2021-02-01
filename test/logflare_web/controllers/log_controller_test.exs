defmodule LogflareWeb.LogControllerTest do
  @moduledoc false
  use LogflareWeb.ConnCase
  alias Logflare.{Users, Sources}
  alias Logflare.Source
  alias Logflare.Source.RecentLogsServer, as: RLS
  alias Logflare.Source.BigQuery.Buffer, as: SourceBuffer
  alias Logflare.SystemMetricsSup
  alias Logflare.Plans
  alias Logflare.Plans.Plan
  use Mimic

  setup_all do
    {:ok, _} = Sources.Counters.start_link()
    :ok
  end

  setup do
    import Logflare.Factory

    {:ok, u1} = Users.insert_or_update_user(params_for(:user))
    {:ok, u2} = Users.insert_or_update_user(params_for(:user))

    u1 = Users.preload_defaults(u1)
    u2 = Users.preload_defaults(u2)

    {:ok, s} = Sources.create_source(params_for(:source, user_id: u1.id, api_quota: 1000), u1)

    s = Sources.get_by_and_preload(id: s.id)

    SystemMetricsSup.start_link()
    Sources.RateCounters.start_link()

    Source.RateCounterServer.start_link(%RLS{source_id: s.token})
    SourceBuffer.start_link(%RLS{source_id: s.token})

    {:ok, users: [u1, u2], sources: [s]}
  end

  describe "/logs/cloudflare POST request fails" do
    setup [:mock_plan_cache]

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
    setup [:expect_plan_cache]

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
    setup [:expect_plan_cache]

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
    setup [:mock_plan_cache]

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

      build_vercel_log_params = fn log_param ->
        %{
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
          |> build_vercel_log_params.()

        conn =
          conn
          |> post(
            log_path(conn, :vercel_ingest),
            log_params
          )

        assert json_response(conn, 200) == %{"message" => "Logged!"}
      end

      assert SourceBuffer.get_count("#{s.token}") == 4
    end
  end

  describe "SetVerifySource for HTML routes" do
    @tag :skip
    test "without a valid source", %{conn: conn, users: [u | _], sources: [_s | _]} do
      conn =
        conn
        |> assign(:user, u)
        |> get(source_path(conn, :show, 100))

      assert html_response(conn, 404) =~ "404"
      assert html_response(conn, 404) =~ "not found"
    end
  end

  describe "typecasting endpoint" do
    setup [:expect_plan_cache]

    test "works correctly", %{conn: conn, users: [u | _], sources: [s]} do
      params = %{
        "batch" => [
          %{
            "body" => %{
              "message" => "yo",
              "metadata" => %{
                "context" => %{"host" => "ontospace", "pid" => "324199"},
                "level" => "info",
                "number1" => "1"
              },
              "timestamp" => 1_593_455_694_611
            },
            "typecasts" => [
              %{"from" => "string", "path" => ["metadata", "number1"], "to" => "float"},
              %{"from" => "string", "path" => ["metadata", "context", "pid"], "to" => "float"}
            ]
          }
        ],
        "source" => Atom.to_string(s.token)
      }

      conn =
        conn
        |> put_req_header("x-api-key", u.api_key)
        |> post(
          log_path(conn, :create_with_typecasts),
          params
        )

      assert json_response(conn, 200) == %{"message" => "Logged!"}
      assert SourceBuffer.get_count(s.token) == 1
      [le] = SourceBuffer.get_log_events(s.token)

      sname = s.name

      assert %Logflare.LogEvent.Body{
               created_at: nil,
               message: "yo",
               metadata: %{
                 "context" => %{"host" => "ontospace", "pid" => 324_199.0},
                 "level" => "info",
                 "number1" => 1.0
               },
               timestamp: _
             } = le.body
    end
  end

  describe "Log params with transforms directives" do
    setup [:expect_plan_cache]

    test "numbers to floats typecasted correctly", %{conn: conn, users: [u | _], sources: [s | _]} do
      params = %{
        "source" => Atom.to_string(s.token),
        "batch" => [
          %{
            "log_entry" => "info message",
            "metadata" => %{
              "number_field" => 1,
              "number_field_2" => 1.0,
              "string_field" => "1",
              "nested" => %{
                "number_field2" => 2,
                "number_field2_1" => 2.1,
                "string_field2" => "2",
                "nested2" => %{
                  "number_field3" => 3,
                  "number_field3_1" => 3.1,
                  "string_field3" => "3"
                }
              }
            },
            "@logflareTransformDirectives" => %{
              "numbersToFloats" => true
            }
          }
        ]
      }

      conn =
        conn
        |> put_req_header("x-api-key", u.api_key)
        |> post(
          log_path(conn, :create),
          params
        )

      assert json_response(conn, 200) == %{"message" => "Logged!"}
      assert SourceBuffer.get_count(s.token) == 1
      [le] = SourceBuffer.get_log_events(s.token)

      sname = s.name

      assert %Logflare.LogEvent.Body{
               created_at: nil,
               message: "info message",
               metadata: %{
                 "nested" => %{
                   "nested2" => %{
                     "number_field3" => 3.0,
                     "number_field3_1" => 3.1,
                     "string_field3" => "3"
                   },
                   "number_field2" => 2.0,
                   "number_field2_1" => 2.1,
                   "string_field2" => "2"
                 },
                 "number_field" => 1.0,
                 "number_field_2" => 1.0,
                 "string_field" => "1"
               },
               timestamp: _
             } = le.body
    end
  end

  describe "Syslog payloads" do
    setup [:expect_plan_cache]

    test "syslog body", %{conn: conn, users: [u | _], sources: [s | _]} do
      body = """
      182 <190>1 2020-08-09T13:30:36.316601+00:00 host phx-limit phx-limit-5885669966-287kp - [36mweb.1  | [0m13:30:36.314 request_id=b4f92e4a104759b02593c34c41d2f0ce [info] Sent 200 in 1ms
      169 <190>1 2020-08-09T13:30:36.576402+00:00 host phx-limit phx-limit-5885669966-287kp - [36mweb.1  | [0m13:30:36.575 [info] CONNECTED TO Phoenix.LiveView.Socket in 202µs
      126 <190>1 2020-08-09T13:30:36.576423+00:00 host phx-limit phx-limit-5885669966-287kp - [36mweb.1  | [0m  Transport: :websocket
      149 <190>1 2020-08-09T13:30:36.576424+00:00 host phx-limit phx-limit-5885669966-287kp - [36mweb.1  | [0m  Serializer: Phoenix.Socket.V2.JSONSerializer
      443 <190>1 2020-08-09T13:30:36.576426+00:00 host phx-limit phx-limit-5885669966-287kp - [36mweb.1  | [0m  Parameters: %{\"_csrf_token\" => \"Rg1gVgJWUjkVCjISGmQKew0kZRYpYBwwpe00D78ZtsQqqUI9gK6zQReD\", \"_mounts\" => \"0\", \"_track_static\" => %{\"0\" => \"https://phx-limit.gigalixirapp.com/css/app-5e472e0beb5f275dce8c669b8ba7c47e.css?vsn=d\", \"1\" => \"https://phx-limit.gigalixirapp.com/js/app-13b608e49f856a3afa3085d9ce96d5fe.js?vsn=d\"}, \"vsn\" => \"2.0.0\"}
      """

      conn =
        conn
        |> put_req_header("x-api-key", u.api_key)
        |> put_req_header("content-type", "application/logplex-1")
        |> post(
          "/logs/logplex?source=#{s.token}",
          body
        )

      assert json_response(conn, 200) == %{"message" => "Logged!"}
      assert SourceBuffer.get_count(s.token) == 5
      [le1, le2, le3, le4, le5] = SourceBuffer.get_log_events(s.token)

      sname = s.name

      assert %Logflare.LogEvent.Body{
               created_at: nil,
               message:
                 "[36mweb.1  | [0m  Parameters: %{\"_csrf_token\" => \"Rg1gVgJWUjkVCjISGmQKew0kZRYpYBwwpe00D78ZtsQqqUI9gK6zQReD\", \"_mounts\" => \"0\", \"_track_static\" => %{\"0\" => \"https://phx-limit.gigalixirapp.com/css/app-5e472e0beb5f275dce8c669b8ba7c47e.css?vsn=d\", \"1\" => \"https://phx-limit.gigalixirapp.com/js/app-13b608e49f856a3afa3085d9ce96d5fe.js?vsn=d\"}, \"vsn\" => \"2.0.0\"}",
               metadata: %{
                 "appname" => "phx-limit",
                 "facility" => "local7",
                 "hostname" => "host",
                 "level" => "info",
                 "message_raw" =>
                   "443 <190>1 2020-08-09T13:30:36.576426+00:00 host phx-limit phx-limit-5885669966-287kp - [36mweb.1  | [0m  Parameters: %{\"_csrf_token\" => \"Rg1gVgJWUjkVCjISGmQKew0kZRYpYBwwpe00D78ZtsQqqUI9gK6zQReD\", \"_mounts\" => \"0\", \"_track_static\" => %{\"0\" => \"https://phx-limit.gigalixirapp.com/css/app-5e472e0beb5f275dce8c669b8ba7c47e.css?vsn=d\", \"1\" => \"https://phx-limit.gigalixirapp.com/js/app-13b608e49f856a3afa3085d9ce96d5fe.js?vsn=d\"}, \"vsn\" => \"2.0.0\"}",
                 "priority" => 190,
                 "process_id" => "phx-limit-5885669966-287kp",
                 "severity" => "info"
               },
               timestamp: 1_596_979_836_576_426
             } = le1.body

      assert %Logflare.LogEvent.Body{
               created_at: nil,
               message: "[36mweb.1  | [0m  Serializer: Phoenix.Socket.V2.JSONSerializer",
               metadata: %{
                 "appname" => "phx-limit",
                 "facility" => "local7",
                 "hostname" => "host",
                 "level" => "info",
                 "message_raw" =>
                   "149 <190>1 2020-08-09T13:30:36.576424+00:00 host phx-limit phx-limit-5885669966-287kp - [36mweb.1  | [0m  Serializer: Phoenix.Socket.V2.JSONSerializer",
                 "priority" => 190,
                 "process_id" => "phx-limit-5885669966-287kp",
                 "severity" => "info"
               },
               timestamp: 1_596_979_836_576_424
             } = le2.body

      assert %Logflare.LogEvent.Body{
               created_at: nil,
               message: "[36mweb.1  | [0m  Transport: :websocket",
               metadata: %{
                 "appname" => "phx-limit",
                 "facility" => "local7",
                 "hostname" => "host",
                 "level" => "info",
                 "message_raw" =>
                   "126 <190>1 2020-08-09T13:30:36.576423+00:00 host phx-limit phx-limit-5885669966-287kp - [36mweb.1  | [0m  Transport: :websocket",
                 "priority" => 190,
                 "process_id" => "phx-limit-5885669966-287kp",
                 "severity" => "info"
               },
               timestamp: 1_596_979_836_576_423
             } = le3.body

      assert %Logflare.LogEvent.Body{
               created_at: nil,
               message:
                 "[36mweb.1  | [0m13:30:36.575 [info] CONNECTED TO Phoenix.LiveView.Socket in 202µs",
               metadata: %{
                 "appname" => "phx-limit",
                 "facility" => "local7",
                 "hostname" => "host",
                 "level" => "info",
                 "message_raw" =>
                   "169 <190>1 2020-08-09T13:30:36.576402+00:00 host phx-limit phx-limit-5885669966-287kp - [36mweb.1  | [0m13:30:36.575 [info] CONNECTED TO Phoenix.LiveView.Socket in 202µs",
                 "priority" => 190,
                 "process_id" => "phx-limit-5885669966-287kp",
                 "severity" => "info"
               },
               timestamp: 1_596_979_836_576_402
             } = le4.body

      assert %Logflare.LogEvent.Body{
               created_at: nil,
               message:
                 "[36mweb.1  | [0m13:30:36.314 request_id=b4f92e4a104759b02593c34c41d2f0ce [info] Sent 200 in 1ms",
               metadata: %{
                 "appname" => "phx-limit",
                 "facility" => "local7",
                 "hostname" => "host",
                 "level" => "info",
                 "message_raw" =>
                   "182 <190>1 2020-08-09T13:30:36.316601+00:00 host phx-limit phx-limit-5885669966-287kp - [36mweb.1  | [0m13:30:36.314 request_id=b4f92e4a104759b02593c34c41d2f0ce [info] Sent 200 in 1ms",
                 "priority" => 190,
                 "process_id" => "phx-limit-5885669966-287kp",
                 "severity" => "info"
               },
               timestamp: 1_596_979_836_316_601
             } = le5.body
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

  def expect_plan_cache(_ctx) do
    expect(Plans.Cache, :get_plan_by, fn _ ->
      %Plan{
        stripe_id: "31415"
      }
    end)

    :ok
  end

  def mock_plan_cache(_ctx) do
    stub(Plans.Cache, :get_plan_by, fn _ ->
      %Plan{
        stripe_id: "31415"
      }
    end)

    :ok
  end
end
