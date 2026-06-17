# credo:disable-for-this-file Credo.Check.Refactor.IoPuts
#
# Usage: MIX_ENV=test mix run test/profiling/bq_schema_update_internals_bench.exs
#
# Env:
#   SAVE_SNAPSHOT=1     - append this run's results to bq_schema_update_internals_bench.history.exs
#   LABEL="..."         - optional label for the new entry
#   MACHINE="..."       - optional machine identifier so cross-machine entries can be filtered
#   PROFILE=1           - run :tprof after the benchmark (Benchee profile_after)
#   TPROF_TYPE=time     - :tprof type when PROFILE=1 (time | calls | memory; default time)
#
# Targets the pure planning work used by Logflare.Sources.Source.BigQuery.Schema's
# GenServer callback, without mailbox scheduling or BigQuery side effects.

alias Logflare.Profiling
alias Logflare.Sources.Source.BigQuery.Schema
alias Logflare.Sources.Source.BigQuery.SchemaBuilder

make_input = fn body, update_body ->
  %{
    body: body,
    update_body: update_body,
    schema: SchemaBuilder.build_table_schema(body, SchemaBuilder.initial_table_schema())
  }
end

inputs =
  %{
    "scalars" =>
      make_input.(
        %{
          "event_message" => "POST /login",
          "metadata" => %{
            "datacenter" => "aws",
            "ip_address" => "100.100.100.100",
            "request_method" => "POST",
            "user" => %{
              "browser" => "Firefox",
              "company" => "Apple",
              "id" => 38,
              "login_count" => 154,
              "vip" => true
            }
          },
          "timestamp" => "2026-01-21T17:54:48.144506Z"
        },
        %{
          "event_message" => "POST /login",
          "metadata" => %{
            "datacenter" => "aws",
            "ip_address" => "100.100.100.100",
            "request_method" => "POST",
            "user" => %{
              "browser" => "Firefox",
              "company" => "Apple",
              "id" => 38,
              "login_count" => 154,
              "plan" => "pro",
              "vip" => true
            }
          },
          "timestamp" => "2026-01-21T17:54:48.144506Z"
        }
      ),
    "lists" =>
      make_input.(
        %{
          "event_message" => "worker exception",
          "metadata" => %{
            "attempts" => [1, 2, 3],
            "stacktrace" => [
              %{"file" => "lib/app/router.ex", "function" => "call/2", "line" => 42},
              %{"file" => "lib/app/worker.ex", "function" => "perform/1", "line" => 108}
            ],
            "tags" => ["api", "critical"]
          },
          "timestamp" => "2026-01-21T17:54:48.144506Z"
        },
        %{
          "event_message" => "worker exception",
          "metadata" => %{
            "attempts" => [1, 2, 3],
            "stacktrace" => [
              %{"file" => "lib/app/router.ex", "function" => "call/2", "line" => 42},
              %{
                "args" => ["project_id", "request_id"],
                "file" => "lib/app/worker.ex",
                "function" => "perform/1",
                "line" => 108
              }
            ],
            "tags" => ["api", "critical"]
          },
          "timestamp" => "2026-01-21T17:54:48.144506Z"
        }
      ),
    "edge log" =>
      make_input.(
        %{
          "event_message" =>
            "POST | 200 | 3.254.227.51 | https://example.supabase.co/rest/v1/calls",
          "id" => "3701391c-dead-405d-9943-61cc7576da87",
          "metadata" => %{
            "request" => %{
              "cf" => %{
                "asn" => 16_509,
                "botManagement" => %{
                  "corporateProxy" => false,
                  "score" => 46,
                  "verifiedBot" => false
                },
                "city" => "Dublin",
                "colo" => "DUB",
                "country" => "IE"
              },
              "headers" => %{
                "content_type" => "application/json",
                "host" => "example.supabase.co",
                "user_agent" => "Deno/2.1.4"
              },
              "method" => "POST",
              "path" => "/rest/v1/calls"
            },
            "response" => %{
              "origin_time" => 12,
              "status_code" => 200
            }
          },
          "timestamp" => "2026-01-21T17:54:48.144506Z"
        },
        %{
          "event_message" =>
            "POST | 200 | 3.254.227.51 | https://example.supabase.co/rest/v1/calls",
          "id" => "3701391c-dead-405d-9943-61cc7576da87",
          "metadata" => %{
            "request" => %{
              "cf" => %{
                "asn" => 16_509,
                "botManagement" => %{
                  "corporateProxy" => false,
                  "ja4Signals" => %{
                    "browser_ratio_1h" => 0.0026,
                    "cache_ratio_1h" => 0.0389,
                    "reqs_rank_1h" => 36
                  },
                  "score" => 46,
                  "verifiedBot" => false
                },
                "city" => "Dublin",
                "colo" => "DUB",
                "country" => "IE"
              },
              "headers" => %{
                "content_type" => "application/json",
                "host" => "example.supabase.co",
                "user_agent" => "Deno/2.1.4"
              },
              "method" => "POST",
              "path" => "/rest/v1/calls"
            },
            "response" => %{
              "origin_time" => 12,
              "status_code" => 200
            }
          },
          "timestamp" => "2026-01-21T17:54:48.144506Z"
        }
      )
  }

profile_after =
  if System.get_env("PROFILE") == "1" do
    type = String.to_existing_atom(System.get_env("TPROF_TYPE") || "time")
    {:tprof, type: type}
  else
    false
  end

suite =
  Benchee.run(
    %{
      "noop" => fn %{body: body, schema: schema} ->
        Schema.plan_update(body, schema, %{next_update: 0})
      end,
      "update" => fn %{update_body: body, schema: schema} ->
        Schema.plan_update(body, schema, %{next_update: 0})
      end
    },
    inputs: inputs,
    time: 5,
    warmup: 2,
    memory_time: 3,
    reduction_time: 3,
    profile_after: profile_after
  )

history_path = Path.expand("bq_schema_update_internals_bench.history.exs", __DIR__)
Profiling.track(suite, history_path)
