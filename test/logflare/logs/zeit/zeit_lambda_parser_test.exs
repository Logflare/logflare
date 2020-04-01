defmodule Logflare.Logs.Zeit.NimbleLambdaMessageParserTest do
  @moduledoc false
  use Logflare.DataCase, async: true
  import Logflare.Logs.Zeit.NimbleLambdaMessageParser

  describe "Zeit Lambda Message Parser" do
    test "example message 1" do
      message = """
      START RequestId: 026080a5-4157-4f7d-8256-0b61aa0fb167 Version: $LATEST
      END RequestId: 026080a5-4157-4f7d-8256-0b61aa0fb167
      REPORT RequestId: 026080a5-4157-4f7d-8256-0b61aa0fb167\tDuration: 17.99 ms\tBilled Duration: 100 ms\tMemory Size: 1024 MB\tMax Memory Used: 78 MB\tInit Duration: 185.18 ms\t\n
      """

      assert {
               :ok,
               %{
                 "report" => %{
                   "billed_duration_ms" => 100,
                   "duration_ms" => 18,
                   "init_duration_ms" => 185,
                   "max_memory_used_mb" => 78,
                   "memory_size_mb" => 1024
                 },
                 "request_id" => "026080a5-4157-4f7d-8256-0b61aa0fb167"
               }
             } == parse(message)
    end

    test "example message 2" do
      message = """
      START RequestId: 4d0ff57e-4022-4bfd-8689-a69e39f80f69 Version: $LATEST
      2020-02-19T17:32:52.353Z\t4d0ff57e-4022-4bfd-8689-a69e39f80f69\tINFO\tGetting metadata
      2020-02-19T17:32:52.364Z\t4d0ff57e-4022-4bfd-8689-a69e39f80f69\tINFO\tGetting projects
      2020-02-19T17:32:52.401Z\t4d0ff57e-4022-4bfd-8689-a69e39f80f69\tINFO\tGetting Logflare sources
      Oh see, it handles more than one line per message
      END RequestId: 4d0ff57e-4022-4bfd-8689-a69e39f80f69
      REPORT RequestId: 4d0ff57e-4022-4bfd-8689-a69e39f80f69\tDuration: 174.83 ms\tBilled Duration: 200 ms\tMemory Size: 1024 MB\tMax Memory Used: 84 MB\t\n
      """

      assert {
               :ok,
               %{
                 "lines" => [
                   %{
                     "level" => "info",
                     "message" => "Getting metadata",
                     "timestamp" => "2020-02-19T17:32:52.353Z"
                   },
                   %{
                     "level" => "info",
                     "message" => "Getting projects",
                     "timestamp" => "2020-02-19T17:32:52.364Z"
                   },
                   %{
                     "level" => "info",
                     "message" =>
                       "Getting Logflare sources\nOh see, it handles more than one line per message",
                     "timestamp" => "2020-02-19T17:32:52.401Z"
                   }
                 ],
                 "report" => %{
                   "billed_duration_ms" => 200,
                   "duration_ms" => 175,
                   "max_memory_used_mb" => 84,
                   "memory_size_mb" => 1024
                 },
                 "request_id" => "4d0ff57e-4022-4bfd-8689-a69e39f80f69",
                 "parse_status" => "full"
               }
             } == parse(message)
    end

    test "example message 3" do
      message = """
      START RequestId: bd8b7963-66f1-40b9-adfd-15e761cd39e8 Version: $LATEST
      END RequestId: bd8b7963-66f1-40b9-adfd-15e761cd39e8
      REPORT RequestId: bd8b7963-66f1-40b9-adfd-15e761cd39e8\tDuration: 22.48 ms\tBilled Duration: 100 ms\tMemory Size: 1024 MB\tMax Memory Used: 85 MB\t\n
      """

      assert {
               :ok,
               %{
                 "report" => %{
                   "billed_duration_ms" => 100,
                   "duration_ms" => 22,
                   "max_memory_used_mb" => 85,
                   "memory_size_mb" => 1024
                 },
                 "request_id" => "bd8b7963-66f1-40b9-adfd-15e761cd39e8"
               }
             } == parse(message)
    end

    test "example message 4" do
      message = """
      START RequestId: cb510178-1382-47e8-9865-1fb954a41325 Version: $LATEST
      2020-02-22T03:40:36.354Z\tcb510178-1382-47e8-9865-1fb954a41325\tINFO\tGetting drains
      2020-02-22T03:40:36.381Z\tcb510178-1382-47e8-9865-1fb954a41325\tINFO\tLogging map
      2020-02-22T03:40:36.381Z\tcb510178-1382-47e8-9865-1fb954a41325\tINFO\tMap {\n  'a string' => \"value associated with 'a string'\",\n  {} => 'value associated with keyObj',\n  [Function: keyFunc] => 'value associated with keyFunc'\n}
      2020-02-22T03:40:36.381Z\tcb510178-1382-47e8-9865-1fb954a41325\tINFO\tGetting metadata
      END RequestId: cb510178-1382-47e8-9865-1fb954a41325
      REPORT RequestId: cb510178-1382-47e8-9865-1fb954a41325\tDuration: 293.60 ms\tBilled Duration: 300 ms\tMemory Size: 1024 MB\tMax Memory Used: 84 MB\t\n
      """

      assert {
               :ok,
               %{
                 "lines" => [
                   %{
                     "level" => "info",
                     "message" => "Getting drains",
                     "timestamp" => "2020-02-22T03:40:36.354Z"
                   },
                   %{
                     "level" => "info",
                     "message" => "Logging map",
                     "timestamp" => "2020-02-22T03:40:36.381Z"
                   },
                   %{
                     "level" => "info",
                     "message" =>
                       "Map {\n  'a string' => \"value associated with 'a string'\",\n  {} => 'value associated with keyObj',\n  [Function: keyFunc] => 'value associated with keyFunc'\n}",
                     "timestamp" => "2020-02-22T03:40:36.381Z"
                   },
                   %{
                     "level" => "info",
                     "message" => "Getting metadata",
                     "timestamp" => "2020-02-22T03:40:36.381Z"
                   }
                 ],
                 "report" => %{
                   "billed_duration_ms" => 300,
                   "duration_ms" => 294,
                   "max_memory_used_mb" => 84,
                   "memory_size_mb" => 1024
                 },
                 "request_id" => "cb510178-1382-47e8-9865-1fb954a41325",
                 "parse_status" => "full"
               }
             } == parse(message)
    end

    test "example message 5: JSON body" do
      message = """
      START RequestId: 50b2f64b-0ce9-442c-a2e4-6e729b2efba0 Version: $LATEST
      {\"level\":30,\"time\":1582754345208,\"pid\":7,\"hostname\":\"169.254.202.181\",\"eventMessage\":\"Getting user with ID 5af38f7a6ca2c9012231de7c\",\"eventType\":\"get_user\",\"httpRequest\":{\"path\":\"/users/5af38f7a6ca2c9012231de7c?userId=5af38f7a6ca2c9012231de7c\",\"method\":\"GET\",\"query\":{\"userId\":\"5af38f7a6ca2c9012231de7c\"}},\"httpResponse\":{\"statusCode\":200},\"user\":{\"email\":\"example2@example.com\",\"dealer\":{\"id\":13201,\"location\":2,\"name\":\"Wagon Trail RV\"}},\"v\":1}
      END RequestId: 50b2f64b-0ce9-442c-a2e4-6e729b2efba0
      REPORT RequestId: 50b2f64b-0ce9-442c-a2e4-6e729b2efba0\tDuration: 126.08 ms\tBilled Duration: 200 ms\tMemory Size: 3008 MB\tMax Memory Used: 125 MB\t\n
      """

      assert {
               :ok,
               %{
                 "report" => %{
                   "billed_duration_ms" => 200,
                   "duration_ms" => 126,
                   "max_memory_used_mb" => 125,
                   "memory_size_mb" => 3008
                 },
                 "request_id" => "50b2f64b-0ce9-442c-a2e4-6e729b2efba0",
                 "lines" => [
                   %{
                     "data" => %{
                       "eventMessage" => "Getting user with ID 5af38f7a6ca2c9012231de7c",
                       "eventType" => "get_user",
                       "hostname" => "169.254.202.181",
                       "httpRequest" => %{
                         "method" => "GET",
                         "path" =>
                           "/users/5af38f7a6ca2c9012231de7c?userId=5af38f7a6ca2c9012231de7c",
                         "query" => %{"userId" => "5af38f7a6ca2c9012231de7c"}
                       },
                       "httpResponse" => %{"statusCode" => 200},
                       "level" => 30,
                       "pid" => 7,
                       "time" => 1_582_754_345_208,
                       "user" => %{
                         "dealer" => %{
                           "id" => 13_201,
                           "location" => 2,
                           "name" => "Wagon Trail RV"
                         },
                         "email" => "example2@example.com"
                       },
                       "v" => 1
                     }
                   }
                 ],
                 "parse_status" => "full"
               }
             } == parse(message)
    end

    test "example message 5.1: JSON body with newlines" do
      message = """
      START RequestId: 50b2f64b-0ce9-442c-a2e4-6e729b2efba0 Version: $LATEST
      {\"level\":30,\"time\":1582754345208,\"pid\":7,
       \"hostname\":\"169.254.202.181\",\"eventMessage\":
       \"Getting user with ID 5af38f7a6ca2c9012231de7c\",\"eventType\":\"get_user\",\"httpRequest\":
       {\"path\":\"/users/5af38f7a6ca2c9012231de7c?userId=5af38f7a6ca2c9012231de7c\",\"method\":\"GET\",\"query\":{\"userId\":\"5af38f7a6ca2c9012231de7c\"}},\"httpResponse\":{\"statusCode\":200},\"user\":{\"email\":\"example2@example.com\",\"dealer\":{\"id\":13201,\"location\":2,\"name\":\"Wagon Trail RV\"}},\"v\":1}
      END RequestId: 50b2f64b-0ce9-442c-a2e4-6e729b2efba0
      REPORT RequestId: 50b2f64b-0ce9-442c-a2e4-6e729b2efba0\tDuration: 126.08 ms\tBilled Duration: 200 ms\tMemory Size: 3008 MB\tMax Memory Used: 125 MB\t\n
      """

      assert {
               :ok,
               %{
                 "report" => %{
                   "billed_duration_ms" => 200,
                   "duration_ms" => 126,
                   "max_memory_used_mb" => 125,
                   "memory_size_mb" => 3008
                 },
                 "request_id" => "50b2f64b-0ce9-442c-a2e4-6e729b2efba0",
                 "lines" => [
                   %{
                     "data" => %{
                       "eventMessage" => "Getting user with ID 5af38f7a6ca2c9012231de7c",
                       "eventType" => "get_user",
                       "hostname" => "169.254.202.181",
                       "httpRequest" => %{
                         "method" => "GET",
                         "path" =>
                           "/users/5af38f7a6ca2c9012231de7c?userId=5af38f7a6ca2c9012231de7c",
                         "query" => %{"userId" => "5af38f7a6ca2c9012231de7c"}
                       },
                       "httpResponse" => %{"statusCode" => 200},
                       "level" => 30,
                       "pid" => 7,
                       "time" => 1_582_754_345_208,
                       "user" => %{
                         "dealer" => %{"id" => 13201, "location" => 2, "name" => "Wagon Trail RV"},
                         "email" => "example2@example.com"
                       },
                       "v" => 1
                     }
                   }
                 ],
                 "parse_status" => "full"
               }
             } == parse(message)
    end

    test "example message 5.2: multiple JSON lines in message body" do
      message = """
      START RequestId: 50b2f64b-0ce9-442c-a2e4-6e729b2efba0 Version: $LATEST
      {\"level\":30,\"time\":1582754345208,\"pid\":7,\"hostname\":\"169.254.202.181\",\"eventMessage\":\"Getting user with ID 5af38f7a6ca2c9012231de7c\",\"eventType\":\"get_user\",\"httpRequest\":{\"path\":\"/users/5af38f7a6ca2c9012231de7c?userId=5af38f7a6ca2c9012231de7c\",\"method\":\"GET\",\"query\":{\"userId\":\"5af38f7a6ca2c9012231de7c\"}},\"httpResponse\":{\"statusCode\":200},\"user\":{\"email\":\"example2@example.com\",\"dealer\":{\"id\":13201,\"location\":2,\"name\":\"Wagon Trail RV\"}},\"v\":1}
      {\"level\":30,\"time\":1582754345208,\"pid\":7,\"hostname\":\"169.254.202.181\",\"eventMessage\":\"Getting user with ID 5af38f7a6ca2c9012231de7c\",\"eventType\":\"get_user\",\"httpRequest\":{\"path\":\"/users/5af38f7a6ca2c9012231de7c?userId=5af38f7a6ca2c9012231de7c\",\"method\":\"GET\",\"query\":{\"userId\":\"5af38f7a6ca2c9012231de7c\"}},\"httpResponse\":{\"statusCode\":200},\"user\":{\"email\":\"example2@example.com\",\"dealer\":{\"id\":13201,\"location\":2,\"name\":\"Wagon Trail RV\"}},\"v\":1}
      END RequestId: 50b2f64b-0ce9-442c-a2e4-6e729b2efba0
      REPORT RequestId: 50b2f64b-0ce9-442c-a2e4-6e729b2efba0\tDuration: 126.08 ms\tBilled Duration: 200 ms\tMemory Size: 3008 MB\tMax Memory Used: 125 MB\t\n
      """

      assert {
               :ok,
               %{
                 "report" => %{
                   "billed_duration_ms" => 200,
                   "duration_ms" => 126,
                   "max_memory_used_mb" => 125,
                   "memory_size_mb" => 3008
                 },
                 "request_id" => "50b2f64b-0ce9-442c-a2e4-6e729b2efba0",
                 "parse_status" => "partial",
                 "lines_string" =>
                   "{\"level\":30,\"time\":1582754345208,\"pid\":7,\"hostname\":\"169.254.202.181\",\"eventMessage\":\"Getting user with ID 5af38f7a6ca2c9012231de7c\",\"eventType\":\"get_user\",\"httpRequest\":{\"path\":\"/users/5af38f7a6ca2c9012231de7c?userId=5af38f7a6ca2c9012231de7c\",\"method\":\"GET\",\"query\":{\"userId\":\"5af38f7a6ca2c9012231de7c\"}},\"httpResponse\":{\"statusCode\":200},\"user\":{\"email\":\"example2@example.com\",\"dealer\":{\"id\":13201,\"location\":2,\"name\":\"Wagon Trail RV\"}},\"v\":1}\n{\"level\":30,\"time\":1582754345208,\"pid\":7,\"hostname\":\"169.254.202.181\",\"eventMessage\":\"Getting user with ID 5af38f7a6ca2c9012231de7c\",\"eventType\":\"get_user\",\"httpRequest\":{\"path\":\"/users/5af38f7a6ca2c9012231de7c?userId=5af38f7a6ca2c9012231de7c\",\"method\":\"GET\",\"query\":{\"userId\":\"5af38f7a6ca2c9012231de7c\"}},\"httpResponse\":{\"statusCode\":200},\"user\":{\"email\":\"example2@example.com\",\"dealer\":{\"id\":13201,\"location\":2,\"name\":\"Wagon Trail RV\"}},\"v\":1}"
               }
             } == parse(message)
    end

    test "example message 6: JSON" do
      message = """
      START RequestId: 73c3160a-bc03-440c-880d-10716bf0f54d Version: $LATEST
      [random] (log) message with some {"json": "values"}
      END RequestId: 73c3160a-bc03-440c-880d-10716bf0f54d
      REPORT RequestId: 73c3160a-bc03-440c-880d-10716bf0f54d\tDuration: 398.56 ms\tBilled Duration: 400 ms\tMemory Size: 3008 MB\tMax Memory Used: 107 MB\t\n
      """

      assert {
               :ok,
               %{
                 "lines" => [
                   %{
                     "data" => %{"json" => "values"},
                     "message" => "[random] (log) message with some "
                   }
                 ],
                 "report" => %{
                   "billed_duration_ms" => 400,
                   "duration_ms" => 399,
                   "max_memory_used_mb" => 107,
                   "memory_size_mb" => 3008
                 },
                 "request_id" => "73c3160a-bc03-440c-880d-10716bf0f54d",
                 "parse_status" => "full"
               }
             } == parse(message)
    end

    test "example message 7" do
      message = """
      START RequestId: 62579afd-84ac-42ac-b09c-a0d44e823374 Version: $LATEST
      2020-02-27T15:58:34.784Z\t62579afd-84ac-42ac-b09c-a0d44e823374\tINFO\t
      2020-02-27T15:58:34.784Z\t62579afd-84ac-42ac-b09c-a0d44e823374\tINFO\tStarted GET \"/_next/data/HMg_oO9QEDGRPz5GoHLkF/posts/388.json\" for 127.0.0.1 at Thu Feb 27 2020 15:58:34 GMT+0000 (Coordinated Universal Time)
      2020-02-27T15:58:34.784Z\t62579afd-84ac-42ac-b09c-a0d44e823374\tINFO\t  params: {\"id\":388,\"query\":{}}
      2020-02-27T15:58:34.784Z\t62579afd-84ac-42ac-b09c-a0d44e823374\tINFO\t  data: {}
      2020-02-27T15:58:34.784Z\t62579afd-84ac-42ac-b09c-a0d44e823374\tINFO\t  Processing by PostsController.show
      2020-02-27T15:58:34.787Z\t62579afd-84ac-42ac-b09c-a0d44e823374\tERROR\tprisma:query SELECT 1
      2020-02-27T15:58:34.791Z\t62579afd-84ac-42ac-b09c-a0d44e823374\tERROR\tprisma:query SELECT \"public\".\"Post\".\"id\", \"public\".\"Post\".\"title\", \"public\".\"Post\".\"content\" FROM \"public\".\"Post\" WHERE \"public\".\"Post\".\"id\" IN ($1) OFFSET $2
      2020-02-27T15:58:34.795Z\t62579afd-84ac-42ac-b09c-a0d44e823374\tERROR\tprisma:query SELECT \"public\".\"Comment\".\"id\", \"public\".\"Comment\".\"content\", \"public\".\"Comment\".\"post\" FROM \"public\".\"Comment\" WHERE \"public\".\"Comment\".\"post\" IN ($1) OFFSET $2
      2020-02-27T15:58:34.795Z\t62579afd-84ac-42ac-b09c-a0d44e823374\tINFO\t  Returning to page after 11ms: {\"props\":{\"post\":{\"id\":388,\"title\":\"Tomato\",\"content\":\"Tomato\",\"comments\":[]}}}
      2020-02-27T15:58:34.796Z\t62579afd-84ac-42ac-b09c-a0d44e823374\tINFO\t
      END RequestId: 62579afd-84ac-42ac-b09c-a0d44e823374
      REPORT RequestId: 62579afd-84ac-42ac-b09c-a0d44e823374\tDuration: 14.94 ms\tBilled Duration: 100 ms\tMemory Size: 1024 MB\tMax Memory Used: 127 MB\t\n
      """

      assert {
               :ok,
               %{
                 "lines" => [
                   %{
                     "level" => "info",
                     "message" => "",
                     "timestamp" => "2020-02-27T15:58:34.784Z"
                   },
                   %{
                     "level" => "info",
                     "message" =>
                       "Started GET \"/_next/data/HMg_oO9QEDGRPz5GoHLkF/posts/388.json\" for 127.0.0.1 at Thu Feb 27 2020 15:58:34 GMT+0000 (Coordinated Universal Time)",
                     "timestamp" => "2020-02-27T15:58:34.784Z"
                   },
                   %{
                     "level" => "info",
                     "message" => "  params: ",
                     "timestamp" => "2020-02-27T15:58:34.784Z",
                     "data" => %{"id" => 388, "query" => %{}}
                   },
                   %{
                     "level" => "info",
                     "message" => "  data: ",
                     "timestamp" => "2020-02-27T15:58:34.784Z",
                     "data" => %{}
                   },
                   %{
                     "level" => "info",
                     "message" => "  Processing by PostsController.show",
                     "timestamp" => "2020-02-27T15:58:34.784Z"
                   },
                   %{
                     "level" => "error",
                     "message" => "prisma:query SELECT 1",
                     "timestamp" => "2020-02-27T15:58:34.787Z"
                   },
                   %{
                     "level" => "error",
                     "message" =>
                       "prisma:query SELECT \"public\".\"Post\".\"id\", \"public\".\"Post\".\"title\", \"public\".\"Post\".\"content\" FROM \"public\".\"Post\" WHERE \"public\".\"Post\".\"id\" IN ($1) OFFSET $2",
                     "timestamp" => "2020-02-27T15:58:34.791Z"
                   },
                   %{
                     "level" => "error",
                     "message" =>
                       "prisma:query SELECT \"public\".\"Comment\".\"id\", \"public\".\"Comment\".\"content\", \"public\".\"Comment\".\"post\" FROM \"public\".\"Comment\" WHERE \"public\".\"Comment\".\"post\" IN ($1) OFFSET $2",
                     "timestamp" => "2020-02-27T15:58:34.795Z"
                   },
                   %{
                     "level" => "info",
                     "message" => "  Returning to page after 11ms: ",
                     "timestamp" => "2020-02-27T15:58:34.795Z",
                     "data" => %{
                       "props" => %{
                         "post" => %{
                           "comments" => [],
                           "content" => "Tomato",
                           "id" => 388,
                           "title" => "Tomato"
                         }
                       }
                     }
                   },
                   %{
                     "level" => "info",
                     "message" => "",
                     "timestamp" => "2020-02-27T15:58:34.796Z"
                   }
                 ],
                 "report" => %{
                   "billed_duration_ms" => 100,
                   "duration_ms" => 15,
                   "max_memory_used_mb" => 127,
                   "memory_size_mb" => 1024
                 },
                 "request_id" => "62579afd-84ac-42ac-b09c-a0d44e823374",
                 "parse_status" => "full"
               }
             } == parse(message)
    end

    test "example message 8: JSON" do
      message = """
      START RequestId: 30167b78-67c9-4d00-8b88-62d11b157d3c Version: $LATEST
      2020-02-27T15:13:40.133Z\t30167b78-67c9-4d00-8b88-62d11b157d3c\tERROR\tUnhandled Promise Rejection \t{\"errorType\":\"Runtime.UnhandledPromiseRejection\",\"errorMessage\":\"SyntaxError: Unexpected end of JSON input\",\"reason\":{\"errorType\":\"SyntaxError\",\"errorMessage\":\"Unexpected end of JSON input\",\"stack\":[\"SyntaxError: Unexpected end of JSON input\",\"    at JSON.parse (<anonymous>)\",\"    at module.exports (/var/task/packages/graphql/src/api/session.js:13:24)\",\"    at processTicksAndRejections (internal/process/task_queues.js:94:5)\",\"    at async Server.<anonymous> (/var/task/___now_helpers.js:875:13)\"]},\"promise\":{},\"stack\":[\"Runtime.UnhandledPromiseRejection: SyntaxError: Unexpected end of JSON input\",\"    at process.<anonymous> (/var/runtime/index.js:35:15)\",\"    at process.emit (events.js:228:7)\",\"    at processPromiseRejections (internal/process/promises.js:201:33)\",\"    at processTicksAndRejections (internal/process/task_queues.js:95:32)\"]}
      END RequestId: 30167b78-67c9-4d00-8b88-62d11b157d3c
      REPORT RequestId: 30167b78-67c9-4d00-8b88-62d11b157d3c\tDuration: 48.35 ms\tBilled Duration: 100 ms\tMemory Size: 3008 MB\tMax Memory Used: 53 MB\t\nUnknown application error occurred\n\n
      """

      assert {
               :ok,
               %{
                 "report" => %{
                   "billed_duration_ms" => 100,
                   "duration_ms" => 48,
                   "max_memory_used_mb" => 53,
                   "memory_size_mb" => 3008
                 },
                 "request_id" => "30167b78-67c9-4d00-8b88-62d11b157d3c",
                 "lines" => [
                   %{
                     "level" => "error",
                     "message" => "Unhandled Promise Rejection \t",
                     "data" => %{
                       "errorMessage" => "SyntaxError: Unexpected end of JSON input",
                       "errorType" => "Runtime.UnhandledPromiseRejection",
                       "promise" => %{},
                       "reason" => %{
                         "errorMessage" => "Unexpected end of JSON input",
                         "errorType" => "SyntaxError",
                         "stack" => [
                           "SyntaxError: Unexpected end of JSON input",
                           "    at JSON.parse (<anonymous>)",
                           "    at module.exports (/var/task/packages/graphql/src/api/session.js:13:24)",
                           "    at processTicksAndRejections (internal/process/task_queues.js:94:5)",
                           "    at async Server.<anonymous> (/var/task/___now_helpers.js:875:13)"
                         ]
                       },
                       "stack" => [
                         "Runtime.UnhandledPromiseRejection: SyntaxError: Unexpected end of JSON input",
                         "    at process.<anonymous> (/var/runtime/index.js:35:15)",
                         "    at process.emit (events.js:228:7)",
                         "    at processPromiseRejections (internal/process/promises.js:201:33)",
                         "    at processTicksAndRejections (internal/process/task_queues.js:95:32)"
                       ]
                     },
                     "timestamp" => "2020-02-27T15:13:40.133Z"
                   }
                 ],
                 "parse_status" => "full"
               }
             } == parse(message)
    end

    test "example message 9: JSON" do
      message = """
      START RequestId: 73c3160a-bc03-440c-880d-10716bf0f54d Version: $LATEST
      [1582816182170] \u001B[32mINFO \u001B[39m (8 on 169.254.141.157): \u001B[36mLogging in user with username example11111@example.com\u001B[39m context: {\n      \"user\": {}\n    }\n    eventType: \"login\"\n    httpRequest: {\n      \"path\": \"/auth/login\",\n      \"method\": \"POST\"\n    }\n    httpResponse: {\n      \"statusCode\": 200\n    }\n    user: {\n      \"email\": \"example17444@example.com\"\n    }
      END RequestId: 73c3160a-bc03-440c-880d-10716bf0f54d
      REPORT RequestId: 73c3160a-bc03-440c-880d-10716bf0f54d\tDuration: 398.56 ms\tBilled Duration: 400 ms\tMemory Size: 3008 MB\tMax Memory Used: 107 MB\t\n
      """

      assert {
               :ok,
               %{
                 "report" => %{
                   "billed_duration_ms" => 400,
                   "duration_ms" => 399,
                   "max_memory_used_mb" => 107,
                   "memory_size_mb" => 3008
                 },
                 "request_id" => "73c3160a-bc03-440c-880d-10716bf0f54d",
                 "parse_status" => "partial",
                 "lines_string" =>
                   "[1582816182170] \e[32mINFO \e[39m (8 on 169.254.141.157): \e[36mLogging in user with username example11111@example.com\e[39m context: {\n      \"user\": {}\n    }\n    eventType: \"login\"\n    httpRequest: {\n      \"path\": \"/auth/login\",\n      \"method\": \"POST\"\n    }\n    httpResponse: {\n      \"statusCode\": 200\n    }\n    user: {\n      \"email\": \"example17444@example.com\"\n    }"
               }
             } == parse(message)
    end

    test "example message 10: JSON" do
      string = """
      START RequestId: 0304cfe3-629e-4d52-b884-1afa78c631a3 Version: $LATEST
      2020-02-28T17:37:13.976Z\t0304cfe3-629e-4d52-b884-1afa78c631a3\tINFO\t
      2020-02-28T17:37:13.977Z\t0304cfe3-629e-4d52-b884-1afa78c631a3\tINFO\tStarted GET \"/_next/data/HMg_oO9oHLkF/posts/434.json\" for 127.0.0.1 at Fri Feb 28 2020 17:37:13 GMT+0000 (Coordinated Universal Time)
      2020-02-28T17:37:13.978Z\t0304cfe3-629e-4d52-b884-1afa78c631a3\tINFO\t  params: {\"id\":434,\"query\":{}}
      2020-02-28T17:37:13.978Z\t0304cfe3-629e-4d52-b884-1afa78c631a3\tINFO\t  data: {}
      2020-02-28T17:37:13.978Z\t0304cfe3-629e-4d52-b884-1afa78c631a3\tINFO\t  Processing by PostsController.show
      2020-02-28T17:37:14.156Z\t0304cfe3-629e-4d52-b884-1afa78c631a3\tERROR\tprisma:info  Starting a postgresql pool with 1 connections.
      2020-02-28T17:37:14.183Z\t0304cfe3-629e-4d52-b884-1afa78c631a3\tERROR\tprisma:info  Started http server on 127.0.0.1:43257
      2020-02-28T17:37:14.281Z\t0304cfe3-629e-4d52-b884-1afa78c631a3\tERROR\tprisma:query SELECT \"public\".\"Post\".\"id\", \"public\".\"Post\".\"title\", \"public\".\"Post\".\"content\" FROM \"public\".\"Post\" WHERE \"public\".\"Post\".\"id\" IN ($1) OFFSET $2
      2020-02-28T17:37:14.285Z\t0304cfe3-629e-4d52-b884-1afa78c631a3\tERROR\tprisma:query SELECT \"public\".\"Comment\".\"id\", \"public\".\"Comment\".\"content\", \"public\".\"Comment\".\"post\" FROM \"public\".\"Comment\" WHERE \"public\".\"Comment\".\"post\" IN ($1) OFFSET $2
      2020-02-28T17:37:14.288Z\t0304cfe3-629e-4d52-b884-1afa78c631a3\tINFO\t  Returning to page after 312ms: {\"props\":{\"post\":{\"id\":434,\"title\":\"Fruit\",\"content\":\"Fruit\",\"comments\":[{\"id\":419,\"content\":\"Orange\"}]}}}
      2020-02-28T17:37:14.288Z\t0304cfe3-629e-4d52-b884-1afa78c631a3\tINFO\t
      END RequestId: 0304cfe3-629e-4d52-b884-1afa78c631a3
      REPORT RequestId: 0304cfe3-629e-4d52-b884-1afa78c631a3\tDuration: 337.43 ms\tBilled Duration: 400 ms\tMemory Size: 1024 MB\tMax Memory Used: 119 MB\tInit Duration: 262.92 ms\t\n
      """

      assert {
               :ok,
               %{
                 "lines" => [
                   %{
                     "level" => "info",
                     "message" => "",
                     "timestamp" => "2020-02-28T17:37:13.976Z"
                   },
                   %{
                     "level" => "info",
                     "message" =>
                       "Started GET \"/_next/data/HMg_oO9oHLkF/posts/434.json\" for 127.0.0.1 at Fri Feb 28 2020 17:37:13 GMT+0000 (Coordinated Universal Time)",
                     "timestamp" => "2020-02-28T17:37:13.977Z"
                   },
                   %{
                     "data" => %{"id" => 434, "query" => %{}},
                     "level" => "info",
                     "message" => "  params: ",
                     "timestamp" => "2020-02-28T17:37:13.978Z"
                   },
                   %{
                     "data" => %{},
                     "level" => "info",
                     "message" => "  data: ",
                     "timestamp" => "2020-02-28T17:37:13.978Z"
                   },
                   %{
                     "level" => "info",
                     "message" => "  Processing by PostsController.show",
                     "timestamp" => "2020-02-28T17:37:13.978Z"
                   },
                   %{
                     "level" => "error",
                     "message" => "prisma:info  Starting a postgresql pool with 1 connections.",
                     "timestamp" => "2020-02-28T17:37:14.156Z"
                   },
                   %{
                     "level" => "error",
                     "message" => "prisma:info  Started http server on 127.0.0.1:43257",
                     "timestamp" => "2020-02-28T17:37:14.183Z"
                   },
                   %{
                     "level" => "error",
                     "message" =>
                       "prisma:query SELECT \"public\".\"Post\".\"id\", \"public\".\"Post\".\"title\", \"public\".\"Post\".\"content\" FROM \"public\".\"Post\" WHERE \"public\".\"Post\".\"id\" IN ($1) OFFSET $2",
                     "timestamp" => "2020-02-28T17:37:14.281Z"
                   },
                   %{
                     "level" => "error",
                     "message" =>
                       "prisma:query SELECT \"public\".\"Comment\".\"id\", \"public\".\"Comment\".\"content\", \"public\".\"Comment\".\"post\" FROM \"public\".\"Comment\" WHERE \"public\".\"Comment\".\"post\" IN ($1) OFFSET $2",
                     "timestamp" => "2020-02-28T17:37:14.285Z"
                   },
                   %{
                     "data" => %{
                       "props" => %{
                         "post" => %{
                           "comments" => [%{"content" => "Orange", "id" => 419}],
                           "content" => "Fruit",
                           "id" => 434,
                           "title" => "Fruit"
                         }
                       }
                     },
                     "level" => "info",
                     "message" => "  Returning to page after 312ms: ",
                     "timestamp" => "2020-02-28T17:37:14.288Z"
                   },
                   %{
                     "level" => "info",
                     "message" => "",
                     "timestamp" => "2020-02-28T17:37:14.288Z"
                   }
                 ],
                 "report" => %{
                   "billed_duration_ms" => 400,
                   "duration_ms" => 337,
                   "init_duration_ms" => 263,
                   "max_memory_used_mb" => 119,
                   "memory_size_mb" => 1024
                 },
                 "request_id" => "0304cfe3-629e-4d52-b884-1afa78c631a3",
                 "parse_status" => "full"
               }
             } == parse(string)
    end

    test "example 11" do
      string =
        "START RequestId: 159d5e82-801c-4a61-8d6a-b4f30c68ffca Version: $LATEST\n2020-02-27T18:49:56.033Z\t159d5e82-801c-4a61-8d6a-b4f30c68ffca\tINFO\t🐘 Spawning: PHP Built-In Server at /var/task/user (document root) and /var/task/user/search/stat.php (router)\n2020-02-27T18:49:56.137Z\t159d5e82-801c-4a61-8d6a-b4f30c68ffca\tERROR\t🐘STDERR: [Thu Feb 27 18:49:56 2020] PHP 7.4.2RC1 Development Server (http://127.0.0.1:8000) started\n\n2020-02-27T18:49:56.150Z\t159d5e82-801c-4a61-8d6a-b4f30c68ffca\tINFO\t🐘 Accessing hotels.tripfinder.cc/search/stat.php?type=countries\n2020-02-27T18:49:56.150Z\t159d5e82-801c-4a61-8d6a-b4f30c68ffca\tINFO\t🐘 Querying /search/stat.php?type=countries\n2020-02-27T18:49:56.155Z\t159d5e82-801c-4a61-8d6a-b4f30c68ffca\tERROR\t🐘STDERR: [Thu Feb 27 18:49:56 2020] 127.0.0.1:37352 Accepted\n[Thu Feb 27 18:49:56 2020] 127.0.0.1:37352 Closed without sending a request; it was probably just an unused speculative preconnection\n[Thu Feb 27 18:49:56 2020] 127.0.0.1:37352 Closing\n[Thu Feb 27 18:49:56 2020] 127.0.0.1:37354 Accepted\n\n2020-02-27T18:49:56.209Z\t159d5e82-801c-4a61-8d6a-b4f30c68ffca\tERROR\t🐘STDERR: [Thu Feb 27 18:49:56 2020] 127.0.0.1:37354 Closing\n\nEND RequestId: 159d5e82-801c-4a61-8d6a-b4f30c68ffca\nREPORT RequestId: 159d5e82-801c-4a61-8d6a-b4f30c68ffca\tDuration: 181.16 ms\tBilled Duration: 200 ms\tMemory Size: 1024 MB\tMax Memory Used: 94 MB\tInit Duration: 152.32 ms\t\n"

      assert {
               :ok,
               %{
                 "lines" => [
                   %{
                     "level" => "info",
                     "message" =>
                       "🐘 Spawning: PHP Built-In Server at /var/task/user (document root) and /var/task/user/search/stat.php (router)",
                     "timestamp" => "2020-02-27T18:49:56.033Z"
                   },
                   %{
                     "level" => "error",
                     "message" =>
                       "🐘STDERR: [Thu Feb 27 18:49:56 2020] PHP 7.4.2RC1 Development Server (http://127.0.0.1:8000) started",
                     "timestamp" => "2020-02-27T18:49:56.137Z"
                   },
                   %{
                     "level" => "info",
                     "message" =>
                       "🐘 Accessing hotels.tripfinder.cc/search/stat.php?type=countries",
                     "timestamp" => "2020-02-27T18:49:56.150Z"
                   },
                   %{
                     "level" => "info",
                     "message" => "🐘 Querying /search/stat.php?type=countries",
                     "timestamp" => "2020-02-27T18:49:56.150Z"
                   },
                   %{
                     "level" => "error",
                     "message" =>
                       "🐘STDERR: [Thu Feb 27 18:49:56 2020] 127.0.0.1:37352 Accepted\n[Thu Feb 27 18:49:56 2020] 127.0.0.1:37352 Closed without sending a request; it was probably just an unused speculative preconnection\n[Thu Feb 27 18:49:56 2020] 127.0.0.1:37352 Closing\n[Thu Feb 27 18:49:56 2020] 127.0.0.1:37354 Accepted",
                     "timestamp" => "2020-02-27T18:49:56.155Z"
                   },
                   %{
                     "level" => "error",
                     "message" => "🐘STDERR: [Thu Feb 27 18:49:56 2020] 127.0.0.1:37354 Closing",
                     "timestamp" => "2020-02-27T18:49:56.209Z"
                   }
                 ],
                 "report" => %{
                   "billed_duration_ms" => 200,
                   "duration_ms" => 181,
                   "init_duration_ms" => 152,
                   "max_memory_used_mb" => 94,
                   "memory_size_mb" => 1024
                 },
                 "request_id" => "159d5e82-801c-4a61-8d6a-b4f30c68ffca",
                 "parse_status" => "full"
               }
             } == parse(string)
    end
  end
end
