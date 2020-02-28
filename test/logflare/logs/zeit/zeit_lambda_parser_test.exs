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
                 "lines" => [],
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
                 "request_id" => "4d0ff57e-4022-4bfd-8689-a69e39f80f69"
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
                 "lines" => [],
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
                 "request_id" => "cb510178-1382-47e8-9865-1fb954a41325"
               }
             } == parse(message)
    end

    test "example message 5: JSON payload" do
      message = """
      START RequestId: 50b2f64b-0ce9-442c-a2e4-6e729b2efba0 Version: $LATEST
      {\"level\":30,\"time\":1582754345208,\"pid\":7,\"hostname\":\"169.254.202.181\",\"eventMessage\":\"Getting user with ID 5af38f7a6ca2c9012231de7c\",\"eventType\":\"get_user\",\"httpRequest\":{\"path\":\"/users/5af38f7a6ca2c9012231de7c?userId=5af38f7a6ca2c9012231de7c\",\"method\":\"GET\",\"query\":{\"userId\":\"5af38f7a6ca2c9012231de7c\"}},\"httpResponse\":{\"statusCode\":200},\"user\":{\"email\":\"payables@vancityrv.com\",\"dealer\":{\"id\":13201,\"location\":2,\"name\":\"Wagon Trail RV\"}},\"v\":1}
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
                 "data" => %{
                   "eventMessage" => "Getting user with ID 5af38f7a6ca2c9012231de7c",
                   "eventType" => "get_user",
                   "hostname" => "169.254.202.181",
                   "httpRequest" => %{
                     "method" => "GET",
                     "path" => "/users/5af38f7a6ca2c9012231de7c?userId=5af38f7a6ca2c9012231de7c",
                     "query" => %{"userId" => "5af38f7a6ca2c9012231de7c"}
                   },
                   "httpResponse" => %{"statusCode" => 200},
                   "level" => 30,
                   "pid" => 7,
                   "time" => 1_582_754_345_208,
                   "user" => %{
                     "dealer" => %{"id" => 13_201, "location" => 2, "name" => "Wagon Trail RV"},
                     "email" => "payables@vancityrv.com"
                   },
                   "v" => 1
                 }
               }
             } == parse(message)
    end
  end
end
