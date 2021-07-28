defmodule Logflare.Logs.Zeit.NimbleLambdaMessageParserUnitTest do
  @moduledoc false
  use ExUnit.Case, async: true
  import Logflare.Logs.Zeit.NimbleLambdaMessageParser

  describe "message line" do
    @tag :run

    test "message_line" do
      message = """
      Getting metadata {"prop": {"prop2": {"prop3": [{"prop4": 4}]}}}
      """

      assert {
               :ok,
               [
                 {:message, "Getting metadata "},
                 {:maybe_json, "{\"prop\": {\"prop2\": {\"prop3\": [{\"prop4\": 4}]}}}"}
               ],
               "",
               %{},
               {2, 64},
               64
             } == message_line(message)

      message = """
      Getting metadata {{{ more test message
      """

      assert {
               :ok,
               [{:message, "Getting metadata "}, {:maybe_json, "{{{ more test message"}],
               "",
               %{},
               {2, 39},
               39
             } == message_line(message)
    end
  end

  describe "body" do
    test "json body" do
      string = """
      {\"level\":30,\"time\":1582754345208,\"pid\":7,\"hostname\":\"169.254.202.181\",\"eventMessage\":\"Getting user with ID 5af38f7a6ca2c9012231de7c\",\"eventType\":\"get_user\",\"httpRequest\":{\"path\":\"/users/5af38f7a6ca2c9012231de7c?userId=5af38f7a6ca2c9012231de7c\",\"method\":\"GET\",\"query\":{\"userId\":\"5af38f7a6ca2c9012231de7c\"}},\"httpResponse\":{\"statusCode\":200},\"user\":{\"email\":\"payables@example.com\",\"dealer\":{\"id\":13201,\"location\":2,\"name\":\"Wagon Trail RV\"}},\"v\":1}
      """

      assert {
               :ok,
               [
                 [
                   {"lines",
                    [
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
                              "id" => 13201,
                              "location" => 2,
                              "name" => "Wagon Trail RV"
                            },
                            "email" => "payables@example.com"
                          },
                          "v" => 1
                        }
                      }
                    ]},
                   {"parse_status", "full"}
                 ]
               ],
               "",
               %{},
               {2, 446},
               446
             } == body(string)
    end
  end

  describe "message" do
    test "message line with json" do
      message = """
      Getting metadata {"prop": {"prop2": {"prop3": [{"prop4": 4}]}}}
      """

      assert {
               :ok,
               [
                 %{
                   "data" => %{"prop" => %{"prop2" => %{"prop3" => [%{"prop4" => 4}]}}},
                   "message" => "Getting metadata "
                 }
               ],
               "",
               %{},
               {2, 64},
               64
             } == message(message)
    end
  end

  describe "log_line" do
    test "with empty message" do
      string = "2020-02-27T15:58:34.784Z\t62579afd-84ac-42ac-b09c-a0d44e823374\tINFO\t\n"

      assert {
               :ok,
               [
                 %{
                   "level" => "info",
                   "message_and_data" => %{"message" => ""},
                   "timestamp" => "2020-02-27T15:58:34.784Z"
                 }
               ],
               "",
               %{},
               {2, 68},
               68
             } == log_line(string)
    end

    test "with message string and json" do
      message = """
      2020-02-19T17:32:52.353Z\t4d0ff57e-4022-4bfd-8689-a69e39f80f69\tINFO\tGetting metadata {"prop": {"prop2": {"prop3": [{"prop4": 4}]}}}
      """

      assert {
               :ok,
               [
                 %{
                   "level" => "info",
                   "message_and_data" => %{
                     "data" => %{"prop" => %{"prop2" => %{"prop3" => [%{"prop4" => 4}]}}},
                     "message" => "Getting metadata "
                   },
                   "timestamp" => "2020-02-19T17:32:52.353Z"
                 }
               ],
               "",
               %{},
               {2, 131},
               131
             } == log_line(message)
    end
  end

  describe "log_lines" do
    @tag :run
    test "with first empty message" do
      string = """
      2020-02-27T15:58:34.784Z\t62579afd-84ac-42ac-b09c-a0d44e823374\tINFO\t
      2020-02-27T15:58:34.784Z\t62579afd-84ac-42ac-b09c-a0d44e823374\tINFO\tStarted GET \"/_next/data/HMg_oO9QEDGRPz5GoHLkF/posts/388.json\" for 127.0.0.1 at Thu Feb 27 2020 15:58:34 GMT+0000 (Coordinated Universal Time)
      """

      assert {
               :ok,
               [
                 {"lines",
                  [
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
                    }
                  ]}
               ],
               "",
               %{},
               {3, 278},
               278
             } == log_lines(string)
    end

    test "log_lines" do
      string = """
      2020-02-22T03:40:36.381Z\tcb510178-1382-47e8-9865-1fb954a41325\tINFO\tMap {\n  'a string' => \"value associated with 'a string'\",\n  {} => 'value associated with keyObj',\n  [Function: keyFunc] => 'value associated with keyFunc'\n}
      """

      assert {
               :ok,
               [
                 %{
                   "level" => "info",
                   "message_and_data" => %{
                     "message" =>
                       "Map {\n  'a string' => \"value associated with 'a string'\",\n  {} => 'value associated with keyObj',\n  [Function: keyFunc] => 'value associated with keyFunc'\n}"
                   },
                   "timestamp" => "2020-02-22T03:40:36.381Z"
                 }
               ],
               "",
               %{},
               {6, 224},
               224
             } == log_line(string)

      string = """
      2020-02-22T03:40:36.354Z\tcb510178-1382-47e8-9865-1fb954a41325\tINFO\tGetting drains
      2020-02-22T03:40:36.381Z\tcb510178-1382-47e8-9865-1fb954a41325\tINFO\tLogging map
      2020-02-22T03:40:36.381Z\tcb510178-1382-47e8-9865-1fb954a41325\tINFO\tMap {\n  'a string' => \"value associated with 'a string'\",\n  {} => 'value associated with keyObj',\n  [Function: keyFunc] => 'value associated with keyFunc'\n}
      2020-02-22T03:40:36.381Z\tcb510178-1382-47e8-9865-1fb954a41325\tINFO\tGetting metadata
      """

      assert {
               :ok,
               [
                 {
                   "lines",
                   [
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
                   ]
                 }
               ],
               "",
               %{},
               {9, 469},
               469
             } == log_lines(string)
    end
  end
end
