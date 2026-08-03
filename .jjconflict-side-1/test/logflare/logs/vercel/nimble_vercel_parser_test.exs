defmodule Logflare.Logs.Vercel.NimbleLambdaMessageParserTest do
  use Logflare.DataCase, async: true

  import Logflare.Logs.Vercel.NimbleLambdaMessageParser

  describe "Vercel Lambda Message Parser" do
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
                 "parse_status" => "full",
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
                   },
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
                 ]
               }
             } == parse(message)

      message = """
      START RequestId: 50b2f64b-0ce9-442c-a2e4-6e729b2efba0 Version: $LATEST
      {\"level\":30,\"time\":1582754345208,\"pid\":7,\"hostname\":\"169.254.202.181\",\"eventMessage\":\"Getting user with ID 5af38f7a6ca2c9012231de7c\",\"eventType\":\"get_user\",\"httpRequest\":{\"path\":\"/users/5af38f7a6ca2c9012231de7c?userId=5af38f7a6ca2c9012231de7c\",\"method\":\"GET\",\"query\":{\"userId\":\"5af38f7a6ca2c9012231de7c\"}},\"httpResponse\":{\"statusCode\":200},\"user\":{\"email\":\"example2@example.com\",\"dealer\":{\"id\":13201,\"location\":2,\"name\":\"Wagon Trail RV\"}},\"v\":1}
      {\"level\":30,\"time\":1582754345208,\"pid\":7,\"hostname\":\"169.254.202.181\",\"eventMessage\":\"Getting user with ID 5af38f7a6ca2c9012231de7c\",\"eventType\":\"get_user\",\"httpRequest\":{\"path\":\"/users/5af38f7a6ca2c9012231de7c?userId=5af38f7a6ca2c9012231de7c\",\"method\":\"GET\",\"query\":{\"userId\":\"5af38f7a6ca2c9012231de7c\"}},\"httpResponse\":{\"statusCode\":200},\"user\":{\"email\":\"example2@example.com\",\"dealer\":{\"id\":13201,\"location\":2,\"name\":\"Wagon Trail RV\"}},\"v\":1}
      {\"level\":30,\"time\":1582754345208,\"pid\":7,\"hostname\":\"169.254.202.181\",\"eventMessage\":\"Getting user with ID 5af38f7a6ca2c9012231de7c\",\"eventType\":\"get_user\",\"httpRequest\":{\"path\":\"/users/5af38f7a6ca2c9012231de7c?userId=5af38f7a6ca2c9012231de7c\",\"method\":\"GET\",\"query\":{\"userId\":\"5af38f7a6ca2c9012231de7c\"}},\"httpResponse\":{\"statusCode\":200},\"user\":{\"email\":\"example2@example.com\",\"dealer\":{\"id\":13201,\"location\":2,\"name\":\"Wagon Trail RV\"}},\"v\":1}
      {\"level\":30,\"time\":1582754345208,\"pid\":7,\"hostname\":\"169.254.202.181\",\"eventMessage\":\"Getting user with ID 5af38f7a6ca2c9012231de7c\",\"eventType\":\"get_user\",\"httpRequest\":{\"path\":\"/users/5af38f7a6ca2c9012231de7c?userId=5af38f7a6ca2c9012231de7c\",\"method\":\"GET\",\"query\":{\"userId\":\"5af38f7a6ca2c9012231de7c\"}},\"httpResponse\":{\"statusCode\":200},\"user\":{\"email\":\"example2@example.com\",\"dealer\":{\"id\":13201,\"location\":2,\"name\":\"Wagon Trail RV\"}},\"v\":1}
      {\"level\":30,\"time\":1582754345208,\"pid\":7,\"hostname\":\"169.254.202.181\",\"eventMessage\":\"Getting user with ID 5af38f7a6ca2c9012231de7c\",\"eventType\":\"get_user\",\"httpRequest\":{\"path\":\"/users/5af38f7a6ca2c9012231de7c?userId=5af38f7a6ca2c9012231de7c\",\"method\":\"GET\",\"query\":{\"userId\":\"5af38f7a6ca2c9012231de7c\"}},\"httpResponse\":{\"statusCode\":200},\"user\":{\"email\":\"example2@example.com\",\"dealer\":{\"id\":13201,\"location\":2,\"name\":\"Wagon Trail RV\"}},\"v\":1}
      {\"level\":30,\"time\":1582754345208,\"pid\":7,\"hostname\":\"169.254.202.181\",\"eventMessage\":\"Getting user with ID 5af38f7a6ca2c9012231de7c\",\"eventType\":\"get_user\",\"httpRequest\":{\"path\":\"/users/5af38f7a6ca2c9012231de7c?userId=5af38f7a6ca2c9012231de7c\",\"method\":\"GET\",\"query\":{\"userId\":\"5af38f7a6ca2c9012231de7c\"}},\"httpResponse\":{\"statusCode\":200},\"user\":{\"email\":\"example2@example.com\",\"dealer\":{\"id\":13201,\"location\":2,\"name\":\"Wagon Trail RV\"}},\"v\":1}
      END RequestId: 50b2f64b-0ce9-442c-a2e4-6e729b2efba0
      REPORT RequestId: 50b2f64b-0ce9-442c-a2e4-6e729b2efba0\tDuration: 126.08 ms\tBilled Duration: 200 ms\tMemory Size: 3008 MB\tMax Memory Used: 125 MB\t\n
      """

      assert {
               :ok,
               %{
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
                         "query" => %{
                           "userId" => "5af38f7a6ca2c9012231de7c"
                         }
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
                   },
                   %{
                     "data" => %{
                       "eventMessage" => "Getting user with ID 5af38f7a6ca2c9012231de7c",
                       "eventType" => "get_user",
                       "hostname" => "169.254.202.181",
                       "httpRequest" => %{
                         "method" => "GET",
                         "path" =>
                           "/users/5af38f7a6ca2c9012231de7c?userId=5af38f7a6ca2c9012231de7c",
                         "query" => %{
                           "userId" => "5af38f7a6ca2c9012231de7c"
                         }
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
                   },
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
                   },
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
                   },
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
                   },
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
                 "parse_status" => "full",
                 "report" => %{
                   "billed_duration_ms" => 200,
                   "duration_ms" => 126,
                   "max_memory_used_mb" => 125,
                   "memory_size_mb" => 3008
                 },
                 "request_id" => "50b2f64b-0ce9-442c-a2e4-6e729b2efba0"
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

    test "example message 6.1: message and JSON" do
      message =
        "START RequestId: 25db085e-093f-470c-b8dd-481038f213b6 Version: $LATEST\n2020-06-02T19:08:48.968Z\t25db085e-093f-470c-b8dd-481038f213b6\tINFO\tNORMAL_REQUEST\n2020-06-02T19:08:48.969Z\t25db085e-093f-470c-b8dd-481038f213b6\tINFO\tCHAMANDO_AIRTABLE{\"table\":\"Status dos Pedidos\",\"filter\":\"{Código Pedido} = \\\"20200602-6CZ8PM\\\"\"}\n2020-06-02T19:08:49.319Z\t25db085e-093f-470c-b8dd-481038f213b6\tINFO\tRESULTADOS_AIRTABLE{\"orders\":1}\n2020-06-02T19:08:49.319Z\t25db085e-093f-470c-b8dd-481038f213b6\tINFO\tCHAMANDO_MERCADOPAGO{\"external_reference\":\"20200602-6CZ8PM\"}\n2020-06-02T19:08:49.665Z\t25db085e-093f-470c-b8dd-481038f213b6\tINFO\tRESULTADOS_MERCADOPAGO{\"mercado_pago_payments_length\":0,\"mercado_pago_payments\":[]}\n2020-06-02T19:08:49.665Z\t25db085e-093f-470c-b8dd-481038f213b6\tINFO\tCHAMANDO_AIRTABLE{\"table\":\"Pagamentos\",\"filter\":\"{Código Pedido} = \\\"20200602-6CZ8PM\\\"\"}\n2020-06-02T19:08:49.949Z\t25db085e-093f-470c-b8dd-481038f213b6\tINFO\tRESULTADOS_AIRTABLE{\"payments_length\":0}\nEND RequestId: 25db085e-093f-470c-b8dd-481038f213b6\nREPORT RequestId: 25db085e-093f-470c-b8dd-481038f213b6\tDuration: 995.69 ms\tBilled Duration: 1000 ms\tMemory Size: 1024 MB\tMax Memory Used: 94 MB\tInit Duration: 486.74 ms\t\n"

      assert {
               :ok,
               %{
                 "lines" => [
                   %{
                     "level" => "info",
                     "message" => "NORMAL_REQUEST",
                     "timestamp" => "2020-06-02T19:08:48.968Z"
                   },
                   %{
                     "data" => %{
                       "filter" => "{Código Pedido} = \"20200602-6CZ8PM\"",
                       "table" => "Status dos Pedidos"
                     },
                     "level" => "info",
                     "message" => "CHAMANDO_AIRTABLE",
                     "timestamp" => "2020-06-02T19:08:48.969Z"
                   },
                   %{
                     "data" => %{"orders" => 1},
                     "level" => "info",
                     "message" => "RESULTADOS_AIRTABLE",
                     "timestamp" => "2020-06-02T19:08:49.319Z"
                   },
                   %{
                     "data" => %{"external_reference" => "20200602-6CZ8PM"},
                     "level" => "info",
                     "message" => "CHAMANDO_MERCADOPAGO",
                     "timestamp" => "2020-06-02T19:08:49.319Z"
                   },
                   %{
                     "data" => %{
                       "mercado_pago_payments" => [],
                       "mercado_pago_payments_length" => 0
                     },
                     "level" => "info",
                     "message" => "RESULTADOS_MERCADOPAGO",
                     "timestamp" => "2020-06-02T19:08:49.665Z"
                   },
                   %{
                     "data" => %{
                       "filter" => "{Código Pedido} = \"20200602-6CZ8PM\"",
                       "table" => "Pagamentos"
                     },
                     "level" => "info",
                     "message" => "CHAMANDO_AIRTABLE",
                     "timestamp" => "2020-06-02T19:08:49.665Z"
                   },
                   %{
                     "data" => %{"payments_length" => 0},
                     "level" => "info",
                     "message" => "RESULTADOS_AIRTABLE",
                     "timestamp" => "2020-06-02T19:08:49.949Z"
                   }
                 ],
                 "parse_status" => "full",
                 "report" => %{
                   "billed_duration_ms" => 1000,
                   "duration_ms" => 996,
                   "max_memory_used_mb" => 94,
                   "memory_size_mb" => 1024,
                   "init_duration_ms" => 487
                 },
                 "request_id" => "25db085e-093f-470c-b8dd-481038f213b6"
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

    test "example message 11: truncated logs" do
      string =
        "d Version: $LATEST\n{\"level\":20,\"time\":1590250719399,\"variables\":{\"id\":\"ck9rmapte00030vl8mfudcatq\"},\"msg\":\"[server/request DashboardSpace]\"}\n{\"level\":10,\"time\":1590250723600,\"response\":{\"http\":{\"headers\":{}},\"data\":{\"dashboard\":{\"space\":{\"id\":\"ck9rmapte00030vl8mfudcatq\",\"slug\":\"demo-space-1\",\"name\":\".Art\",\"extent\":5,\"description\":\"ART is the only domain zone created specifically for the global creative community. With .ART individual artists, galleries, museums, art projects, corporate collections and art media can register a clear and concise web address.\",\"color\":\"#ff8cda\",\"visited\":true,\"sectionId\":\"ck9rj94h000010vlbdyax3nc5\",\"section\":{\"id\":\"ck9rj94h000010vlbdyax3nc5\",\"name\":\"Sponsors\",\"color\":\"#be46ff\",\"__typename\":\"Section\"},\"banner\":{\"id\":\"ckaiv26x300464mqq521b7fbw\",\"type\":\"Image\",\"publicId\":\"uploads/i1yaxauyybrsvkya1qko\",\"uri\":null,\"__typename\":\"Asset\"},\"walls\":[{\"id\":\"ck9rmaptf00050vl8hbdlp7an\",\"x\":0,\"y\":1,\"__typename\":\"Wall\",\"visited\":false,\"work\":null},{\"id\":\"ck9rmaptf00060vl8cadl12dq\",\"x\":0,\"y\":2,\"__typename\":\"Wall\",\"visited\":false,\"work\":null},{\"id\":\"ck9rmaptf00070vl8gvmc8ba2\",\"x\":0,\"y\":3,\"__typename\":\"Wall\",\"visited\":false,\"work\":null},{\"id\":\"ck9rmaptf00080vl8c69e21jg\",\"x\":0,\"y\":4,\"__typename\":\"Wall\",\"visited\":false,\"work\":null},{\"id\":\"ck9rmaptf00090vl81zgv6wut\",\"x\":1,\"y\":0,\"__typename\":\"Wall\",\"visited\":false,\"work\":null},{\"id\":\"ck9rmaptf00100vl8uc5byfyz\",\"x\":1,\"y\":1,\"__typename\":\"Wall\",\"visited\":false,\"work\":null},{\"id\":\"ck9rmaptf00110vl8ic7n0idp\",\"x\":1,\"y\":2,\"__typename\":\"Wall\",\"visited\":false,\"work\":null},{\"id\":\"ck9rmaptf00120vl8rgx36wvu\",\"x\":1,\"y\":3,\"__typename\":\"Wall\",\"visited\":false,\"work\":null},{\"id\":\"ck9rmaptf00130vl8s43e5bvj\",\"x\":1,\"y\":4,\"__typename\":\"Wall\",\"visited\":false,\"work\":null},{\"id\":\"ck9rmaptf00140vl8s7hf5a58\",\"x\":2,\"y\":0,\"__typename\":\"Wall\",\"visited\":false,\"work\":null},{\"id\":\"ck9rmaptf00150vl8j50ichts\",\"x\":2,\"y\":1,\"__typename\":\"Wall\",\"visited\":false,\"work\":null},{\"id\":\"ck9rmaptf00160vl8ouhdj9b3\",\"x\":2,\"y\":2,\"__typename\":\"Wall\",\"visited\":true,\"work\":null},{\"id\":\"ck9rmaptf00170vl8p8uottnx\",\"x\":2,\"y\":3,\"__typename\":\"Wall\",\"visited\":false,\"work\":null},{\"id\":\"ck9rmaptf00180vl87k6npcdo\",\"x\":2,\"y\":4,\"__typename\":\"Wall\",\"visited\":false,\"work\":null},{\"id\":\"ck9rmaptf00190vl89gnngj0t\",\"x\":3,\"y\":0,\"__typename\":\"Wall\",\"visited\":true,\"work\":null},{\"id\":\"ck9rmaptf00200vl86mn9yqfa\",\"x\":3,\"y\":1,\"__typename\":\"Wall\",\"visited\":true,\"work\":null},{\"id\":\"ck9rmaptf00210vl8cw4c2sd6\",\"x\":3,\"y\":2,\"__typename\":\"Wall\",\"visited\":true,\"work\":null},{\"id\":\"ck9rmaptf00220vl8urekxxkw\",\"x\":3,\"y\":3,\"__typename\":\"Wall\",\"visited\":false,\"work\":null},{\"id\":\"ck9rmaptf00230vl8fqjya6mu\",\"x\":3,\"y\":4,\"__typename\":\"Wall\",\"visited\":false,\"work\":null},{\"id\":\"ck9rmaptf00240vl8fkfiana7\",\"x\":4,\"y\":0,\"__typename\":\"Wall\",\"visited\":false,\"work\":null},{\"id\":\"ck9rmaptf00250vl8xmsrzd8b\",\"x\":4,\"y\":1,\"__typename\":\"Wall\",\"visited\":false,\"work\":null},{\"id\":\"ck9rmaptf00260vl8na7u0396\",\"x\":4,\"y\":2,\"__typename\":\"Wall\",\"visited\":false,\"work\":null},{\"id\":\"ck9rmaptf00270vl8vujk4pyt\",\"x\":4,\"y\":3,\"__typename\":\"Wall\",\"visited\":false,\"work\":null},{\"id\":\"ck9rmaptf00280vl8yvs0niby\",\"x\":4,\"y\":4,\"__typename\":\"Wall\",\"visited\":false,\"work\":null},{\"id\":\"ck9rmaptf00040vl8p86phxiy\",\"x\":0,\"y\":0,\"__typename\":\"Wall\",\"visited\":false,\"work\":{\"id\":\"ckaju7ufy00000vl1ewjqp7dk\",\"slug\":\"tree-of-life\",\"asset\":{\"id\":\"ckaju7ufz00010vl1fg6vt57h\",\"type\":\"Website\",\"publicId\":null,\"uri\":\"https://bitforms.art/thetreeoflife/\",\"__typename\":\"Asset\"},\"name\":\"Tree of Life\",\"timeframe\":null,\"medium\":null,\"description\":null,\"dimensions\":null,\"audioAsset\":null,\"authorId\":null,\"author\":null,\"__typename\":\"Work\"}}],\"__typename\":\"Space\",\"event\":{\"id\":\"ck9rj8sqx00000vl90jaa1hvi\",\"slug\":\"cadaf-online\",\"organization\":{\"id\":\"ck9rj7u7h00010vjxl31elqdz\",\"slug\":\"cadaf\",\"__typename\":\"Organization\"},\"__typename\":\"Event\"},\"managers\":[]},\"__typename\":\"DashboardQueries\"}}},\"msg\":\"[server/response DashboardSpace]\"}\nEND RequestId: a6f8045c-a1b5-418f-b29a-aeeba7747f4d\nREPORT RequestId: a6f8045c-a1b5-418f-b29a-aeeba7747f4d\tDuration: 4330.29 ms\tBilled Duration: 4400 ms\tMemory Size: 1024 MB\tMax Memory Used: 191 MB\t\n"

      assert {:ok,
              %{
                "lines" => [
                  %{
                    "data" => %{
                      "level" => 20,
                      "msg" => "[server/request DashboardSpace]",
                      "time" => 1_590_250_719_399,
                      "variables" => %{"id" => "ck9rmapte00030vl8mfudcatq"}
                    }
                  },
                  %{
                    "data" => %{
                      "level" => 10,
                      "msg" => "[server/response DashboardSpace]",
                      "response" => %{
                        "data" => %{
                          "dashboard" => %{
                            "__typename" => "DashboardQueries",
                            "space" => %{
                              "__typename" => "Space",
                              "banner" => %{
                                "__typename" => "Asset",
                                "id" => "ckaiv26x300464mqq521b7fbw",
                                "publicId" => "uploads/i1yaxauyybrsvkya1qko",
                                "type" => "Image",
                                "uri" => nil
                              },
                              "color" => "#ff8cda",
                              "description" =>
                                "ART is the only domain zone created specifically for the global creative community. With .ART individual artists, galleries, museums, art projects, corporate collections and art media can register a clear and concise web address.",
                              "event" => %{
                                "__typename" => "Event",
                                "id" => "ck9rj8sqx00000vl90jaa1hvi",
                                "organization" => %{
                                  "__typename" => "Organization",
                                  "id" => "ck9rj7u7h00010vjxl31elqdz",
                                  "slug" => "cadaf"
                                },
                                "slug" => "cadaf-online"
                              },
                              "extent" => 5,
                              "id" => "ck9rmapte00030vl8mfudcatq",
                              "managers" => [],
                              "name" => ".Art",
                              "section" => %{
                                "__typename" => "Section",
                                "color" => "#be46ff",
                                "id" => "ck9rj94h000010vlbdyax3nc5",
                                "name" => "Sponsors"
                              },
                              "sectionId" => "ck9rj94h000010vlbdyax3nc5",
                              "slug" => "demo-space-1",
                              "visited" => true,
                              "walls" => [
                                %{
                                  "__typename" => "Wall",
                                  "id" => "ck9rmaptf00050vl8hbdlp7an",
                                  "visited" => false,
                                  "work" => nil,
                                  "x" => 0,
                                  "y" => 1
                                },
                                %{
                                  "__typename" => "Wall",
                                  "id" => "ck9rmaptf00060vl8cadl12dq",
                                  "visited" => false,
                                  "work" => nil,
                                  "x" => 0,
                                  "y" => 2
                                },
                                %{
                                  "__typename" => "Wall",
                                  "id" => "ck9rmaptf00070vl8gvmc8ba2",
                                  "visited" => false,
                                  "work" => nil,
                                  "x" => 0,
                                  "y" => 3
                                },
                                %{
                                  "__typename" => "Wall",
                                  "id" => "ck9rmaptf00080vl8c69e21jg",
                                  "visited" => false,
                                  "work" => nil,
                                  "x" => 0,
                                  "y" => 4
                                },
                                %{
                                  "__typename" => "Wall",
                                  "id" => "ck9rmaptf00090vl81zgv6wut",
                                  "visited" => false,
                                  "work" => nil,
                                  "x" => 1,
                                  "y" => 0
                                },
                                %{
                                  "__typename" => "Wall",
                                  "id" => "ck9rmaptf00100vl8uc5byfyz",
                                  "visited" => false,
                                  "work" => nil,
                                  "x" => 1,
                                  "y" => 1
                                },
                                %{
                                  "__typename" => "Wall",
                                  "id" => "ck9rmaptf00110vl8ic7n0idp",
                                  "visited" => false,
                                  "work" => nil,
                                  "x" => 1,
                                  "y" => 2
                                },
                                %{
                                  "__typename" => "Wall",
                                  "id" => "ck9rmaptf00120vl8rgx36wvu",
                                  "visited" => false,
                                  "work" => nil,
                                  "x" => 1,
                                  "y" => 3
                                },
                                %{
                                  "__typename" => "Wall",
                                  "id" => "ck9rmaptf00130vl8s43e5bvj",
                                  "visited" => false,
                                  "work" => nil,
                                  "x" => 1,
                                  "y" => 4
                                },
                                %{
                                  "__typename" => "Wall",
                                  "id" => "ck9rmaptf00140vl8s7hf5a58",
                                  "visited" => false,
                                  "work" => nil,
                                  "x" => 2,
                                  "y" => 0
                                },
                                %{
                                  "__typename" => "Wall",
                                  "id" => "ck9rmaptf00150vl8j50ichts",
                                  "visited" => false,
                                  "work" => nil,
                                  "x" => 2,
                                  "y" => 1
                                },
                                %{
                                  "__typename" => "Wall",
                                  "id" => "ck9rmaptf00160vl8ouhdj9b3",
                                  "visited" => true,
                                  "work" => nil,
                                  "x" => 2,
                                  "y" => 2
                                },
                                %{
                                  "__typename" => "Wall",
                                  "id" => "ck9rmaptf00170vl8p8uottnx",
                                  "visited" => false,
                                  "work" => nil,
                                  "x" => 2,
                                  "y" => 3
                                },
                                %{
                                  "__typename" => "Wall",
                                  "id" => "ck9rmaptf00180vl87k6npcdo",
                                  "visited" => false,
                                  "work" => nil,
                                  "x" => 2,
                                  "y" => 4
                                },
                                %{
                                  "__typename" => "Wall",
                                  "id" => "ck9rmaptf00190vl89gnngj0t",
                                  "visited" => true,
                                  "work" => nil,
                                  "x" => 3,
                                  "y" => 0
                                },
                                %{
                                  "__typename" => "Wall",
                                  "id" => "ck9rmaptf00200vl86mn9yqfa",
                                  "visited" => true,
                                  "work" => nil,
                                  "x" => 3,
                                  "y" => 1
                                },
                                %{
                                  "__typename" => "Wall",
                                  "id" => "ck9rmaptf00210vl8cw4c2sd6",
                                  "visited" => true,
                                  "work" => nil,
                                  "x" => 3,
                                  "y" => 2
                                },
                                %{
                                  "__typename" => "Wall",
                                  "id" => "ck9rmaptf00220vl8urekxxkw",
                                  "visited" => false,
                                  "work" => nil,
                                  "x" => 3,
                                  "y" => 3
                                },
                                %{
                                  "__typename" => "Wall",
                                  "id" => "ck9rmaptf00230vl8fqjya6mu",
                                  "visited" => false,
                                  "work" => nil,
                                  "x" => 3,
                                  "y" => 4
                                },
                                %{
                                  "__typename" => "Wall",
                                  "id" => "ck9rmaptf00240vl8fkfiana7",
                                  "visited" => false,
                                  "work" => nil,
                                  "x" => 4,
                                  "y" => 0
                                },
                                %{
                                  "__typename" => "Wall",
                                  "id" => "ck9rmaptf00250vl8xmsrzd8b",
                                  "visited" => false,
                                  "work" => nil,
                                  "x" => 4,
                                  "y" => 1
                                },
                                %{
                                  "__typename" => "Wall",
                                  "id" => "ck9rmaptf00260vl8na7u0396",
                                  "visited" => false,
                                  "work" => nil,
                                  "x" => 4,
                                  "y" => 2
                                },
                                %{
                                  "__typename" => "Wall",
                                  "id" => "ck9rmaptf00270vl8vujk4pyt",
                                  "visited" => false,
                                  "work" => nil,
                                  "x" => 4,
                                  "y" => 3
                                },
                                %{
                                  "__typename" => "Wall",
                                  "id" => "ck9rmaptf00280vl8yvs0niby",
                                  "visited" => false,
                                  "work" => nil,
                                  "x" => 4,
                                  "y" => 4
                                },
                                %{
                                  "__typename" => "Wall",
                                  "id" => "ck9rmaptf00040vl8p86phxiy",
                                  "visited" => false,
                                  "work" => %{
                                    "__typename" => "Work",
                                    "asset" => %{
                                      "__typename" => "Asset",
                                      "id" => "ckaju7ufz00010vl1fg6vt57h",
                                      "publicId" => nil,
                                      "type" => "Website",
                                      "uri" => "https://bitforms.art/thetreeoflife/"
                                    },
                                    "audioAsset" => nil,
                                    "author" => nil,
                                    "authorId" => nil,
                                    "description" => nil,
                                    "dimensions" => nil,
                                    "id" => "ckaju7ufy00000vl1ewjqp7dk",
                                    "medium" => nil,
                                    "name" => "Tree of Life",
                                    "slug" => "tree-of-life",
                                    "timeframe" => nil
                                  },
                                  "x" => 0,
                                  "y" => 0
                                }
                              ]
                            }
                          }
                        },
                        "http" => %{"headers" => %{}}
                      },
                      "time" => 1_590_250_723_600
                    }
                  }
                ],
                "parse_status" => "full",
                "message_truncated" => true,
                "report" => %{
                  "billed_duration_ms" => 4400,
                  "duration_ms" => 4330,
                  "max_memory_used_mb" => 191,
                  "memory_size_mb" => 1024
                },
                "request_id" => "a6f8045c-a1b5-418f-b29a-aeeba7747f4d"
              }} == parse(string)
    end
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
               "lines" => [line],
               "parse_status" => "full"
             }
           } = parse(message)

    assert %{
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
           } = line
  end

  test "GET requests" do
    message = """
    START RequestId: 105ebfa1-b4a2-49a8-87eb-053396eb7f94
    [GET] /workspaces/64a7125e5724b20008b24e7f status=303
    END RequestId: 105ebfa1-b4a2-49a8-87eb-053396eb7f94
    REPORT RequestId: 105ebfa1-b4a2-49a8-87eb-053396eb7f94 Duration: 33 ms Billed Duration: 34 ms Memory Size: 3008 MB Max Memory Used: 460 MB
    """

    assert {
             :ok,
             %{
               "report" => %{
                 "billed_duration_ms" => 34,
                 "duration_ms" => 33,
                 "max_memory_used_mb" => 460,
                 "memory_size_mb" => 3008
               },
               "method" => "GET",
               "path" => "/workspaces/64a7125e5724b20008b24e7f",
               "status" => 303,
               "request_id" => "105ebfa1-b4a2-49a8-87eb-053396eb7f94",
               "parse_status" => "full"
             }
           } = parse(message)
  end
end
