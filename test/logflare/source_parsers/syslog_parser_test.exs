defmodule Logflare.Logs.SyslogParserTest do
  @moduledoc false
  use Logflare.DataCase, async: true
  import Logflare.Logs.SyslogParser
  alias Logflare.Logs.SyslogMessage

  describe "Syslog parser for heroku payloads" do
    test "example message 1" do
      message =
        """
        182 <190>1 2020-08-09T13:30:36.316601+00:00 host phx-limit phx-limit-5885669966-287kp - \e[36mweb.1  | \e[0m13:30:36.314 request_id=b4f92e4a104759b02593c34c41d2f0ce [info] Sent 200 in 1ms
        """
        |> String.trim()

      assert {
               :ok,
               %Logflare.Logs.SyslogMessage{
                 appname: "phx-limit",
                 facility: "local7",
                 hostname: "host",
                 message:
                   "web.1  | 13:30:36.314 request_id=b4f92e4a104759b02593c34c41d2f0ce [info] Sent 200 in 1ms",
                 message_id: nil,
                 message_raw:
                   "182 <190>1 2020-08-09T13:30:36.316601+00:00 host phx-limit phx-limit-5885669966-287kp - \e[36mweb.1  | \e[0m13:30:36.314 request_id=b4f92e4a104759b02593c34c41d2f0ce [info] Sent 200 in 1ms",
                 message_text:
                   "\e[36mweb.1  | \e[0m13:30:36.314 request_id=b4f92e4a104759b02593c34c41d2f0ce [info] Sent 200 in 1ms",
                 priority: 190,
                 process_id: "phx-limit-5885669966-287kp",
                 sd: nil,
                 severity: "info",
                 timestamp: "2020-08-09T13:30:36.316601Z"
               }
             } == parse(message, dialect: :heroku)
    end

    test "example message 2" do
      message =
        """
        169 <190>1 2020-08-09T13:30:36.576402+00:00 host phx-limit phx-limit-5885669966-287kp - \e[36mweb.1  | \e[0m13:30:36.575 [info] CONNECTED TO Phoenix.LiveView.Socket in 202µs
        """
        |> String.trim()

      assert {
               :ok,
               %Logflare.Logs.SyslogMessage{
                 appname: "phx-limit",
                 facility: "local7",
                 hostname: "host",
                 message:
                   "web.1  | 13:30:36.575 [info] CONNECTED TO Phoenix.LiveView.Socket in 202µs",
                 message_id: nil,
                 message_raw:
                   "169 <190>1 2020-08-09T13:30:36.576402+00:00 host phx-limit phx-limit-5885669966-287kp - \e[36mweb.1  | \e[0m13:30:36.575 [info] CONNECTED TO Phoenix.LiveView.Socket in 202µs",
                 message_text:
                   "\e[36mweb.1  | \e[0m13:30:36.575 [info] CONNECTED TO Phoenix.LiveView.Socket in 202µs",
                 priority: 190,
                 process_id: "phx-limit-5885669966-287kp",
                 sd: nil,
                 severity: "info",
                 timestamp: "2020-08-09T13:30:36.576402Z"
               }
             } == parse(message, dialect: :heroku)
    end

    test "example message 3" do
      message =
        """
        126 <190>1 2020-08-09T13:30:36.576423+00:00 host phx-limit phx-limit-5885669966-287kp - \e[36mweb.1  | \e[0m  Transport: :websocket
        """
        |> String.trim()

      assert {
               :ok,
               %Logflare.Logs.SyslogMessage{
                 appname: "phx-limit",
                 facility: "local7",
                 hostname: "host",
                 message: "web.1  |   Transport: :websocket",
                 message_id: nil,
                 message_raw:
                   "126 <190>1 2020-08-09T13:30:36.576423+00:00 host phx-limit phx-limit-5885669966-287kp - \e[36mweb.1  | \e[0m  Transport: :websocket",
                 message_text: "\e[36mweb.1  | \e[0m  Transport: :websocket",
                 priority: 190,
                 process_id: "phx-limit-5885669966-287kp",
                 sd: nil,
                 severity: "info",
                 timestamp: "2020-08-09T13:30:36.576423Z"
               }
             } == parse(message, dialect: :heroku)
    end

    test "example message 4" do
      message =
        """
        149 <190>1 2020-08-09T13:30:36.576424+00:00 host phx-limit phx-limit-5885669966-287kp - \e[36mweb.1  | \e[0m  Serializer: Phoenix.Socket.V2.JSONSerializer
        """
        |> String.trim()

      assert {
               :ok,
               %Logflare.Logs.SyslogMessage{
                 appname: "phx-limit",
                 facility: "local7",
                 hostname: "host",
                 message: "web.1  |   Serializer: Phoenix.Socket.V2.JSONSerializer",
                 message_id: nil,
                 message_raw:
                   "149 <190>1 2020-08-09T13:30:36.576424+00:00 host phx-limit phx-limit-5885669966-287kp - \e[36mweb.1  | \e[0m  Serializer: Phoenix.Socket.V2.JSONSerializer",
                 message_text:
                   "\e[36mweb.1  | \e[0m  Serializer: Phoenix.Socket.V2.JSONSerializer",
                 priority: 190,
                 process_id: "phx-limit-5885669966-287kp",
                 sd: nil,
                 severity: "info",
                 timestamp: "2020-08-09T13:30:36.576424Z"
               }
             } == parse(message, dialect: :heroku)
    end

    test "example message 55" do
      message =
        """
        443 <190>1 2020-08-09T13:30:36.576426+00:00 host phx-limit phx-limit-5885669966-287kp - \e[36mweb.1  | \e[0m  Parameters: %{\"_csrf_token\" => \"Rg1gVgJWUjkVCjISGmQKew0kZRYpYBwwpe00D78ZtsQqqUI9gK6zQReD\", \"_mounts\" => \"0\", \"_track_static\" => %{\"0\" => \"https://phx-limit.gigalixirapp.com/css/app-5e472e0beb5f275dce8c669b8ba7c47e.css?vsn=d\", \"1\" => \"https://phx-limit.gigalixirapp.com/js/app-13b608e49f856a3afa3085d9ce96d5fe.js?vsn=d\"}, \"vsn\" => \"2.0.0\"}
        """
        |> String.trim()

      assert {
               :ok,
               %Logflare.Logs.SyslogMessage{
                 appname: "phx-limit",
                 facility: "local7",
                 hostname: "host",
                 message:
                   "web.1  |   Parameters: %{\"_csrf_token\" => \"Rg1gVgJWUjkVCjISGmQKew0kZRYpYBwwpe00D78ZtsQqqUI9gK6zQReD\", \"_mounts\" => \"0\", \"_track_static\" => %{\"0\" => \"https://phx-limit.gigalixirapp.com/css/app-5e472e0beb5f275dce8c669b8ba7c47e.css?vsn=d\", \"1\" => \"https://phx-limit.gigalixirapp.com/js/app-13b608e49f856a3afa3085d9ce96d5fe.js?vsn=d\"}, \"vsn\" => \"2.0.0\"}",
                 message_id: nil,
                 message_raw:
                   "443 <190>1 2020-08-09T13:30:36.576426+00:00 host phx-limit phx-limit-5885669966-287kp - \e[36mweb.1  | \e[0m  Parameters: %{\"_csrf_token\" => \"Rg1gVgJWUjkVCjISGmQKew0kZRYpYBwwpe00D78ZtsQqqUI9gK6zQReD\", \"_mounts\" => \"0\", \"_track_static\" => %{\"0\" => \"https://phx-limit.gigalixirapp.com/css/app-5e472e0beb5f275dce8c669b8ba7c47e.css?vsn=d\", \"1\" => \"https://phx-limit.gigalixirapp.com/js/app-13b608e49f856a3afa3085d9ce96d5fe.js?vsn=d\"}, \"vsn\" => \"2.0.0\"}",
                 message_text:
                   "\e[36mweb.1  | \e[0m  Parameters: %{\"_csrf_token\" => \"Rg1gVgJWUjkVCjISGmQKew0kZRYpYBwwpe00D78ZtsQqqUI9gK6zQReD\", \"_mounts\" => \"0\", \"_track_static\" => %{\"0\" => \"https://phx-limit.gigalixirapp.com/css/app-5e472e0beb5f275dce8c669b8ba7c47e.css?vsn=d\", \"1\" => \"https://phx-limit.gigalixirapp.com/js/app-13b608e49f856a3afa3085d9ce96d5fe.js?vsn=d\"}, \"vsn\" => \"2.0.0\"}",
                 priority: 190,
                 process_id: "phx-limit-5885669966-287kp",
                 sd: nil,
                 severity: "info",
                 timestamp: "2020-08-09T13:30:36.576426Z"
               }
             } == parse(message, dialect: :heroku)
    end

    test "example message 6" do
      message =
        """
        119 <40>1 2012-11-30T06:45:26+00:00 host app web.3 - Starting process with command `bundle exec rackup config.ru -p 24405`
        """
        |> String.trim()

      assert {
               :ok,
               %SyslogMessage{
                 appname: "app",
                 facility: "syslogd",
                 hostname: "host",
                 message: "Starting process with command `bundle exec rackup config.ru -p 24405`",
                 message_id: nil,
                 message_text:
                   "Starting process with command `bundle exec rackup config.ru -p 24405`",
                 message_raw:
                   "119 <40>1 2012-11-30T06:45:26+00:00 host app web.3 - Starting process with command `bundle exec rackup config.ru -p 24405`",
                 priority: 40,
                 process_id: "web.3",
                 sd: nil,
                 severity: "emergency",
                 timestamp: "2012-11-30T06:45:26Z"
               }
             } == parse(message, dialect: :heroku)
    end

    test "example message 7" do
      message =
        """
        84 <85>1 2018-10-11T22:14:15.003Z leodido - 31932 - [ex@31932 iut="3"] An auth token...
        """
        |> String.trim()

      assert {
               :ok,
               %SyslogMessage{
                 sd: [%{"id" => "ex@31932", "iut" => "3"}],
                 facility: "auth",
                 hostname: "leodido",
                 message: "An auth token...",
                 message_raw:
                   "84 <85>1 2018-10-11T22:14:15.003Z leodido - 31932 - [ex@31932 iut=\"3\"] An auth token...",
                 priority: 85,
                 message_text: "An auth token...",
                 process_id: "31932",
                 severity: "notice",
                 timestamp: "2018-10-11T22:14:15.003Z"
               }
             } == parse(message, dialect: :heroku)
    end

    test "example message 8 with sd and json payload" do
      message =
        """
        84 <85>1 2018-10-11T22:14:15.003Z leodido - 31932 - [ex@31932 iut="3"] JSON payload: {"user": {"id": 4, "name": "John Doe"}, "request_id": 1000}
        """
        |> String.trim()

      assert {
               :ok,
               %SyslogMessage{
                 appname: nil,
                 facility: "auth",
                 hostname: "leodido",
                 message:
                   "JSON payload: {\"user\": {\"id\": 4, \"name\": \"John Doe\"}, \"request_id\": 1000}",
                 message_id: nil,
                 message_text:
                   "JSON payload: {\"user\": {\"id\": 4, \"name\": \"John Doe\"}, \"request_id\": 1000}",
                 message_raw:
                   "84 <85>1 2018-10-11T22:14:15.003Z leodido - 31932 - [ex@31932 iut=\"3\"] JSON payload: {\"user\": {\"id\": 4, \"name\": \"John Doe\"}, \"request_id\": 1000}",
                 priority: 85,
                 process_id: "31932",
                 sd: [
                   %{
                     "id" => "json",
                     "request_id" => 1000,
                     "user" => %{"id" => 4, "name" => "John Doe"}
                   },
                   %{"id" => "ex@31932", "iut" => "3"}
                 ],
                 severity: "notice",
                 timestamp: "2018-10-11T22:14:15.003Z"
               }
             } == parse(message, dialect: :heroku)

      message =
        """
        84 <85>1 2018-10-11T22:14:15.003Z leodido - 31932 - [ex@31932 iut="3"] {"user": {"id": 4, "name": "John Doe"}, "request_id": 1000}
        """
        |> String.trim()

      assert {
               :ok,
               %SyslogMessage{
                 appname: nil,
                 facility: "auth",
                 hostname: "leodido",
                 message: "{\"user\": {\"id\": 4, \"name\": \"John Doe\"}, \"request_id\": 1000}",
                 message_id: nil,
                 message_raw:
                   "84 <85>1 2018-10-11T22:14:15.003Z leodido - 31932 - [ex@31932 iut=\"3\"] {\"user\": {\"id\": 4, \"name\": \"John Doe\"}, \"request_id\": 1000}",
                 message_text:
                   "{\"user\": {\"id\": 4, \"name\": \"John Doe\"}, \"request_id\": 1000}",
                 priority: 85,
                 process_id: "31932",
                 sd: [
                   %{
                     "id" => "json",
                     "request_id" => 1000,
                     "user" => %{"id" => 4, "name" => "John Doe"}
                   },
                   %{"id" => "ex@31932", "iut" => "3"}
                 ],
                 severity: "notice",
                 timestamp: "2018-10-11T22:14:15.003Z"
               }
             } == parse(message, dialect: :heroku)

      message =
        """
        84 <85>1 2018-10-11T22:14:15.003Z leodido - 31932 - [ex@31932 iut="3"] {"user": {"id": 4, "name": "John Doe"}, "request_id": 1000} trailing message log text
        """
        |> String.trim()

      assert {
               :ok,
               %SyslogMessage{
                 appname: nil,
                 facility: "auth",
                 hostname: "leodido",
                 message:
                   "{\"user\": {\"id\": 4, \"name\": \"John Doe\"}, \"request_id\": 1000} trailing message log text",
                 message_text:
                   "{\"user\": {\"id\": 4, \"name\": \"John Doe\"}, \"request_id\": 1000} trailing message log text",
                 message_id: nil,
                 message_raw:
                   "84 <85>1 2018-10-11T22:14:15.003Z leodido - 31932 - [ex@31932 iut=\"3\"] {\"user\": {\"id\": 4, \"name\": \"John Doe\"}, \"request_id\": 1000} trailing message log text",
                 priority: 85,
                 process_id: "31932",
                 sd: [
                   %{
                     "id" => "json",
                     "request_id" => 1000,
                     "user" => %{"id" => 4, "name" => "John Doe"}
                   },
                   %{"id" => "ex@31932", "iut" => "3"}
                 ],
                 severity: "notice",
                 timestamp: "2018-10-11T22:14:15.003Z"
               }
             } == parse(message, dialect: :heroku)

      message =
        """
        84 <85>1 2018-10-11T22:14:15.003Z leodido - 31932 - [ex@31932 iut="3"] \e[36mweb.1  | \e[0m{"user": {"id": 4, "name": "John Doe"}, "request_id": 1000} trailing message log text
        """
        |> String.trim()

      assert {
               :ok,
               %Logflare.Logs.SyslogMessage{
                 appname: nil,
                 facility: "auth",
                 hostname: "leodido",
                 message:
                   "web.1  | {\"user\": {\"id\": 4, \"name\": \"John Doe\"}, \"request_id\": 1000} trailing message log text",
                 message_id: nil,
                 message_raw:
                   "84 <85>1 2018-10-11T22:14:15.003Z leodido - 31932 - [ex@31932 iut=\"3\"] \e[36mweb.1  | \e[0m{\"user\": {\"id\": 4, \"name\": \"John Doe\"}, \"request_id\": 1000} trailing message log text",
                 message_text:
                   "\e[36mweb.1  | \e[0m{\"user\": {\"id\": 4, \"name\": \"John Doe\"}, \"request_id\": 1000} trailing message log text",
                 priority: 85,
                 process_id: "31932",
                 sd: [
                   %{
                     "id" => "json",
                     "request_id" => 1000,
                     "user" => %{"id" => 4, "name" => "John Doe"}
                   },
                   %{"id" => "ex@31932", "iut" => "3"}
                 ],
                 severity: "notice",
                 timestamp: "2018-10-11T22:14:15.003Z"
               }
             } == parse(message, dialect: :heroku)
    end

    test "example message 8 without sd, with json payload" do
      message =
        """
        84 <85>1 2018-10-11T22:14:15.003Z leodido - 31932 ID0001 {"user": {"id": 4, "name": "John Doe"}, "request_id": 1000}
        """
        |> String.trim()

      assert {
               :ok,
               %Logflare.Logs.SyslogMessage{
                 appname: nil,
                 facility: "auth",
                 hostname: "leodido",
                 message: "{\"user\": {\"id\": 4, \"name\": \"John Doe\"}, \"request_id\": 1000}",
                 message_id: "ID0001",
                 message_raw:
                   "84 <85>1 2018-10-11T22:14:15.003Z leodido - 31932 ID0001 {\"user\": {\"id\": 4, \"name\": \"John Doe\"}, \"request_id\": 1000}",
                 priority: 85,
                 message_text:
                   "{\"user\": {\"id\": 4, \"name\": \"John Doe\"}, \"request_id\": 1000}",
                 process_id: "31932",
                 sd: [
                   %{
                     "id" => "json",
                     "request_id" => 1000,
                     "user" => %{"id" => 4, "name" => "John Doe"}
                   }
                 ],
                 severity: "notice",
                 timestamp: "2018-10-11T22:14:15.003Z"
               }
             } == parse(message, dialect: :heroku)
    end
  end

  describe "Syslog parser" do
    test "example 1" do
      message =
        """
        <34>1 2003-10-11T22:14:15.003Z mymachine.example.com su - ID47 - BOM'su root' failed for lonvick on /dev/pts/8
        """
        |> String.trim()

      assert {
               :ok,
               %SyslogMessage{
                 appname: "su",
                 facility: "auth",
                 hostname: "mymachine.example.com",
                 message: "BOM'su root' failed for lonvick on /dev/pts/8",
                 message_id: "ID47",
                 message_raw:
                   "<34>1 2003-10-11T22:14:15.003Z mymachine.example.com su - ID47 - BOM'su root' failed for lonvick on /dev/pts/8",
                 message_text: "BOM'su root' failed for lonvick on /dev/pts/8",
                 priority: 34,
                 process_id: nil,
                 sd: nil,
                 severity: "critical",
                 timestamp: "2003-10-11T22:14:15.003Z"
               }
             } == parse(message)
    end

    test "example 2" do
      message =
        """
        <165>1 2003-08-24T05:14:15.000003-07:00 192.0.2.1 myproc 8710 - - %% It's time to make the do-nuts.
        """
        |> String.trim()

      assert {
               :ok,
               %SyslogMessage{
                 appname: "myproc",
                 facility: "local4",
                 hostname: "192.0.2.1",
                 message: "%% It's time to make the do-nuts.",
                 message_id: nil,
                 message_text: "%% It's time to make the do-nuts.",
                 message_raw:
                   "<165>1 2003-08-24T05:14:15.000003-07:00 192.0.2.1 myproc 8710 - - %% It's time to make the do-nuts.",
                 priority: 165,
                 process_id: "8710",
                 sd: nil,
                 severity: "notice",
                 timestamp: "2003-08-24T12:14:15.000003Z"
               }
             } == parse(message)
    end

    test "example 3" do
      message =
        """
        <165>1 2003-10-11T22:14:15.003Z mymachine.example.com evntslog - ID47 [exampleSDID@32473 iut="3" eventSource="Application" eventID="1011"] BOMAn application event log entry...
        """
        |> String.trim()

      assert {
               :ok,
               %SyslogMessage{
                 appname: "evntslog",
                 facility: "local4",
                 hostname: "mymachine.example.com",
                 message: "BOMAn application event log entry...",
                 message_id: "ID47",
                 message_raw:
                   "<165>1 2003-10-11T22:14:15.003Z mymachine.example.com evntslog - ID47 [exampleSDID@32473 iut=\"3\" eventSource=\"Application\" eventID=\"1011\"] BOMAn application event log entry...",
                 message_text: "BOMAn application event log entry...",
                 priority: 165,
                 process_id: nil,
                 sd: [
                   %{
                     "eventID" => "1011",
                     "eventSource" => "Application",
                     "id" => "exampleSDID@32473",
                     "iut" => "3"
                   }
                 ],
                 severity: "notice",
                 timestamp: "2003-10-11T22:14:15.003Z"
               }
             } == parse(message)
    end

    test "example 4" do
      message =
        """
        <165>1 2003-10-11T22:14:15.003Z mymachine.example.com evntslog - ID47 [exampleSDID@32473 iut="3" eventSource="Application" eventID="1011"][examplePriority@32473 class="high"]
        """
        |> String.trim()

      assert {
               :ok,
               %SyslogMessage{
                 appname: "evntslog",
                 facility: "local4",
                 hostname: "mymachine.example.com",
                 message: nil,
                 message_id: "ID47",
                 message_raw:
                   "<165>1 2003-10-11T22:14:15.003Z mymachine.example.com evntslog - ID47 [exampleSDID@32473 iut=\"3\" eventSource=\"Application\" eventID=\"1011\"][examplePriority@32473 class=\"high\"]",
                 priority: 165,
                 process_id: nil,
                 sd: [
                   %{
                     "eventID" => "1011",
                     "eventSource" => "Application",
                     "id" => "exampleSDID@32473",
                     "iut" => "3"
                   },
                   %{
                     "class" => "high",
                     "id" => "examplePriority@32473"
                   }
                 ],
                 severity: "notice",
                 timestamp: "2003-10-11T22:14:15.003Z"
               }
             } == parse(message)
    end

    test "example 4 with escapes" do
      message =
        ~S"""
        <165>1 2003-10-11T22:14:15.003Z mymachine.example.com evntslog - ID47 [exampleSDID@32473 iut="3" eventSource="Application" eventID="1011"][examplePriority@32473 class="\"high\""]
        """
        |> String.trim()

      assert {
               :ok,
               %SyslogMessage{
                 appname: "evntslog",
                 facility: "local4",
                 hostname: "mymachine.example.com",
                 message: nil,
                 message_id: "ID47",
                 message_raw:
                   "<165>1 2003-10-11T22:14:15.003Z mymachine.example.com evntslog - ID47 [exampleSDID@32473 iut=\"3\" eventSource=\"Application\" eventID=\"1011\"][examplePriority@32473 class=\"\\\"high\\\"\"]",
                 priority: 165,
                 process_id: nil,
                 sd: [
                   %{
                     "eventID" => "1011",
                     "eventSource" => "Application",
                     "id" => "exampleSDID@32473",
                     "iut" => "3"
                   },
                   %{
                     "class" => "\"high\"",
                     "id" => "examplePriority@32473"
                   }
                 ],
                 severity: "notice",
                 timestamp: "2003-10-11T22:14:15.003Z"
               }
             } == parse(message)
    end
  end
end
