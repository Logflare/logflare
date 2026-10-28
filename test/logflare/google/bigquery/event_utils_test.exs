defmodule Logflare.Google.BigQuery.EventUtilsTest do
  use ExUnit.Case, async: true

  alias Logflare.Google.BigQuery.EventUtils

  doctest EventUtils

  describe "wrap_body_fields/1" do
    test "wraps nested maps in lists" do
      event = %{"message" => "hello", "metadata" => %{"user_id" => "123"}}

      result = EventUtils.wrap_body_fields(event)
      expected = %{"message" => "hello", "metadata" => [%{"user_id" => "123"}]}

      assert result == expected
    end

    test "handles lists of maps unchanged" do
      event = %{"tags" => [%{"key" => "env", "value" => "prod"}]}

      result = EventUtils.wrap_body_fields(event)

      assert result == event
    end

    test "wraps maps nested in top-level lists of maps" do
      event = %{
        "events" => [
          %{"name" => "exception", "attributes" => %{"exception.message" => "timeout"}}
        ]
      }

      assert EventUtils.wrap_body_fields(event) == %{
               "events" => [
                 %{"name" => "exception", "attributes" => [%{"exception.message" => "timeout"}]}
               ]
             }
    end

    test "handles nested list-of-lists (e.g. a serialized stacktrace)" do
      event = %{
        "stacktrace" => [
          ["Elixir.ProjectThree.TmAmTicket", "ingest", 2],
          ["Elixir.ProjectThree.TmHost", "dispatch", 1]
        ]
      }

      assert EventUtils.wrap_body_fields(event) == event
    end

    test "handles a list whose head is a map but tail contains lists and scalars" do
      event = %{
        "mixed" => [
          %{"a" => %{"b" => 1}},
          ["nested", "list"],
          "scalar"
        ]
      }

      result = EventUtils.wrap_body_fields(event)

      expected = %{
        "mixed" => [
          %{"a" => [%{"b" => 1}]},
          ["nested", "list"],
          "scalar"
        ]
      }

      assert result == expected
    end
  end
end
