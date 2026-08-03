defmodule Logflare.Google.BigQuery.EventUtilsTest do
  use ExUnit.Case, async: true

  alias Logflare.Google.BigQuery.EventUtils

  doctest EventUtils

  describe "prepare_for_ingest/1" do
    test "wraps event in list and nested maps in lists" do
      event = %{"message" => "hello", "metadata" => %{"user_id" => "123"}}

      result = EventUtils.prepare_for_ingest(event)
      expected = [%{"message" => "hello", "metadata" => [%{"user_id" => "123"}]}]

      assert result == expected
    end

    test "handles lists of maps unchanged" do
      event = %{"tags" => [%{"key" => "env", "value" => "prod"}]}

      result = EventUtils.prepare_for_ingest(event)

      assert result == [event]
    end

    test "handles nested list-of-lists (e.g. a serialized stacktrace)" do
      event = %{
        "stacktrace" => [
          ["Elixir.ProjectThree.TmAmTicket", "ingest", 2],
          ["Elixir.ProjectThree.TmHost", "dispatch", 1]
        ]
      }

      assert EventUtils.prepare_for_ingest(event) == [event]
    end

    test "handles a list whose head is a map but tail contains lists and scalars" do
      event = %{
        "mixed" => [
          %{"a" => %{"b" => 1}},
          ["nested", "list"],
          "scalar"
        ]
      }

      result = EventUtils.prepare_for_ingest(event)

      expected = [
        %{
          "mixed" => [
            %{"a" => [%{"b" => 1}]},
            ["nested", "list"],
            "scalar"
          ]
        }
      ]

      assert result == expected
    end
  end
end
