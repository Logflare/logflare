defmodule Logflare.BigQuery.EventUtilsTest do
  alias Logflare.BigQuery.EventUtils
  use ExUnit.Case

  describe "event utils" do
    test "wraps maps with lists to be injested by BigQuery" do
      assert EventUtils.prepare_for_injest(raw()) === wrapped()
    end
  end

  def raw do
    %{
      "datacenter" => "aws",
      "ip_address" => "100.100.100.100",
      "request_headers" => %{
        "connection" => "close",
        "servers" => %{
          "blah" => "water",
          "home" => "not home",
          "deep_nest" => [
            %{"more_deep_nest" => %{"a" => 1}},
            %{"more_deep_nest2" => %{"a" => 2}}
          ]
        },
        "user_agent" => "chrome"
      },
      "request_method" => "POST"
    }
  end

  def wrapped do
    %{
      "datacenter" => "aws",
      "ip_address" => "100.100.100.100",
      "request_headers" => [
        %{
          "connection" => "close",
          "servers" => [
            %{
              "blah" => "water",
              "home" => "not home",
              "deep_nest" => [
                %{"more_deep_nest" => [%{"a" => 1}]},
                %{"more_deep_nest2" => [%{"a" => 2}]}
              ]
            }
          ],
          "user_agent" => "chrome"
        }
      ],
      "request_method" => "POST"
    }
  end
end
