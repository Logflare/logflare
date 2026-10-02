defmodule Logflare.RuntimeConfigurationTest do
  use ExUnit.Case, async: true

  @aws_environment %{
    "AWS_ACCESS_KEY_ID" => "ASIATEMPORARY",
    "AWS_SECRET_ACCESS_KEY" => "temporary-secret",
    "AWS_SESSION_TOKEN" => "session-token"
  }

  test "builds RDS credentials from a complete AWS environment tuple" do
    assert Env.aws_rds_credentials(@aws_environment) == [
             access_key_id: "ASIATEMPORARY",
             secret_access_key: "temporary-secret",
             security_token: "session-token"
           ]
  end

  test "rejects missing and empty values from the AWS environment tuple" do
    for key <- Map.keys(@aws_environment) do
      assert Env.aws_rds_credentials(Map.delete(@aws_environment, key)) == []
      assert Env.aws_rds_credentials(Map.put(@aws_environment, key, "")) == []
    end
  end

  test "ignores unrelated environment values" do
    environment = Map.put(@aws_environment, "AWS_REGION", "eu-west-1")

    assert Env.aws_rds_credentials(environment) == [
             access_key_id: "ASIATEMPORARY",
             secret_access_key: "temporary-secret",
             security_token: "session-token"
           ]
  end

  test "scales HTTP acceptors with online schedulers" do
    assert Env.http_num_acceptors(nil, 1) == 40
    assert Env.http_num_acceptors(nil, 2) == 80
    assert Env.http_num_acceptors(nil, 4) == 160
    assert Env.http_num_acceptors(nil, 31) == 1240
  end

  test "caps scaled HTTP acceptors at 1250" do
    assert Env.http_num_acceptors(nil, 32) == 1250
    assert Env.http_num_acceptors(nil, 128) == 1250
  end

  test "treats a blank HTTP acceptor override as unset" do
    assert Env.http_num_acceptors("", 2) == 80
    assert Env.http_num_acceptors(" \t\n", 2) == 80
  end

  test "uses an explicit HTTP acceptor override without the cap" do
    assert Env.http_num_acceptors("8", 32) == 8
    assert Env.http_num_acceptors(" 2000 ", 1) == 2000
  end

  test "rejects invalid HTTP acceptor overrides" do
    for value <- ["0", "-1", "1.5", "10x", "invalid"] do
      assert_raise ArgumentError,
                   ~r/PHX_HTTP_NUM_ACCEPTORS must be a positive integer/,
                   fn -> Env.http_num_acceptors(value, 2) end
    end
  end

  test "merges HTTP acceptors into the endpoint thousand_island options" do
    thousand_island_options =
      Application.fetch_env!(:logflare, LogflareWeb.Endpoint)[:http][:thousand_island_options]

    assert thousand_island_options[:num_acceptors] ==
             Env.http_num_acceptors(
               System.get_env("PHX_HTTP_NUM_ACCEPTORS"),
               System.schedulers_online()
             )

    assert thousand_island_options[:read_timeout] == 620_000
    assert thousand_island_options[:transport_options][:reuseport_lb]
  end
end
