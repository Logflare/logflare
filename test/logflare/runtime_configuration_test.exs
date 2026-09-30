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
end
