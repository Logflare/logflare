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

  test "aws_region/1 prefers AWS_REGION over AWS_DEFAULT_REGION" do
    assert Env.aws_region(%{"AWS_REGION" => "eu-west-1", "AWS_DEFAULT_REGION" => "us-east-1"}) ==
             "eu-west-1"
  end

  test "aws_region/1 falls back to AWS_DEFAULT_REGION" do
    assert Env.aws_region(%{"AWS_DEFAULT_REGION" => "us-west-2"}) == "us-west-2"
  end

  test "aws_region/1 returns nil when neither is set or both are empty" do
    assert Env.aws_region(%{}) == nil
    assert Env.aws_region(%{"AWS_REGION" => "", "AWS_DEFAULT_REGION" => ""}) == nil
  end

  test "get_integer/2 with an integer env value" do
    for {value, expected} <- [{"42", 42}, {"-7", -7}, {"0", 0}] do
      env = put_env(value)

      assert Env.get_integer(env) == expected
      assert Env.get_integer(env, 10) == expected
    end
  end

  test "get_integer/2 with an unset or empty env value" do
    for env <- ["LOGFLARE_TEST_GET_INTEGER_UNSET", put_env("")] do
      assert Env.get_integer(env) == nil
      assert Env.get_integer(env, 10) == 10
    end
  end

  test "get_integer/2 with a non-integer env value" do
    for value <- ["abc", "12abc", "1.5", " 42"] do
      env = put_env(value)

      assert_raise ArgumentError, ~r/#{env}/, fn -> Env.get_integer(env) end
    end
  end

  defp put_env(value) do
    env = "LOGFLARE_TEST_GET_INTEGER_#{System.unique_integer([:positive])}"
    System.put_env(env, value)
    on_exit(fn -> System.delete_env(env) end)
    env
  end
end
