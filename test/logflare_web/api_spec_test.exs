defmodule LogflareWeb.ApiSpecTest do
  use ExUnit.Case, async: true

  alias LogflareWeb.ApiSpec

  test "resolves every schema reference" do
    assert %OpenApiSpex.OpenApi{} = ApiSpec.spec()
  end

  test "rejects an undocumented response status" do
    conn =
      Phoenix.ConnTest.build_conn(:get, "/api/account")
      |> OpenApiSpex.Plug.PutApiSpec.call(ApiSpec)
      |> Plug.Conn.put_resp_content_type("application/json")
      |> Plug.Conn.send_resp(418, "{}")

    assert_raise ExUnit.AssertionError,
                 ~r/No OpenAPI response is documented for GET \/api\/account with status 418/,
                 fn -> LogflareWeb.ConnCase.assert_open_api_response(conn) end
  end

  test "documents every management API route" do
    spec = ApiSpec.spec()

    missing_operations =
      for {path, verb} <- ApiSpec.management_route_operations(),
          is_nil(spec.paths |> Map.fetch!(path) |> Map.get(verb)),
          do: "#{verb |> Atom.to_string() |> String.upcase()} #{path}"

    assert missing_operations == [],
           "Management routes without an OpenAPI operation:\n#{Enum.join(missing_operations, "\n")}"
  end
end
