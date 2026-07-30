defmodule LogflareWeb.ApiSpecTest do
  use ExUnit.Case, async: true

  alias LogflareWeb.ApiSpec
  alias LogflareWeb.OpenApiSchemas.BackendApiParams
  alias LogflareWeb.OpenApiSchemas.BackendApiSchema
  alias LogflareWeb.OpenApiSchemas.EndpointApiParams
  alias LogflareWeb.OpenApiSchemas.EndpointApiSchema
  alias LogflareWeb.OpenApiSchemas.RuleApiSchema
  alias LogflareWeb.OpenApiSchemas.RuleParams
  alias LogflareWeb.OpenApiSchemas.Source
  alias LogflareWeb.OpenApiSchemas.SourceParams
  alias LogflareWeb.OpenApiSchemas.Team
  alias LogflareWeb.OpenApiSchemas.TeamParams
  alias OpenApiSpex.Plug.PutApiSpec

  test "resolves every schema reference" do
    assert %OpenApiSpex.OpenApi{} = ApiSpec.spec()
  end

  test "rejects an undocumented response status" do
    conn =
      Phoenix.ConnTest.build_conn(:get, "/api/account")
      |> PutApiSpec.call(ApiSpec)
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

  test "uses dedicated write schemas for management resource mutations" do
    spec = ApiSpec.spec()

    for {path, verb, request_schema, response_schema, output_only_fields} <- [
          {"/api/sources", :post, SourceParams, Source, [:id, :token, :api_quota, :backends]},
          {"/api/sources/{token}", :put, SourceParams, Source,
           [:id, :token, :api_quota, :backends]},
          {"/api/backends", :post, BackendApiParams, BackendApiSchema,
           [:id, :token, :inserted_at, :updated_at]},
          {"/api/backends/{token}", :put, BackendApiParams, BackendApiSchema,
           [:id, :token, :inserted_at, :updated_at]},
          {"/api/endpoints", :post, EndpointApiParams, EndpointApiSchema,
           [:id, :token, :source_mapping]},
          {"/api/endpoints/{token}", :put, EndpointApiParams, EndpointApiSchema,
           [:id, :token, :source_mapping]},
          {"/api/rules", :post, RuleParams, RuleApiSchema, [:id, :token]},
          {"/api/rules/{token}", :put, RuleParams, RuleApiSchema, [:id, :token]},
          {"/api/teams", :post, TeamParams, Team, [:token, :user, :team_users]},
          {"/api/teams/{token}", :put, TeamParams, Team, [:token, :user, :team_users]}
        ] do
      assert request_schema(spec, path, verb).title == request_schema.schema().title
      refute request_schema.schema().properties == response_schema.schema().properties

      for field <- output_only_fields do
        refute Map.has_key?(request_schema.schema().properties, field)
      end
    end
  end

  defp request_schema(spec, path, verb) do
    spec.paths
    |> Map.fetch!(path)
    |> Map.fetch!(verb)
    |> Map.fetch!(:requestBody)
    |> Map.fetch!(:content)
    |> Map.fetch!("application/json")
    |> Map.fetch!(:schema)
    |> resolve_schema(spec)
  end

  defp resolve_schema(%OpenApiSpex.Reference{} = ref, spec) do
    OpenApiSpex.Reference.resolve_schema(ref, spec.components.schemas)
  end

  defp resolve_schema(%OpenApiSpex.Schema{} = schema, _spec), do: schema
end
