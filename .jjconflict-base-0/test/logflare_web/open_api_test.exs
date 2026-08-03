defmodule LogflareWeb.OpenApiTest do
  use ExUnit.Case, async: true

  alias LogflareWeb.Api.AccessTokenController
  alias LogflareWeb.Api.QueryController
  alias LogflareWeb.Api.TeamController
  alias LogflareWeb.OpenApiSchemas.AccessToken
  alias LogflareWeb.OpenApiSchemas.QueryResult
  alias OpenApiSpex.MediaType
  alias OpenApiSpex.Response
  alias OpenApiSpex.Schema

  @bad_request_error_schema %Schema{
    oneOf: [
      %Schema{type: :string},
      %Schema{type: :object}
    ]
  }
  @string_error_schema %Schema{type: :string}

  test "Management API query success is documented as an object containing result rows" do
    response =
      QueryController.open_api_operation(:query)
      |> Map.fetch!(:responses)
      |> Map.fetch!(200)

    assert %Response{
             content: %{"application/json" => %MediaType{schema: QueryResult}}
           } = response

    assert %Schema{
             type: :object,
             properties: %{result: %Schema{type: :array, items: %Schema{type: :object}}},
             required: [:result]
           } = QueryResult.schema()
  end

  test "Management API access token timestamps are documented as RFC3339" do
    assert %Schema{type: :string, format: :"date-time"} =
             AccessToken.schema().properties.inserted_at
  end

  test "Management API 400 errors are documented as JSON" do
    for action <- [:parse, :query] do
      QueryController.open_api_operation(action)
      |> Map.fetch!(:responses)
      |> Map.fetch!(400)
      |> assert_json_error_response("BadRequestResponse", @bad_request_error_schema)
    end
  end

  test "Management API 401 errors are documented as JSON" do
    AccessTokenController.open_api_operation(:create)
    |> Map.fetch!(:responses)
    |> Map.fetch!(401)
    |> assert_json_error_response("UnauthorizedResponse", @string_error_schema)

    for action <- [:parse, :query] do
      QueryController.open_api_operation(action)
      |> Map.fetch!(:responses)
      |> Map.fetch!(401)
      |> assert_json_error_response("UnauthorizedResponse", @string_error_schema)
    end
  end

  test "Management API 404 errors are documented as JSON" do
    TeamController.open_api_operation(:show)
    |> Map.fetch!(:responses)
    |> Map.fetch!(404)
    |> assert_json_error_response("NotFoundResponse", @string_error_schema)
  end

  defp assert_json_error_response(response, schema_title, error_schema) do
    assert %Response{
             content: %{
               "application/json" => %MediaType{
                 schema: %Schema{
                   title: ^schema_title,
                   type: :object,
                   properties: %{error: ^error_schema},
                   required: [:error]
                 }
               }
             }
           } = response
  end
end
