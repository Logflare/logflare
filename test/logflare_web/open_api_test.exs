defmodule LogflareWeb.OpenApiTest do
  use ExUnit.Case, async: true

  alias LogflareWeb.Api.QueryController
  alias LogflareWeb.Api.TeamController
  alias OpenApiSpex.MediaType
  alias OpenApiSpex.Response
  alias OpenApiSpex.Schema

  @bad_request_error_schema %Schema{
    oneOf: [
      %Schema{type: :string},
      %Schema{type: :object}
    ]
  }
  @not_found_error_schema %Schema{type: :string}

  test "Management API 400 errors are documented as JSON" do
    for action <- [:parse, :query] do
      QueryController.open_api_operation(action)
      |> Map.fetch!(:responses)
      |> Map.fetch!(400)
      |> assert_json_error_response("BadRequestResponse", @bad_request_error_schema)
    end
  end

  test "Management API 404 errors are documented as JSON" do
    TeamController.open_api_operation(:show)
    |> Map.fetch!(:responses)
    |> Map.fetch!(404)
    |> assert_json_error_response("NotFoundResponse", @not_found_error_schema)
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
