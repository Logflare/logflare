defmodule LogflareWeb.Api.KeyValueController do
  use LogflareWeb, :controller
  use OpenApiSpex.ControllerSpecs

  alias Logflare.Billing
  alias Logflare.KeyValues
  alias LogflareWeb.OpenApi.Created
  alias LogflareWeb.OpenApi.List
  alias LogflareWeb.OpenApiSchemas.KeyValueApiSchema

  action_fallback(LogflareWeb.Api.FallbackController)

  tags(["management"])

  operation(:index,
    summary: "List key-value pairs",
    parameters: [
      key: [in: :query, description: "Filter by key (exact match)", type: :string],
      value: [in: :query, description: "Filter by value (exact match)", type: :string]
    ],
    responses: %{200 => List.response(KeyValueApiSchema)}
  )

  def index(%{assigns: %{user: user}} = conn, params) do
    key_values =
      KeyValues.list_key_values(user_id: user.id, key: params["key"], value: params["value"])

    json(conn, key_values)
  end

  operation(:create,
    summary: "Bulk upsert key-value pairs",
    request_body:
      {"Key-value pairs", "application/json",
       %OpenApiSpex.Schema{
         type: :array,
         items: KeyValueApiSchema
       }},
    responses: %{
      201 => Created.response(KeyValueApiSchema)
    }
  )

  def create(%{assigns: %{user: user}} = conn, %{"_json" => entries}) when is_list(entries) do
    plan = Billing.get_plan_by_user(user)
    current_count = KeyValues.count_key_values(user.id)

    if current_count + length(entries) > plan.limit_key_values do
      {:error, "Key-value limit of #{plan.limit_key_values} exceeded"}
    else
      {count, _} = KeyValues.bulk_upsert_key_values(user.id, entries)

      conn
      |> put_status(201)
      |> json(%{inserted_count: count})
    end
  end

  operation(:delete,
    summary: "Bulk delete key-value pairs by keys or values",
    request_body:
      {"Bulk delete", "application/json",
       %OpenApiSpex.Schema{
         type: :object,
         properties: %{
           keys: %OpenApiSpex.Schema{type: :array, items: %OpenApiSpex.Schema{type: :string}},
           values: %OpenApiSpex.Schema{type: :array, items: %OpenApiSpex.Schema{type: :string}}
         }
       }},
    responses: %{
      200 =>
        {"Deleted count", "application/json",
         %OpenApiSpex.Schema{
           type: :object,
           properties: %{deleted_count: %OpenApiSpex.Schema{type: :integer}}
         }}
    }
  )

  def delete(%{assigns: %{user: user}} = conn, params) do
    deleted =
      cond do
        keys = params["keys"] ->
          {count, _} = KeyValues.bulk_delete_by_keys(user.id, keys)
          count

        values = params["values"] ->
          {count, _} = KeyValues.bulk_delete_by_values(user.id, values)
          count

        true ->
          0
      end

    json(conn, %{deleted_count: deleted})
  end
end
