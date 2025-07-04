defmodule LogflareWeb.Api.RuleController do
  use LogflareWeb, :controller
  use OpenApiSpex.ControllerSpecs

  alias Logflare.Backends
  alias Logflare.Sources
  alias Logflare.Sources.Rules
  alias LogflareWeb.OpenApi.Accepted
  alias LogflareWeb.OpenApi.Created
  alias LogflareWeb.OpenApi.List
  alias LogflareWeb.OpenApi.NotFound
  alias LogflareWeb.OpenApiSchemas.RuleApiSchema

  action_fallback(LogflareWeb.Api.FallbackController)

  tags(["management"])

  operation(:index,
    summary: "List rules",
    responses: %{200 => List.response(RuleApiSchema)}
  )

  def index(%{assigns: %{user: user}} = conn, filters)
      when is_map_key(filters, "backend_id") or is_map_key(filters, "backend_token") do
    kw =
      case filters do
        %{"backend_id" => bid} -> [id: bid]
        %{"backend_token" => token} -> [token: token]
      end

    with {:ok, backend} <- Backends.fetch_backend_by([{:user_id, user.id} | kw]) do
      rules = Rules.list_rules(backend)
      json(conn, rules)
    end
  end

  operation(:show,
    summary: "Fetch rule",
    parameters: [token: [in: :path, description: "Rule UUID", type: :string]],
    responses: %{
      200 => RuleApiSchema.response(),
      404 => NotFound.response()
    }
  )

  def show(%{assigns: %{user: user}} = conn, %{"token" => token}) do
    with {:ok, backend} <- Rules.fetch_rule_by(token: token, user_id: user.id) do
      json(conn, backend)
    end
  end

  operation(:create,
    summary: "Create rule. Allows batch creation if as a list.",
    request_body: RuleApiSchema.params(),
    responses: %{
      201 => Created.response(RuleApiSchema),
      404 => NotFound.response()
    }
  )

  def create(%{assigns: %{user: user}} = conn, %{"_json" => batch}) when is_list(batch) do
    rules =
      for params <- batch do
        with {:ok, _} <- verify_backend_owner(params, user),
             {:ok, _} <- verify_source_owner(params, user),
             {:ok, rule} <- Rules.create_rule(params) do
          rule
        end
      end

    {errors, results} =
      Enum.split_with(rules, fn
        {:error, _} -> true
        _ -> false
      end)

    conn
    |> case do
      conn when errors == [] ->
        put_status(conn, 201)

      _ ->
        put_status(conn, 400)
    end
    |> json(%{
      errors:
        Enum.flat_map(errors, fn
          {:error, %Ecto.Changeset{} = changeset} ->
            Ecto.Changeset.traverse_errors(changeset, fn _, _, {message, _} -> message end)
            |> Enum.map(fn {field, errs} -> "#{field} #{Enum.join(errs, ", ")}" end)

          {:error, :not_found} ->
            "Unauthorized"

          nil ->
            "Unauthorized"
        end),
      results: results
    })
  end

  def create(%{assigns: %{user: user}} = conn, params) do
    with {:ok, _} <- verify_backend_owner(params, user),
         {:ok, _} <- verify_source_owner(params, user),
         {:ok, rule} <- Rules.create_rule(params) do
      conn
      |> put_status(201)
      |> json(rule)
    end
  end

  defp verify_backend_owner(%{"backend_id" => id}, user) do
    Backends.fetch_backend_by(id: id, user_id: user.id)
  end

  defp verify_source_owner(%{"source_id" => id}, user) do
    Sources.fetch_source_by(id: id, user_id: user.id)
  end

  operation(:update,
    summary: "Update rule",
    parameters: [token: [in: :path, description: "Rule UUID", type: :string]],
    request_body: RuleApiSchema.params(),
    responses: %{
      204 => Accepted.response(),
      200 => Accepted.response(RuleApiSchema),
      404 => NotFound.response()
    }
  )

  def update(%{assigns: %{user: user}} = conn, %{"token" => token} = params) do
    with {:ok, rule} <- Rules.fetch_rule_by(token: token, user_id: user.id),
         {:ok, updated} <- Rules.update_rule(rule, params) do
      conn
      |> case do
        %{method: "PATCH"} ->
          conn |> send_resp(204, "")

        %{method: "PUT"} ->
          put_status(conn, 200)
          |> json(updated)
      end
    end
  end

  operation(:delete,
    summary: "Delete rule",
    parameters: [token: [in: :path, description: "Rule UUID", type: :string]],
    responses: %{
      204 => Accepted.response(),
      404 => NotFound.response()
    }
  )

  def delete(%{assigns: %{user: user}} = conn, %{"token" => token}) do
    with {:ok, rule} <- Rules.fetch_rule_by(token: token, user_id: user.id),
         {:ok, _} <- Rules.delete_rule(rule) do
      conn
      |> Plug.Conn.send_resp(204, [])
      |> Plug.Conn.halt()
    end
  end
end
