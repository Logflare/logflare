defmodule LogflareWeb.Plugs.MaybeContentTypeToJson do
  @moduledoc """
  Turns the Content-Type request header to `application/json`
  """
  use Plug.Builder

  import Plug.Conn

  def call(conn, _params) do
    [content_type] = conn |> get_req_header("content-type")

    case content_type do
      "application/csp-report" ->
        conn
        |> put_req_header("content-type", "application/json")

      _else ->
        conn
    end
  end
end
