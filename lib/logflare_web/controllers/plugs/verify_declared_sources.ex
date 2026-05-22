defmodule LogflareWeb.Plugs.VerifyDeclaredSources do
  @moduledoc """
  Resolves and authorizes sources declared per-event via the `__LF_SOURCE`
  body field for multi-source ingestion.

  Multi-source mode is detected by pattern matching on the first event in the
  batch: if the first event has `__LF_SOURCE`, every unique UUID across the
  batch is resolved and ownership/scope is verified for each. The resolved
  sources are stored in `conn.assigns.declared_sources` as
  `%{uuid_string => %Source{}}` for the controller to dispatch against.

  If the first event lacks `__LF_SOURCE`, the plug is a no-op and the
  existing single-source ingest flow handles the request.
  """

  import Plug.Conn

  alias Logflare.Sources
  alias Logflare.Sources.Source
  alias Logflare.User
  alias Logflare.Utils
  alias LogflareWeb.Api.FallbackController
  alias LogflareWeb.Plugs.VerifyResourceAccess

  @lf_source_key "__LF_SOURCE"

  def init(_opts), do: nil

  def call(%{assigns: %{resource_type: :source, user: %User{} = user}} = conn, _opts) do
    case extract_events(conn.body_params) do
      [first | _] = events when is_map(first) ->
        if Utils.Map.get(first, :"#{@lf_source_key}") do
          resolve_and_verify(conn, events, user)
        else
          conn
        end

      _ ->
        conn
    end
  end

  def call(conn, _opts), do: conn

  defp extract_events(%{"batch" => batch}) when is_list(batch), do: batch
  defp extract_events(%{"_json" => batch}) when is_list(batch), do: batch
  defp extract_events(event) when is_map(event), do: [event]
  defp extract_events(_), do: []

  defp resolve_and_verify(conn, events, user) do
    access_token = Map.get(conn.assigns, :access_token)

    tokens =
      events
      |> Enum.reduce(MapSet.new(), fn event, acc ->
        case Utils.Map.get(event, :"#{@lf_source_key}") do
          token when is_binary(token) -> MapSet.put(acc, token)
          _ -> acc
        end
      end)
      |> Enum.filter(&uuid?/1)

    case resolve_sources(tokens, user, access_token) do
      {:ok, declared} ->
        assign(conn, :declared_sources, declared)

      :error ->
        conn
        |> FallbackController.call({:error, :unauthorized})
        |> halt()
    end
  end

  defp resolve_sources(tokens, user, access_token) do
    Enum.reduce_while(tokens, {:ok, %{}}, fn token, {:ok, acc} ->
      case Sources.Cache.get_by_and_preload_rules(token: token) do
        %Source{} = source ->
          source = Sources.refresh_source_metrics_for_ingest(source)

          if VerifyResourceAccess.verify_source_access(source, user, access_token) do
            {:cont, {:ok, Map.put(acc, token, source)}}
          else
            {:halt, :error}
          end

        _ ->
          {:halt, :error}
      end
    end)
  end

  defp uuid?(value) when is_binary(value) do
    case Ecto.UUID.dump(value) do
      {:ok, _} -> true
      _ -> false
    end
  end

  defp uuid?(_), do: false
end
