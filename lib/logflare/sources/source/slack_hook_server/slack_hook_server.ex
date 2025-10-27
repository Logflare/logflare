defmodule Logflare.Sources.Source.SlackHookServer do
  @moduledoc false
  use GenServer

  require Logger

  alias Logflare.Sources
  alias Logflare.Sources.Source
  alias Logflare.Sources.Counters
  alias Logflare.Backends
  alias Logflare.Backends.Adaptor.SlackAdaptor

  def start_link(args) do
    source = Keyword.get(args, :source)
    GenServer.start_link(__MODULE__, args, name: Backends.via_source(source, __MODULE__))
  end

  @spec test_post(Source.t()) :: {:ok, Tesla.Env.t()} | {:error, Tesla.Env.t()}
  def test_post(%Source{} = source) when source.slack_hook_url != nil do
    events = fetch_events(source, 3, false)

    SlackAdaptor.send_message(source, events, 3)
    |> handle_response(source)
  end

  def init(args) do
    source = Keyword.get(args, :source)
    check_rate(source.notifications_every)

    {:ok, current_inserts} = Counters.get_inserts(source.token)

    {:ok,
     %{
       source_id: source.id,
       source_token: source.token,
       notifications_every: source.notifications_every,
       inserts_since_boot: current_inserts
     }}
  end

  def handle_info(:check_rate, state) do
    {:ok, current_inserts} = Counters.get_inserts(state.source_token)
    new_count = current_inserts - state.inserts_since_boot

    state =
      with source when source != nil <- Sources.Cache.get_by_id(state.source_token),
           true <- new_count > 0,
           true <- source.slack_hook_url != nil,
           events when is_list(events) <- fetch_events(source, new_count),
           true <- Enum.count(events) > 0 do
        SlackAdaptor.send_message(source, events, new_count)
        |> handle_response(source)

        %{state | inserts_since_boot: current_inserts}
      else
        _ -> state
      end

    check_rate(state.notifications_every)
    {:noreply, state}
  end

  def handle_info({:EXIT, _pid, :normal}, state) do
    :noop

    {:noreply, state}
  end

  def handle_info({:ssl_closed, _details}, state) do
    # See https://github.com/benoitc/hackney/issues/464
    :noop

    {:noreply, state}
  end

  defp check_rate(notifications_every) do
    Process.send_after(self(), :check_rate, notifications_every)
  end

  defp fetch_events(source, new_count, filter \\ true) do
    Backends.list_recent_logs_local(source)
    |> Enum.filter(fn
      _event when filter == false ->
        true

      event when filter == true ->
        # only include events that are less than 60s (max notification interval)
        DateTime.diff(
          DateTime.utc_now(),
          DateTime.from_unix!(event.body["timestamp"], :microsecond),
          :millisecond
        ) <= 60_000
    end)
    |> take_events(new_count)
  end

  defp handle_response(result, source) do
    case result do
      {:ok, %Tesla.Env{status: 200} = response} ->
        {:ok, response}

      {:ok, %Tesla.Env{url: _url, body: "invalid_blocks"} = response} ->
        resp = prep_tesla_resp_for_log(response)

        Logger.warning("Slack hook response: invalid_blocks", slackhook_response: resp)

        {:error, response}

      {:ok, %Tesla.Env{body: "no_service"} = response} ->
        resp = prep_tesla_resp_for_log(response)

        Logger.warning("Slack hook response: no_service", slackhook_response: resp)

        case Sources.delete_slack_hook_url(source) do
          {:ok, _source} ->
            Logger.warning("Slack hook url deleted.")

          {:error, _changeset} ->
            Logger.error("Error deleting Slack hook url.")
        end

        {:error, response}

      {:ok, %Tesla.Env{} = response} ->
        resp = prep_tesla_resp_for_log(response)

        Logger.warning("Slack hook error!", slackhook_response: resp)

        {:error, response}
    end
  end

  defp take_events(recent_events, rate) do
    cond do
      0 == rate ->
        []

      rate in 1..3 ->
        recent_events
        |> Enum.take(-rate)

      true ->
        recent_events
        |> Enum.take(-3)
    end
  end

  defp prep_tesla_resp_for_log(response) do
    Map.from_struct(response)
    |> Map.drop([:__client__, :__module__, :headers, :opts, :query])
    |> Map.put(:body, inspect(response.body))
  end
end
