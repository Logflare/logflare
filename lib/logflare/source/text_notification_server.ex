defmodule Logflare.Source.TextNoticationServer do
  use GenServer

  require Logger

  alias Logflare.{Sources, Users}
  alias Logflare.Source.RateCounterServer
  alias LogflareWeb.Router.Helpers, as: Routes
  alias LogflareWeb.Endpoint

  @check_rate_every 1_000
  @twilio_phone "+16026006731"

  def start_link(source_id) do
    GenServer.start_link(
      __MODULE__,
      %{
        source: source_id,
        events: []
      },
      name: name(source_id)
    )
  end

  def init(state) do
    Logger.info("Table texter started: #{state.source}")
    Process.flag(:trap_exit, true)
    check_rate()
    {:ok, state}
  end

  def handle_info(:check_rate, state) do
    rate = RateCounterServer.get_rate(state.source)

    case rate > 0 do
      true ->
        source = Sources.Cache.get_by_id(state.source)
        user = Users.Cache.get_by_id(source.user_id)
        source_link = build_host() <> Routes.source_path(Endpoint, :show, source.id)

        {target_number, body} =
          {user.phone, "#{source.name} has #{rate} new event(s). See: #{source_link} "}

        if source.user_text_notifications == true do
          Task.Supervisor.start_child(Logflare.TaskSupervisor, fn ->
            ExTwilio.Message.create(to: target_number, from: @twilio_phone, body: body)
          end)
        end

        check_rate()
        {:noreply, state}

      false ->
        check_rate()
        {:noreply, state}
    end
  end

  def terminate(reason, _state) do
    # Do Shutdown Stuff
    Logger.info("Going Down: #{__MODULE__}")
    reason
  end

  defp check_rate() do
    Process.send_after(self(), :check_rate, @check_rate_every)
  end

  defp name(source_id) do
    String.to_atom("#{source_id}" <> "-texter")
  end

  defp build_host() do
    host_info = LogflareWeb.Endpoint.struct_url()
    host_info.scheme <> "://" <> host_info.host
  end
end
