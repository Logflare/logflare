defmodule Logflare.UserLogInterceptor do
  @moduledoc """
  Intercepts Logger messages related to specific users, and send them to the respective
  System Source when the user has activated it
  """
  alias Logflare.{Users, Sources}
  alias Logflare.Logs
  alias Logflare.Logs.Processor

  def run(log, _) do
    with %{meta: meta} <- log,
         user_id when is_integer(user_id) <- Users.get_related_user_id(meta),
         %{system_monitoring: true} <- Users.Cache.get(user_id),
         %{} = source <- get_system_source(user_id) do
      LogflareLogger.Formatter.format(
        log.level,
        format_message(log),
        get_datetime(),
        meta
      )
      |> List.wrap()
      |> Processor.ingest(Logs.Raw, source)

      :stop
    else
      _ -> :ignore
    end
  end

  defp get_system_source(user_id),
    do:
      Sources.Cache.get_by(user_id: user_id, system_source_type: :logs)
      |> Sources.refresh_source_metrics()
      |> Sources.Cache.preload_rules()

  defp format_message(event),
    do:
      :logger_formatter.format(event, %{single_line: true, template: [:msg]})
      |> IO.iodata_to_binary()

  defp get_datetime do
    dt = NaiveDateTime.utc_now()
    {date, {hour, minute, second}} = NaiveDateTime.to_erl(dt)
    {date, {hour, minute, second, dt.microsecond}}
  end
end
