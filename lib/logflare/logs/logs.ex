defmodule Logflare.Logs do
  @moduledoc false
  require Logger
  use Publicist

  alias Logflare.LogEvent, as: LE
  alias Logflare.Logs.{RejectedLogEvents}
  alias Logflare.{SystemMetrics, Source, Sources}
  alias Logflare.Source.{BigQuery.Buffer, RecentLogsServer}
  alias Logflare.Rule

  @spec injest_logs(list(map), Source.t()) :: :ok | {:error, term}
  def injest_logs(log_params_batch, %Source{} = source) do
    log_params_batch
    |> Enum.map(&LE.make(&1, %{source: source}))
    |> Enum.map(fn %LE{} = le ->
      if le.valid? do
        injest_by_source_rules(le)
        injest_and_broadcast(le)
      else
        RejectedLogEvents.injest(le)
      end

      le
    end)
    |> Enum.reduce([], fn log, acc ->
      if log.valid? do
        acc
      else
        [log.validation_error | acc]
      end
    end)
    |> case do
      [] -> :ok
      errors when is_list(errors) -> {:error, errors}
    end
  end

  @spec injest_by_source_rules(LE.t()) :: term | :noop
  defp injest_by_source_rules(%LE{via_rule: %Rule{} = rule} = le) when not is_nil(rule) do
    Logger.error(
      "LogEvent #{le.id} has already been routed using the rule #{rule.id}, can't proceed!"
    )

    :noop
  end

  defp injest_by_source_rules(%LE{source: %Source{} = source, via_rule: nil} = le) do
    for rule <- source.rules, Regex.match?(~r{#{rule.regex}}, le.body.message) do
      sink_source = Sources.Cache.get_by(token: rule.sink)

      if sink_source do
        injest_and_broadcast(%{le | source: sink_source, via_rule: rule})
      else
        Logger.error("Sink source for UUID #{rule.sink} doesn't exist")
      end
    end
  end

  defp injest_and_broadcast(%LE{source: %Source{} = source} = le) do
    source_table_string = Atom.to_string(source.token)

    # indvididual source genservers
    RecentLogsServer.push(source.token, le)
    Buffer.push(source_table_string, le)

    # all sources genservers
    Sources.Counters.incriment(source.token)
    SystemMetrics.AllLogsLogged.incriment(:total_logs_logged)

    # broadcasters
    Source.ChannelTopics.broadcast_log_count(source)
    Source.ChannelTopics.broadcast_new(le)
  end
end
