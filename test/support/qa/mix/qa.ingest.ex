defmodule Mix.Tasks.Qa.Ingest do
  @shortdoc "Runs ingestion QA against the server started by `mix qa.server`"
  @moduledoc """
  Sends a run of events over HTTP, WebSocket and gRPC, verifies inside the server node
  that every source rule and backend rule routed exactly the events it matches, and
  captures the events in the search UI.

      mix qa.ingest
      mix qa.ingest --no-screenshot

  Prints `QA_CHECK PASS|FAIL` lines and `QA_CAPTURE <png>` lines, then `QA_RESULT`.
  Exits non-zero on any failure. Verify each capture against its `.json` expectations.
  """
  use Mix.Task

  alias Logflare.QA.Browser
  alias Logflare.QA.Check
  alias Logflare.QA.Config
  alias Logflare.QA.Ingest.Channels
  alias Logflare.QA.Ingest.SearchSpec
  alias Logflare.QA.Ingest.Setup
  alias Logflare.QA.Ingest.Targets
  alias Logflare.QA.Ingest.Verify
  alias Logflare.QA.Remote
  alias Logflare.QA.Report

  @impl Mix.Task
  def run(args) do
    {opts, _} = OptionParser.parse!(args, strict: [screenshot: :boolean])
    Mix.Task.run("compile")
    {:ok, _} = Application.ensure_all_started([:jason, :logger])

    config = Targets.config()
    Remote.connect!()
    %{token: token, id: source_id} = Remote.call(Setup, :run, [config])
    run_id = "run#{System.os_time(:second)}"
    batch = &Targets.messages(config, run_id, [&1])
    key = [{"x-api-key", Config.public_token()}]

    ingest = [
      Report.check(
        "HTTP ingest by source token",
        Channels.http("source=#{token}", batch.("http_token"), key) == 200
      ),
      Report.check(
        "HTTP ingest by source name",
        Channels.http("source_name=#{config.main}", batch.("http_name"), key) == 200
      ),
      Report.check(
        "HTTP rejects unknown source",
        Channels.http("source=00000000-0000-0000-0000-000000000000", [], key) == 401
      ),
      Report.check(
        "HTTP rejects missing api key",
        Channels.http("source=#{token}", [], []) == 401
      ),
      Report.check(
        "HTTP rejects wrong api key",
        Channels.http("source=#{token}", [], [{"x-api-key", "wrong"}]) == 401
      ),
      channel_check(
        "WebSocket LogChannel ingest",
        Channels.websocket(token, batch.("websocket"))
      ),
      channel_check("gRPC OTLP logs export", Channels.grpc(token, batch.("grpc")))
    ]

    routing =
      for %Check{label: label, ok?: ok?, detail: detail} <-
            Remote.call(Verify, :run, [config, run_id]),
          do: Report.check(label, ok?, detail)

    ui =
      if Keyword.get(opts, :screenshot, true) do
        browser = Browser.start!()
        messages = Targets.messages(config, run_id)

        [
          Report.spec("search UI lists the run's events", fn ->
            SearchSpec.run(browser, source_id, run_id, messages)
          end)
        ]
      else
        []
      end

    Report.finish!(ingest ++ routing ++ ui, run_id)
  end

  defp channel_check(label, :ok), do: Report.check(label, true)
  defp channel_check(label, {:error, reason}), do: Report.check(label, false, inspect(reason))
end
