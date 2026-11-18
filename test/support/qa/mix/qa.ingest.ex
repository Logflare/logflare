defmodule Mix.Tasks.Qa.Ingest do
  @shortdoc "Runs ingestion QA against the server started by `mix qa.server`"
  @moduledoc """
  Sends a run of events over HTTP, WebSocket and gRPC, checks inside the server node that
  every source rule and backend rule stored exactly the events it matches, and captures
  the run in the search UI. Exits non-zero on any failure.

      mix qa.ingest
      mix qa.ingest --no-screenshot
  """
  use Mix.Task

  alias Logflare.QA.Browser
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

    Remote.connect!()
    %{token: token, id: source_id} = Remote.call(Setup, :run, [])
    run_id = "run#{System.os_time(:second)}"

    ingest = [
      Report.check(
        "HTTP ingest by source token",
        Channels.http("source=#{token}", Targets.batch("http_token", run_id))
      ),
      Report.check(
        "HTTP ingest by source name",
        Channels.http("source_name=#{Targets.main()}", Targets.batch("http_name", run_id))
      ),
      Report.check(
        "WebSocket ingest",
        Channels.websocket(token, Targets.batch("websocket", run_id))
      ),
      Report.check("gRPC OTLP ingest", Channels.grpc(token, Targets.batch("grpc", run_id)))
    ]

    routing =
      for {label, ok?, detail} <- Remote.call(Verify, :run, [run_id]),
          do: Report.check(label, ok?, detail)

    ui =
      if Keyword.get(opts, :screenshot, true) do
        browser = Browser.start!()
        messages = Targets.messages(run_id)

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
end
