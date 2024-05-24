defmodule Logflare.PipelinesTest do
  defmodule StubProducer do
    use GenStage

    def start_link(opts) when is_list(opts) do
      GenStage.start_link(__MODULE__, opts)
    end

    def init(_opts) do
      {:producer, %{}, []}
    end

    def handle_demand(_demand, state) do
      {:noreply, [], state}
    end
  end

  defmodule StubPipeline do
    @moduledoc "Stub pipeline used for testing DynamicPipeline"
    use Broadway
    def start_link(opts) do
      Broadway.start_link(__MODULE__,
        name: opts[:name],
        producer: [
          module: {Logflare.PipelinesTest.StubProducer, []}
        ],
        processors: [
          default: [concurrency: 1]
        ]
      )
    end

    # pipeline name is sharded
    def process_name({:via, module, {registry, identifier}}, base_name) do
      {:via, module, {registry, {identifier, base_name}}}
    end

    def handle_message(_processor_name, message, _context) do
      :timer.sleep(1_000)
      message
    end
  end
end
