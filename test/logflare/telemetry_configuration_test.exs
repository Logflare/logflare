defmodule Logflare.TelemetryConfigurationTest do
  use ExUnit.Case, async: false

  alias Logflare.Telemetry

  @config_key :broadway_message_sample_denominator
  @processor_message_metric_name [:broadway, :processor, :message, :stop, :duration]

  setup do
    original_value = Application.fetch_env!(:logflare, @config_key)

    on_exit(fn ->
      Application.put_env(:logflare, @config_key, original_value)
    end)

    :ok
  end

  test "uses a configured positive Broadway processor message sample denominator" do
    denominator = 7
    Application.put_env(:logflare, @config_key, denominator)

    metric = processor_message_metric!()
    context = make_ref()

    assert metric.keep.(%{telemetry_span_context: context}) ==
             (:erlang.phash2(context, denominator) == 0)
  end

  test "omits the Broadway processor message metric when sampling is disabled" do
    Application.put_env(:logflare, @config_key, :disabled)

    metric_names = Enum.map(Telemetry.metrics(), & &1.name)

    refute @processor_message_metric_name in metric_names

    for metric_name <- [
          [:broadway, :batcher, :stop, :duration],
          [:broadway, :batch_processor, :stop, :duration],
          [:broadway, :processor, :stop, :duration]
        ] do
      assert metric_name in metric_names
    end
  end

  test "rejects invalid Broadway processor message sample denominators" do
    for value <- [0, -1, 4_294_967_297, "invalid"] do
      Application.put_env(:logflare, @config_key, value)

      assert_raise ArgumentError,
                   ~r/LOGFLARE_BROADWAY_MESSAGE_SAMPLE_DENOMINATOR must be 'disabled' or an integer between 1 and 4294967296/,
                   fn -> Telemetry.metrics() end
    end
  end

  defp processor_message_metric! do
    Enum.find(Telemetry.metrics(), &(&1.name == @processor_message_metric_name)) ||
      flunk("expected Broadway processor message metric to be defined")
  end
end
