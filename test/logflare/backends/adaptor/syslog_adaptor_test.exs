defmodule Logflare.Backends.Adaptor.SyslogAdaptorTest do
  use Logflare.DataCase, async: false
  @moduletag :telegraf

  @telegraf_output_path "db/telegraf_output/metrics.out"

  setup_all do
    on_exit(fn ->
      if File.exists?(@telegraf_output_path) do
        File.write!(@telegraf_output_path, _empty = "")
      end
    end)
  end

  test "basic fields check" do
    backend_config = %{host: "localhost", port: 6514}

    assert [
             %{
               "fields" => %{
                 "message" => telegraf_message
               },
               "name" => "syslog",
               "tags" => %{
                 "appname" => "logflare",
                 "facility" => "local0",
                 "severity" => "info"
               }
             }
           ] =
             ingest_syslog(
               [build(:log_event, message: "basic unicode message ✍️")],
               backend_config
             )

    assert %{"event_message" => "basic unicode message ✍️"} =
             Jason.decode!(telegraf_message)
  end

  test "handles opentelemetry metadata" do
    backend_config = %{host: "localhost", port: 6514}

    assert [
             %{
               "tags" => %{
                 "appname" => "Logflare_(from_resource.name)",
                 "hostname" => ":\"logflare-versioned@10.0.0.123\""
               }
             }
           ] =
             ingest_syslog(
               [
                 build(:log_event,
                   message: "hello from opentelemetry",
                   resource: %{
                     "cluster" => "versioned",
                     "name" => "Logflare (from resource.name)",
                     "node" => ":\"logflare-versioned@10.0.0.123\"",
                     "service" => %{"name" => "Logflare", "version" => "1.26.25"}
                   }
                 )
               ],
               backend_config
             )
  end

  test "extracts level from input" do
    backend_config = %{host: "localhost", port: 6514}

    assert [
             %{"tags" => %{"severity" => "debug"}},
             %{"tags" => %{"severity" => "err"}}
           ] =
             ingest_syslog(
               [
                 build(:log_event, level: "debug", message: "eh"),
                 build(:log_event, metadata: %{"level" => "error"}, message: "eh")
               ],
               backend_config
             )
  end

  test "replaces invalid or empty log level with `info` severity code" do
    backend_config = %{host: "localhost", port: 6514}

    assert [
             %{"tags" => %{"severity" => "info"}},
             %{"tags" => %{"severity" => "info"}}
           ] =
             ingest_syslog(
               [
                 build(:log_event, message: "no level"),
                 build(:log_event, message: "bad level", level: "bad")
               ],
               backend_config
             )
  end

  test "sends message over mTLS" do
    backend_config = %{
      host: "localhost",
      port: 6515,
      tls: true,
      ca_cert: File.read!("priv/telegraf/ca.crt"),
      client_cert: File.read!("priv/telegraf/client.crt"),
      client_key: File.read!("priv/telegraf/client.key")
    }

    assert [%{"fields" => %{"message" => telegraf_event_message}}] =
             ingest_syslog(
               [build(:log_event, message: "hello world over tls")],
               backend_config
             )

    assert %{"event_message" => "hello world over tls"} = Jason.decode!(telegraf_event_message)
  end

  test "validates PEM configuration" do
    config = %{
      tls: true,
      ca_cert: "invalid-pem",
      client_cert: "invalid-pem",
      client_key: "invalid-pem"
    }

    assert %Ecto.Changeset{valid?: false} =
             changeset =
             config
             |> Logflare.Backends.Adaptor.SyslogAdaptor.cast_config()
             |> Logflare.Backends.Adaptor.SyslogAdaptor.validate_config()

    assert %{
             ca_cert: ["must be a valid PEM encoded string"],
             client_cert: ["must be a valid PEM encoded string"],
             client_key: ["must be a valid PEM encoded string"]
           } = errors_on(changeset)
  end

  test "can send encrypted message" do
    key = :crypto.strong_rand_bytes(32)
    backend_config = %{host: "localhost", port: 6514, cipher_key: Base.encode64(key)}

    assert [%{"fields" => %{"message" => encrypted_message}}] =
             ingest_syslog([build(:log_event, message: "hello cipher")], backend_config)

    assert <<iv::12-bytes, tag::16-bytes, ciphertext::bytes>> = Base.decode64!(encrypted_message)

    plaintext =
      :crypto.crypto_one_time_aead(:aes_256_gcm, key, iv, ciphertext, "syslog", tag, false)

    assert %{"event_message" => "hello cipher"} = Jason.decode!(plaintext)
  end

  test "handles backend config change" do
    backend_config = %{host: "localhost", port: 6514}

    assert [_, _] =
             ingest_syslog(
               [build(:log_event, message: "one"), build(:log_event, message: "two")],
               backend_config
             )

    key = :crypto.strong_rand_bytes(32)

    assert {:ok, backend} =
             Logflare.Backends.update_backend(lookup_backend(), %{
               "config" => %{
                 "host" => "localhost",
                 "port" => 6515,
                 "cipher_key" => Base.encode64(key),
                 "tls" => true,
                 "ca_cert" => File.read!("priv/telegraf/ca.crt"),
                 "client_cert" => File.read!("priv/telegraf/client.crt"),
                 "client_key" => File.read!("priv/telegraf/client.key")
               }
             })

    # simulate cache bust
    Logflare.ContextCache.bust_keys([{Logflare.Backends, backend.id}])

    assert [%{"fields" => %{"message" => encrypted_message}}] =
             ingest_syslog([build(:log_event, message: "three")], nil)

    assert <<iv::12-bytes, tag::16-bytes, ciphertext::bytes>> = Base.decode64!(encrypted_message)

    plaintext =
      :crypto.crypto_one_time_aead(:aes_256_gcm, key, iv, ciphertext, "syslog", tag, false)

    assert %{"event_message" => "three"} = Jason.decode!(plaintext)
  end

  defp ingest_syslog(log_events, backend_config, timeout \\ to_timeout(second: 5)) do
    deadline = System.monotonic_time(:millisecond) + timeout
    source = lookup_source() || create_source(backend_config)

    log_events =
      log_events
      |> List.wrap()
      |> Enum.map(fn log_event -> %{log_event | source_id: source.id} end)

    {:ok, _count} = Logflare.Backends.ingest_logs(log_events, source)
    collect_telegraf_logs(log_events, deadline)
  end

  defp lookup_source do
    Process.get(:syslog_source)
  end

  defp create_source(backend_config) do
    start_supervised!(Logflare.SystemMetrics.AllLogsLogged)
    insert(:plan)

    user = insert(:user)
    source = insert(:source, user: user)
    Process.put(:syslog_source, source)

    backend =
      insert(:backend, type: :syslog, sources: [source], config: backend_config, user: user)

    Process.put(:syslog_backend, backend)
    start_supervised!({Logflare.Backends.AdaptorSupervisor, {source, backend}})

    source
  end

  defp lookup_backend do
    Process.get(:syslog_backend)
  end

  defp collect_telegraf_logs(log_events, deadline) do
    # extract msgids to match them with telegraf output
    syslog_msgids =
      Enum.map(log_events, fn log_event ->
        # uuid -> base32 is what we do in syslog formatter to fit in MSGID size limits
        log_event.id |> Ecto.UUID.dump!() |> Base.encode32(padding: false)
      end)

    telegraf_logs =
      @telegraf_output_path
      |> File.stream!()
      |> Stream.map(fn line ->
        case Jason.decode(line) do
          {:ok, json} ->
            json

          {:error, reason} ->
            raise """
            Failed to parse telegraf line (from #{@telegraf_output_path}) as JSON.
            Line: #{inspect(line)}
            Error: #{Exception.format(:error, reason)}
            """
        end
      end)
      |> Enum.filter(fn %{"fields" => %{"msgid" => msgid}} ->
        msgid in syslog_msgids
      end)

    cond do
      length(log_events) == length(telegraf_logs) ->
        telegraf_logs_lookup =
          Map.new(telegraf_logs, fn %{"fields" => %{"msgid" => syslog_msg_id}} = log ->
            log_event_id = syslog_msg_id |> Base.decode32!(padding: false) |> Ecto.UUID.load!()
            {log_event_id, log}
          end)

        # now we match "output" telegraf logs with "input" log events
        Enum.map(log_events, fn log_event ->
          Map.fetch!(telegraf_logs_lookup, log_event.id)
        end)

      System.monotonic_time(:millisecond) < deadline ->
        Process.sleep(100)
        collect_telegraf_logs(log_events, deadline)

      true ->
        raise """
        Failed to collect all #{length(log_events)} telegraf logs (from #{@telegraf_output_path}) before deadline.
        Collected #{length(telegraf_logs)} logs: #{inspect(telegraf_logs)}
        """
    end
  end
end
