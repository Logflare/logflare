# Debug utility: re-publishes a queue message for every spool file already
# sitting in the local fake-gcs emulator's bucket directory. Useful after a
# consumer-side bug discarded/never-processed a batch of files whose original
# queue messages are gone (acked/expired) but the files themselves are still
# on disk — this lets you push them back through the pipeline without
# re-ingesting the original events.
#
# Usage: mix run scripts/replay_spool_files.exs
#
# Talks directly to the local PubSub emulator's REST API (no Goth/auth —
# the emulator doesn't validate tokens), so it runs standalone without
# needing to attach to an already-running node. Hardcoded to match the
# defaults in config/dev.exs and docker-compose.gcp.yml.

pubsub_url = "http://localhost:8085"
topic = "projects/logflare/topics/logflare-spool"
bucket_dir = Path.join(["tmp", "fake-gcs", "logflare-spool"])

Application.ensure_all_started(:inets)
Application.ensure_all_started(:ssl)

file_keys =
  bucket_dir
  |> Path.join("**/*")
  |> Path.wildcard()
  |> Enum.filter(&File.regular?/1)
  |> Enum.reject(&String.ends_with?(&1, ".metadata"))
  |> Enum.map(&Path.relative_to(&1, bucket_dir))
  |> Enum.reject(&(&1 == ".keep"))

IO.puts("Found #{length(file_keys)} spool file(s) in #{bucket_dir}")

publish = fn file_key ->
  data = Jason.encode!(%{file_key: file_key}) |> Base.encode64()
  body = Jason.encode!(%{messages: [%{data: data}]})
  url = String.to_charlist("#{pubsub_url}/v1/#{topic}:publish")
  headers = [{~c"content-type", ~c"application/json"}]

  case :httpc.request(:post, {url, headers, ~c"application/json", body}, [], []) do
    {:ok, {{_, status, _}, _, _}} when status in 200..299 -> :ok
    {:ok, {{_, status, resp_body}, _, _}} -> {:error, {status, resp_body}}
    {:error, reason} -> {:error, reason}
  end
end

{ok_count, error_count} =
  Enum.reduce(file_keys, {0, 0}, fn file_key, {ok, error} ->
    case publish.(file_key) do
      :ok ->
        IO.puts("  published #{file_key}")
        {ok + 1, error}

      {:error, reason} ->
        IO.puts("  FAILED #{file_key}: #{inspect(reason)}")
        {ok, error + 1}
    end
  end)

IO.puts("\nDone: #{ok_count} published, #{error_count} failed")
